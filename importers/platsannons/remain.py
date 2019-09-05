import time
import logging
import sys
import requests
import math
import itertools
import concurrent.futures
from datetime import datetime
from multiprocessing import Value
from importers import settings
from importers.platsannons import converter, enricher_mt_rest_multiple as enricher
from importers.repository import elastic, postgresql

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


def _setup_index(args):
    if len(args) > 1:
        es_index = args[1]
    else:
        es_index = None

    try:
        es_index = elastic.setup_indices(es_index, settings.ES_ANNONS_PREFIX,
                                         settings.platsannons_mappings)
        log.info('Starting importer %s with PG_BATCH_SIZE: %s for index %s'
                 % ('af-platsannons', settings.PG_BATCH_SIZE, es_index))
    except Exception as e:
        log.error("Elastic operations failed: %s" % str(e))
        sys.exit(1)
    return es_index


def _check_last_timestamp(es_index):
    last_timestamp = elastic.get_last_timestamp(es_index)
    log.info("Last timestamp: %d (%s)" % (last_timestamp,
                                          datetime.fromtimestamp(
                                              (last_timestamp + 1) / 1000)
                                          ))
    last_identifiers = elastic.get_ids_with_timestamp(last_timestamp,
                                                      es_index)
    return last_timestamp


def reload():
    start_time = time.time()
    # Get, set and create elastic index
    es_index = _setup_index(sys.argv)
    last_timestamp = _check_last_timestamp(es_index)
    # Check psql table existance
    # Load list of updated ad ids
    ad_ids = _load_list_of_updated_ids(last_timestamp)
    # Partition list into manageable chunks

    log.info('Fetching details for %s ads...' % len(ad_ids))
    nr_of_items_per_batch = int(settings.PG_BATCH_SIZE)
    nr_of_items_per_batch = min(nr_of_items_per_batch, len(ad_ids))
    nr_of_batches = math.ceil(len(ad_ids) / nr_of_items_per_batch)

    ad_batches = iter(lambda: list(itertools.islice(iter(ad_ids),
                                                    nr_of_items_per_batch)), [])
    processed_ads_total = 0
    for i, ad_batch in enumerate(ad_batches):
        log.info('Processing batch %s/%s' % (i + 1, nr_of_batches))

        # Fetch ads from LA to raw-list
        ad_details, failed_ads = _bulk_fetch_ad_details(ad_batch)
        for failed_ad in failed_ads.copy():
            # On fail, check for ad in postgresql
            failed_id = failed_ad['annonsId']
            pgsql_ad = postgresql.fetch_ad(failed_id, settings.PG_PLATSANNONS_TABLE)
            if pgsql_ad:
                ad_details[failed_id] = pgsql_ad
                failed_ads.remove(failed_ad)
            # On fail, ad ID to failed-list

        raw_ads = list(ad_details.values())
        # Save raw-list to postgresql
        postgresql.bulk(raw_ads, settings.PG_PLATSANNONS_TABLE)

        # Loop over raw-list, convert and enrich into cooked-list
        converted_ads = [converter.convert_ad(raw_ad) for raw_ad in raw_ads]
        enriched_ads = enricher.enrich(converted_ads)
        # Bulk save cooked-list to elastic
        elastic.bulk_index(enriched_ads, es_index)
        processed_ads_total = processed_ads_total + len(ad_batch)

        log.info('Processed %s/%s ads' % (processed_ads_total, len(ad_ids)))

    # Iterate over failed-list, trying to find in LA, postgresql
    log.info("Last pass, trying to load failed ads from LA")
    recovered_ads = []
    for failed_ad in failed_ads.copy():
        recovered_ad = _load_details_from_la(failed_ad)
        if recovered_ad:
            recovered_ads.append(recovered_ad)
            failed_ads.remove(failed_ad)

    # Save any recovered ads
    converted_ads = [converter.convert_ad(recovered_ad) for recovered_ad in recovered_ads]
    enriched_ads = enricher.enrich(converted_ads)
    elastic.bulk_index(enriched_ads, es_index)
    if failed_ads:
        log.warning("There are %d ads reported from stream that can't be download.",
                    len(failed_ads))


counter = None


def _bulk_fetch_ad_details(ad_batch):
    parallelism = int(settings.LA_DETAILS_PARALLELISM)
    log.info('Multithreaded fetch ad details with %s processes' % str(parallelism))

    global counter
    counter = Value('i', 0)
    result_output = {}
    failed_ids = []
    # with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        # Start the load operations
        future_to_fetch_result = {
            executor.submit(_load_details_from_la, ad_data): ad_data
            for ad_data in ad_batch
        }
        for future in concurrent.futures.as_completed(future_to_fetch_result):
            input_data = future_to_fetch_result[future]
            try:
                detailed_result = future.result()
                result_output[detailed_result['annonsId']] = detailed_result
                # += operation is not atomic, so we need to get a lock for counter:
                with counter.get_lock():
                    # log.info('Counter: %s' % counter.value)
                    counter.value += 1
                    if counter.value % 100 == 0:
                        log.info("Multithreaded fetch ad details - Processed %s docs" %
                                 (str(counter.value)))
            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code
                error_message = 'Fetch ad details call generated an exception: %s' % \
                    (str(exc))
                log.error(error_message)
                failed_ids.append(input_data)
            except Exception as exc:
                error_message = 'Fetch ad details call generated an exception: %s' % \
                    (str(exc))
                log.error(error_message)
                failed_ids.append(input_data)
    return result_output, failed_ids


def _load_details_from_la(ad_meta):
    fail_count = 0
    fail_max = 10
    ad_id = ad_meta['annonsId']
    detail_url = settings.LA_DETAILS_URL + str(ad_id)
    while True:
        try:
            r = requests.get(detail_url, timeout=60)
            r.raise_for_status()
            ad = r.json()
            if ad:
                ad['id'] = str(ad['annonsId'])
                ad['updatedAt'] = ad_meta['uppdateradTid']
                ad['expiresAt'] = ad['sistaPubliceringsdatum']
                if 'kontaktpersoner' in ad:
                    del ad['kontaktpersoner']
                if 'organisationsnummer' in ad and \
                        len(ad.get('organisationsnummer', '').strip()) > 9:
                    orgnr = ad['organisationsnummer']
                    significate_number_position = 4 if len(orgnr) == 12 else 2
                    if int(orgnr[significate_number_position]) < 2:
                        ad['organisationsnummer'] = None
                clean_ad = _cleanup_stringvalues(ad)
                return clean_ad
        # On fail, try again 10 times with 0.3 second delay
        except requests.exceptions.ConnectionError as e:
            fail_count += 1
            time.sleep(0.3)
            log.warning("Unable to load data from %s - Connection error" % detail_url)
            if fail_count >= fail_max:
                log.error("Unable to continue loading data from %s - Connection" %
                          detail_url, e)
                sys.exit(1)
        except requests.exceptions.Timeout as e:
            fail_count += 1
            time.sleep(0.3)
            log.warning("Unable to load data from %s - Timeout" % detail_url)
            if fail_count >= fail_max:
                log.error("Unable to continue loading data from %s - Timeout" %
                          detail_url, e)
                sys.exit(1)
        except requests.exceptions.RequestException as e:
            fail_count += 1
            time.sleep(0.3)
            log.warning(e)
            if fail_count >= fail_max:
                log.error("Failed to fetch data at %s, skipping" % detail_url, e)
                raise e


def _load_list_of_updated_ids(timestamp = 0):
    feed_url = settings.LA_BOOTSTRAP_FEED_URL \
        if timestamp == 0 else settings.LA_FEED_URL + str(timestamp)

    try:
        r = requests.get(feed_url)
        r.raise_for_status()
        json_result = r.json()
        items = json_result.get('idLista', [])

    except requests.exceptions.RequestException as e:
        log.error("Failed to read from %s" % feed_url, e)

    return items


def _cleanup_stringvalues(value):
    if isinstance(value, dict):
        value = {_cleanup_stringvalues(k): _cleanup_stringvalues(v) for k, v in value.items()}
    elif isinstance(value, list):
        value = [_cleanup_stringvalues(v) for v in value]
    elif isinstance(value, str):
        value = ''.join([i if ord(i) > 0 else '' for i in value])
    return value


if __name__ == '__main__':
    reload()
