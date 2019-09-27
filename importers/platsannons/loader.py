import concurrent.futures
import sys
import logging
import time
from multiprocessing import Value

import requests
from importers import settings

log = logging.getLogger(__name__)

counter = None


def bulk_fetch_ad_details(ad_batch):
    parallelism = int(settings.LA_DETAILS_PARALLELISM)
    log.info('Multithreaded fetch ad details with %s processes' % str(parallelism))

    global counter
    counter = Value('i', 0)
    result_output = {}
    failed_ads = []
    # with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        # Start the load operations
        future_to_fetch_result = {
            executor.submit(load_details_from_la, ad_data): ad_data
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
                # status_code = exc.response.status_code
                error_message = 'Fetch ad details call generated an exception: %s' % \
                    (str(exc))
                log.error(error_message)
                failed_ads.append(input_data)
            except Exception as exc:
                error_message = 'Fetch ad details call generated an exception: %s' % \
                    (str(exc))
                log.error(error_message)
                failed_ads.append(input_data)

    return result_output, failed_ads


def load_details_from_la(ad_meta):
    fail_count = 0
    fail_max = 10
    ad_id = ad_meta['annonsId']
    if ad_meta.get('avpublicerad', False):
        log.debug("Ad %s is removed, preparing to delete" % ad_id)
        return {'annonsId': ad_id, 'id': ad_id, 'removed': True, 'avpublicerad': True}
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
                    try:
                        if int(orgnr[significate_number_position]) < 2:
                            ad['organisationsnummer'] = None
                    except ValueError:
                        ad['organisationsnummer'] = None
                        log.error(f"Valueerror in loader for orgnummer, ad {detail_url}. Replacing with None.")
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


def load_list_of_updated_ads(timestamp=0):
    items = []
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
