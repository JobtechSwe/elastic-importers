import logging
import requests
import sys
import time
from multiprocessing import Value
import concurrent.futures
from importers import settings

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def fetch_bootstrap_ads():
    feed_url = settings.LA_BOOTSTRAP_FEED_URL

    log.info('fetch_bootstrap_ad_ids - fetching ads from %s' % feed_url)

    distinct_items = None
    try:
        r = requests.get(feed_url)
        r.raise_for_status()
        json_result = r.json()
        if json_result and 'idLista' in json_result:
            items = json_result['idLista']
            distinct_items = list({v['annonsId']: v for v in items}.values())
    except requests.exceptions.RequestException as e:
        log.error("Failed to read from %s" % feed_url, e)
    return distinct_items


def fetch_updated_ads(last_ts, exclude_ids):
    exclude_ids = set(exclude_ids)
    feed_url = settings.LA_FEED_URL + str(last_ts)
    log.info('fetch_updated_ads - fetching ads from %s' % feed_url)

    distinct_items = None
    try:
        r = requests.get(feed_url)
        r.raise_for_status()
        json_result = r.json()
        if json_result and 'idLista' in json_result:
            items = json_result['idLista']
            distinct_items = list({v['annonsId']: v for v in items
                                   if is_ad_included(v, last_ts, exclude_ids)}.values())
    except requests.exceptions.RequestException as e:
        log.error("Failed to read from %s" % feed_url, e)

    return distinct_items


def is_ad_included(ad_id_item, last_ts, exclude_ids):
    if ad_id_item['uppdateradTid'] == last_ts and \
            str(ad_id_item['annonsId']) in exclude_ids:
        return False
    else:
        return True


def clean_stringvalues(value):
    if isinstance(value, dict):
        value = {clean_stringvalues(k): clean_stringvalues(v) for k, v in value.items()}
    elif isinstance(value, list):
        value = [clean_stringvalues(v) for v in value]
    elif isinstance(value, str):
        value = ''.join([i if ord(i) > 0 else '' for i in value])
    return value


def fetch_ad_details(ad_id, ts, url):
    fail_count = 0
    fail_max = 10
    detail_url = url + str(ad_id)
    while True:
        try:
            r = requests.get(detail_url, timeout=60)
            r.raise_for_status()
            ad = r.json()
            if ad:
                ad['id'] = str(ad['annonsId'])
                ad['updatedAt'] = ts
                ad['expiresAt'] = ad['sistaPubliceringsdatum']
                if 'kontaktpersoner' in ad:
                    del ad['kontaktpersoner']
                if 'organisationsnummer' in ad and \
                        len(ad.get('organisationsnummer', '').strip()) > 9:
                    orgnr = ad['organisationsnummer']
                    significate_number_position = 4 if len(orgnr) == 12 else 2
                    if int(orgnr[significate_number_position]) < 2:
                        ad['organisationsnummer'] = None
                clean_ad = clean_stringvalues(ad)
                return clean_ad
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


def execute_calls(ad_datas, details_url, parallelism):
    parallelism = int(parallelism)
    log.info('Multithreaded fetch ad details with %s processes' % str(parallelism))

    global counter
    counter = Value('i', 0)
    result_output = {}
    error_ids = []
    not_found_ids = []
    # with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        # Start the load operations
        future_to_fetch_result = {
            executor.submit(fetch_ad_details, ad_data['annonsId'],
                            ad_data['uppdateradTid'], details_url): ad_data for
            ad_data in ad_datas}
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
                if status_code == 404:
                    # This exception will happen if the list of updated ads contains ads
                    # with 'avpublicerad' == false even though the ad does not exist when
                    # trying to fetch the ad details (wrong status in the list).
                    not_found_ids.append(input_data)
                else:
                    error_ids.append(input_data['annonsId'])
            except Exception as exc:
                error_message = 'Fetch ad details call generated an exception: %s' % \
                    (str(exc))
                log.error(error_message)
                error_ids.append(input_data['annonsId'])

    return result_output, error_ids, not_found_ids
