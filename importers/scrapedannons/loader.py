import sys
import time
from multiprocessing import Value
import concurrent.futures
import requests
import bs4
from importers import settings
from importers.common import log
from importers.scrapedannons.converter import convert_ad_onepartnergroup


def get_all_ads_url_from_onepartnergroup():
    url = settings.participants_webs['onepartnergroup']
    result = requests.get(url=url)
    result_soup = bs4.BeautifulSoup(result.text, 'lxml')
    ids = result_soup.find_all(class_="job-list-item-wrapper")
    main_url = '/'.join(url.split('/')[:-1])
    url_list = []
    for id in ids:
        url_list.append(main_url+id.find('a').get('href'))
    return url_list


def get_all_ads_from_onepartnergroup():
    url_list = get_all_ads_url_from_onepartnergroup()
    for url in url_list:
        message = requests.get(url=url)
        if message:
            annons = convert_ad_onepartnergroup(message, url)


def bulk_fetch_ad_details(ad_batch):
    len_ad_batch = len(ad_batch)
    parallelism = settings.LA_DETAILS_PARALLELISM if len_ad_batch > 99 else 1
    log.info(f'Fetch ad details. Processes: {parallelism}, batch len: {len_ad_batch}')

    global counter
    counter = Value('i', 0)
    result_output = {}
    # with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        # Start the load operations
        future_to_fetch_result = {
            executor.submit(load_details_from_one_group, ad_data): ad_data
            for ad_data in ad_batch
        }
        for future in concurrent.futures.as_completed(future_to_fetch_result):
            try:
                detailed_result = future.result()
                result_output[detailed_result['id']] = detailed_result
                # += operation is not atomic, so we need to get a lock for counter:
                with counter.get_lock():
                    counter.value += 1
                    if counter.value % 500 == 0:
                        log.info(f"Threaded fetch ad details. Processed docs: {str(counter.value)}")
            except requests.exceptions.HTTPError as exc:
                # status_code = exc.response.status_code
                log.error(f'Fetch ad details call generated an exception: {exc}')
            except Exception as exc:
                log.error(f'Fetch ad details call generated an exception: {exc}')

    return result_output


def load_details_from_one_group(ad_meta):
    fail_count = 0
    #fail_max = settings.LA_ANNONS_MAX_TRY
    fail_max = 1
    try:
        print(ad_meta)
        message = requests.get(url=ad_meta)
        if message:
            annons = convert_ad_onepartnergroup(message, ad_meta)
            return annons
    except requests.exceptions.ConnectionError as e:
        fail_count += 1
        time.sleep(0.3)
        log.warning(f"Unable to load data from: {ad_meta} Connection error, try: {fail_count}")
        if fail_count >= fail_max:
            log.error(f"Failed to load data from: {ad_meta} after: {fail_max}. {e} Exit!")
            sys.exit(1)
    except requests.exceptions.Timeout as e:
        fail_count += 1
        time.sleep(0.3)
        log.warning(f"Unable to load data from: {ad_meta} Timeout, try: {fail_count}")
        if fail_count >= fail_max:
            log.error(f"Failed to load data from: {ad_meta} after: {fail_max}. {e} Exit!")
            sys.exit(1)
    except requests.exceptions.RequestException as e:
        fail_count += 1
        time.sleep(0.3)
        log.warning(f"Unable to fetch data at: {ad_meta}, try: {fail_count}, {e}")
        if fail_count >= fail_max:
            log.error(f"Failed to fetch: {ad_meta} after: {fail_max}, skipping. {e}")
            raise e

