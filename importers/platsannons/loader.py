import concurrent.futures
import sys
import logging
import time
from multiprocessing import Value

import requests
from importers import settings
from importers.repository import elastic

log = logging.getLogger(__name__)

counter = None
logo_cache = {}


def bulk_fetch_ad_details(ad_batch):
    len_ad_batch = len(ad_batch)
    parallelism = settings.LA_DETAILS_PARALLELISM if len_ad_batch > 99 else 1
    log.info(f'Fetch ad details. Processes: {parallelism}, batch len: {len_ad_batch}')
    log.info(f'LA v2: {settings.LA_ANNONS_V2}, ad details base url: {settings.LA_DETAILS_URL}')
    global counter
    counter = Value('i', 0)
    result_output = {}
    # with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        # Start the load operations
        future_to_fetch_result = {
            executor.submit(load_ad_details, ad_data): ad_data
            for ad_data in ad_batch
        }
        for future in concurrent.futures.as_completed(future_to_fetch_result):
            try:
                detailed_result = future.result()
                result_output[detailed_result['annonsId']] = detailed_result
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


def load_ad_to_remove(unpublished_ad_meta):
    # Populate an unpublished ad with details
    ad_id = unpublished_ad_meta['annonsId']
    log.info(f"Ad is avpublicerad, preparing to remove it: {ad_id}")
    removed_date = unpublished_ad_meta.get('avpubliceringsdatum') or \
                   time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(unpublished_ad_meta.get('uppdateradTid') / 1000))
    occupation_concept_id = None
    occupation_group_concept_id = None
    occupation_field_concept_id = None
    municipality_concept_id = None
    region_concept_id = None
    country_concept_id = None
    # Fetch filtering details for unpublished ad from Elastic Search...
    ad_from_elastic = elastic.get_ad_by_id(ad_id)
    if ad_from_elastic:
        occupation = ad_from_elastic.get('occupation')
        if occupation:
            occupation_concept_id = occupation.get('concept_id')
        occupation_field = ad_from_elastic.get('occupation_field')
        if occupation_field:
            occupation_field_concept_id = occupation_field.get('concept_id')
        occupation_group = ad_from_elastic.get('occupation_group')
        if occupation_group:
            occupation_group_concept_id = occupation_group.get('concept_id')
        workplace_address = ad_from_elastic.get('workplace_address')
        if workplace_address:
            municipality_concept_id = workplace_address.get('municipality_concept_id')
            region_concept_id = workplace_address.get('region_concept_id')
            country_concept_id = workplace_address.get('country_concept_id')
    else:
        ad_from_elastic = elastic.get_ad_by_id(ad_id)
        log.info(f'Remove ads could fetch from elastic. id: {ad_id} get_id: {ad_from_elastic}')

    return {'annonsId': ad_id,
            'id': ad_id,
            'removed': True,
            'avpublicerad': True,
            'avpubliceringsdatum': removed_date,
            'removed_date': removed_date,
            'uppdateradTid': unpublished_ad_meta.get('uppdateradTid'),
            'updatedAt': unpublished_ad_meta.get('uppdateradTid'),
            'timestamp': unpublished_ad_meta.get('uppdateradTid'),
            'removed_ad_filter': {
                'occupation_concept_id': occupation_concept_id,
                'occupation_field_concept_id': occupation_field_concept_id,
                'occupation_group_concept_id': occupation_group_concept_id,
                'municipality_concept_id': municipality_concept_id,
                'region_concept_id': region_concept_id,
                'country_concept_id': country_concept_id}
            }


def load_ad_details(ad_meta):
    fail_count = 0
    fail_max = settings.LA_ANNONS_MAX_TRY
    ad_id = ad_meta['annonsId']
    # If ad is unpublished; handle it as removed...
    if ad_meta.get('avpublicerad', False):
        return load_ad_to_remove(ad_meta)

    detail_url_la = settings.LA_DETAILS_URL + str(ad_id)
    while True:
        try:
            r = requests.get(detail_url_la, timeout=settings.LA_ANNONS_TIMEOUT)
            r.raise_for_status()
            ad = r.json()
            if ad:
                ad['id'] = str(ad['annonsId'])
                ad['updatedAt'] = ad_meta['uppdateradTid']
                ad['expiresAt'] = ad['sistaPubliceringsdatum']
                ad['logo_url'] = find_correct_logo_url(ad.get('arbetsplatsId'),
                                                       ad.get('organisationsnummer'))
                desensitized_ad = _clean_sensitive_data(ad, detail_url_la)
                clean_ad = _cleanup_stringvalues(desensitized_ad)
                return clean_ad
        # On fail, try again 10 times with 0.3 second delay
        except requests.exceptions.ConnectionError as e:
            fail_count += 1
            time.sleep(0.3)
            log.warning(f"Unable to load data from: {detail_url_la} Connection error, try: {fail_count}")
            if fail_count >= fail_max:
                log.error(f"Failed to load data from: {detail_url_la} after: {fail_max}. {e} Exit!")
                sys.exit(1)
        except requests.exceptions.Timeout as e:
            fail_count += 1
            time.sleep(0.3)
            log.warning(f"Unable to load data from: {detail_url_la} Timeout, try: {fail_count}")
            if fail_count >= fail_max:
                log.error(f"Failed to load data from: {detail_url_la} after: {fail_max}. {e} Exit!")
                sys.exit(1)
        except requests.exceptions.RequestException as e:
            fail_count += 1
            time.sleep(0.3)
            log.warning(f"Unable to fetch data at: {detail_url_la}, try: {fail_count}, {e}")
            if fail_count >= fail_max:
                log.error(f"Failed to fetch: {detail_url_la} after: {fail_max}, skipping. {e}")
                raise e


def _clean_sensitive_data(ad, detail_url):
    if 'organisationsnummer' in ad and len(ad.get('organisationsnummer', '').strip()) > 9:
        orgnr = ad['organisationsnummer']
        significate_number_position = 4 if len(orgnr) == 12 else 2
        try:
            if int(orgnr[significate_number_position]) < 2:
                ad['organisationsnummer'] = None
        except ValueError:
            ad['organisationsnummer'] = None
            log.error(f"Value error in loader for orgnummer, ad {detail_url}. Replacing with None.")
    return ad


def load_list_of_updated_ads(timestamp=0):
    items = []
    feed_url = settings.LA_BOOTSTRAP_FEED_URL \
        if timestamp == 0 else settings.LA_FEED_URL + str(timestamp)
    log.info(f"Loading updates from endpoint: {feed_url}")
    fail_count = 0
    fail_max = settings.LA_ANNONS_MAX_TRY
    while True:
        try:
            r = requests.get(feed_url, timeout=60)
            r.raise_for_status()
            json_result = r.json()
            items = json_result.get('idLista', [])
        # On fail, try again 10 times with 0.3 second delay
        except (
                requests.exceptions.RequestException) as e:
            fail_count += 1
            time.sleep(0.3)
            log.warning(
                f"Unable to load data from: {feed_url} Exception: {type(e).__name__} , try: {fail_count}")
            if fail_count >= fail_max:
                log.error(f"Failed to read from {feed_url} after: {fail_max}.")
                raise e
    return items


def find_correct_logo_url(workplace_id, org_number):
    global logo_cache
    logo_url = None
    cache_key = "%s-%s" % (str(workplace_id), str(org_number))
    if cache_key in logo_cache:
        logo_url = logo_cache.get(cache_key)
        log.debug(f"Returning cached logo for workplace-orgnr {cache_key}: {logo_url}")
        return logo_url

    cache_logo = False
    try:
        if workplace_id and int(workplace_id) > 0:
            possible_logo_url = "%sarbetsplatser/%s/logotyper/logo.png" \
                                % (settings.COMPANY_LOGO_BASE_URL, workplace_id)
            r = requests.head(possible_logo_url, timeout=settings.COMPANY_LOGO_TIMEOUT)
            if r.status_code == 200:
                logo_url = possible_logo_url
                cache_logo = True

        if not logo_url and org_number:
            possible_logo_url = '%sorganisation/%s/logotyper/logo.png' \
                                % (settings.COMPANY_LOGO_BASE_URL, org_number)
            r = requests.head(possible_logo_url, timeout=settings.COMPANY_LOGO_TIMEOUT)
            if r.status_code == 200:
                logo_url = possible_logo_url
                cache_logo = True

    except requests.exceptions.ReadTimeout as e:
        log.warning(f"Logo URL read timeout: {e}")
    except requests.exceptions.ConnectionError as e:
        log.warning(f"Logo URL connection error: {e}")

    if cache_logo:
        logo_cache[cache_key] = logo_url

    log.debug(f"Found logo url for workplace-orgnr {workplace_id}-{org_number}:{logo_url}")
    return logo_url


def _cleanup_stringvalues(value):
    if isinstance(value, dict):
        value = {_cleanup_stringvalues(k): _cleanup_stringvalues(v)
                 for k, v in value.items()}
    elif isinstance(value, list):
        value = [_cleanup_stringvalues(v) for v in value]
    elif isinstance(value, str):
        value = ''.join([i if ord(i) > 0 else '' for i in value])
    return value
