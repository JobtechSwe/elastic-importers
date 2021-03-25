import time
import logging
import sys
import math
import itertools
from datetime import datetime

import requests
from jobtech.common.customlogging import configure_logging
import importers.mappings
from importers import settings
from importers.platsannons import loader, converter, enricher_mt_rest_multiple as enricher
from importers.repository import elastic
from importers.indexmaint.main import set_platsannons_read_alias, set_platsannons_write_alias, \
    check_index_size_before_switching_alias
from index_from_file.file_handling import save_enriched_ads_to_file

configure_logging([__name__.split('.')[0], 'importers'])
log = logging.getLogger(__name__)
enriched_ads_to_save = []


def _setup_index(es_index):
    if len(sys.argv) > 1:
        es_index = sys.argv[1]

    try:
        es_index, delete_index = elastic.setup_indices(es_index,
                                                       settings.ES_ANNONS_PREFIX,
                                                       importers.mappings.platsannons_mappings,
                                                       importers.mappings.platsannons_deleted_mappings)
        log.info(f'Starting importer with batch: {settings.PG_BATCH_SIZE} for index: {es_index}')
        log.info(f'Index for removed items: {delete_index}')
    except Exception as e:
        log.error(f"Elastic operations failed. Exit! {e}")
        sys.exit(1)
    return es_index, delete_index


def _check_last_timestamp(es_index):
    if not settings.LA_LAST_TIMESTAMP_MANUAL:
        last_timestamp = elastic.get_last_timestamp(es_index)
        log.info("Index: %s Last timestamp: %d (%s)"
                 % (es_index, last_timestamp, datetime.fromtimestamp(last_timestamp / 1000)))
    else:
        last_timestamp = settings.LA_LAST_TIMESTAMP
        log.warning("Index: %s Last timestamp set MANUALLY: %d (%s)"
                    % (es_index, last_timestamp, datetime.fromtimestamp(last_timestamp / 1000)))
    return last_timestamp


def start(es_index=None):
    start_time = time.time()
    # Get, set and create elastic index
    es_index, es_index_deleted = _setup_index(es_index)
    log.info("Starting ad import into index: %s" % es_index)
    last_timestamp = _check_last_timestamp(es_index)
    log.info("Timestamp to load from: %d" % last_timestamp)

    if not settings.LA_FEED_URL:
        log.error("LA_FEED_URL is not set. Exit!")
        sys.exit(1)

    # Load list of updated ad ids
    ad_ids = loader.load_list_of_updated_ads(last_timestamp)
    number_total = len(ad_ids)
    number_of_ids_to_load = number_total
    # variable to skip endless loop of fetching missing ads:
    number_of_ids_missing_fix = -1
    while number_of_ids_to_load > 0:
        log.info(f'Fetching details for ads: {number_of_ids_to_load}')
        _load_and_process_ads(ad_ids, es_index, es_index_deleted)
        log.info(f"Verifying that all ads were indexed in: {es_index}")
        ad_ids = _find_missing_ids_and_create_loadinglist(ad_ids, es_index)
        number_of_ids_to_load = len(ad_ids)

        if number_of_ids_missing_fix == number_of_ids_to_load:
            log.error(f"Missing ads amount is same as before: {number_of_ids_to_load}")
            log.error(f"No more trying to fetch. Check these ads: {ad_ids}")
            break
        if number_of_ids_to_load > 0:
            number_of_ids_missing_fix = number_of_ids_to_load
            log.info(f"Missing ads: {ad_ids}")
            log.warning(f"Still missing ads: {number_of_ids_to_load}. Trying again...")
        else:
            log.info("No missing ads to load.")
            break

    elapsed_time = time.time() - start_time
    m, s = divmod(elapsed_time, 60)
    log.info("Processed %d docs in: %d minutes %5.2f seconds." % (number_total, m, s))

    num_doc_elastic = elastic.document_count(es_index)
    if num_doc_elastic:
        log.info(f"Index: {es_index} has: {num_doc_elastic} indexed documents.")
    if not check_index_size_before_switching_alias(es_index):
        log.error(f"Index: {es_index} has: {num_doc_elastic} indexed documents. Exit!")
        sys.exit(1)


def _load_and_process_ads(ad_ids, es_index, es_index_deleted):
    doc_counter = 0
    len_ads = len(ad_ids)
    nr_of_items_per_batch = settings.PG_BATCH_SIZE
    nr_of_items_per_batch = min(nr_of_items_per_batch, len_ads)
    if nr_of_items_per_batch < 1:
        log.error("Failed to retrieve any ads. Exit!")
        sys.exit(1)
    nr_of_batches = math.ceil(len_ads / nr_of_items_per_batch)
    # Partition list into manageable chunks
    ad_batches = _grouper(nr_of_items_per_batch, ad_ids)
    processed_ads_total = 0
    taxonomy_2 = _get_taxonomy_multiple_versions()

    for i, ad_batch in enumerate(ad_batches):
        log.info('Processing batch %s/%s' % (i + 1, nr_of_batches))

        # Fetch ads from LA to raw-list
        ad_details = loader.bulk_fetch_ad_details(ad_batch)

        raw_ads = [raw_ad for raw_ad in list(ad_details.values())
                   if not raw_ad.get('removed', False)]
        doc_counter += len(raw_ads)
        log.info(f'doc_counter=len(raw_ads): {doc_counter}')
        log.info(f'Fetched batch of ads (id, updatedAt): '
                 f'{", ".join(("(" + str(ad["annonsId"]) + ", " + str(ad["updatedAt"])) + ")" for ad in raw_ads)}')

        _convert_and_save_to_elastic(ad_details.values(), es_index, es_index_deleted, taxonomy_2)
        processed_ads_total = processed_ads_total + len(ad_batch)

        log.info(f'Processed ads: {processed_ads_total}/{len_ads}')
    save_enriched_ads()

    return doc_counter


def save_enriched_ads():
    """
    saves enriched ads to a Pickle file that can be used when creating an index from a known state
    """
    if settings.SAVE_ENRICHED_ADS:
        save_enriched_ads_to_file(enriched_ads_to_save)


def _get_taxonomy_multiple_versions():
    headers = {"api-key": settings.TAXONOMY_2_API_KEY, }
    taxonomy_2 = requests.get(settings.TAXONOMY_2_URL, headers=headers)
    return taxonomy_2.json()


def _convert_and_save_to_elastic(raw_ads, es_index, deleted_index, taxonomy_2):
    # Loop over raw-list, convert and enrich into cooked-list
    log.info(f"Converting: {len(raw_ads)} ads to proper format ...")
    converted_ads = [converter.convert_ad(raw_ad, taxonomy_2) for raw_ad in raw_ads]
    log.info("Enriching ads with ML ...")
    enriched_ads = enricher.enrich(converted_ads)

    if settings.SAVE_ENRICHED_ADS:
        global enriched_ads_to_save
        enriched_ads_to_save.extend(enriched_ads)

    log.info(f"Indexing: {len(enriched_ads)} enriched documents into: {es_index}")
    # Bulk save cooked-list to elastic
    num_indexed = elastic.bulk_index(enriched_ads, es_index, deleted_index)
    return num_indexed


def _find_missing_ids_and_create_loadinglist(ad_ids, es_index):
    id_lookup = {str(a['annonsId']): a for a in ad_ids if not a['avpublicerad']}
    loaded_ids = [str(a['annonsId']) for a in ad_ids if not a['avpublicerad']]
    missing_ids = elastic.find_missing_ad_ids(loaded_ids, es_index)
    return [id_lookup[missing_id] for missing_id in missing_ids]


def _grouper(n, iterable):
    iterable = iter(iterable)
    return iter(lambda: list(itertools.islice(iterable, n)), [])


def start_daily_index():
    new_index_name = "%s-%s" % (settings.ES_ANNONS_PREFIX, datetime.now().strftime('%Y%m%d-%H.%M'))
    log.info(f"Start creating new daily index: {new_index_name}")
    start(new_index_name)
    set_platsannons_read_alias(new_index_name)
    set_platsannons_write_alias(new_index_name)
    log.info(f"Switching alias to new index completed successfully: {new_index_name}")


if __name__ == '__main__':
    configure_logging([__name__, 'importers'])
    log = logging.getLogger(__name__)
    start_daily_index()
