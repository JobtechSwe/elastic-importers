import itertools
import math
import sys
import time
import logging

from importers import settings
from importers.common import log
from jobtech.common.customlogging import configure_logging
from importers.platsannons.main import _check_last_timestamp
from importers.repository import elastic
from importers.scrapedannons import loader
from importers.scrapedannons.loader import get_all_ads_url_from_onepartnergroup
from importers.platsannons import enricher_mt_rest_multiple as enricher


def _setup_index(es_index):
    if len(sys.argv) > 1:
        es_index = sys.argv[1]

    try:
        es_index, delete_index = elastic.setup_indices(es_index,
                                                       settings.ES_SCRAPED_ANNONS_PREFIX,
                                                       settings.platsannons_mappings,
                                                       settings.platsannons_deleted_mappings)
        log.info(f'Starting importer with batch: {settings.PG_BATCH_SIZE} for index: {es_index}')
        log.info(f'Index for removed items: {delete_index}')
    except Exception as e:
        log.error(f"Elastic operations failed. Exit! {e}")
        sys.exit(1)
    return es_index, delete_index


def _grouper(n, iterable):
    iterable = iter(iterable)
    return iter(lambda: list(itertools.islice(iterable, n)), [])


def _convert_and_save_to_elastic(raw_ads, es_index):
    # Loop over raw-list, convert and enrich into cooked-list
    log.info(f"Converting: {len(raw_ads)} ads to proper format ...")
    converted_ads = [raw_ad for raw_ad in raw_ads]
    log.info("Enriching ads with ML ...")
    enriched_ads = enricher.enrich(converted_ads)
    log.info(f"Indexing: {len(enriched_ads)} enriched documents into: {es_index}")
    # Bulk save cooked-list to elastic
    num_indexed = elastic.bulk_index(enriched_ads, es_index)
    return num_indexed


def _load_and_process_ads(ad_ids, es_index):
    doc_counter = 0
    len_ads = len(ad_ids)
    nr_of_items_per_batch = settings.PG_BATCH_SIZE
    nr_of_items_per_batch = min(nr_of_items_per_batch, len_ads)
    if nr_of_items_per_batch < 1:
        log.error("Failed to retrieve any ads. Exit!")
        sys.exit(1)
    nr_of_batches = math.ceil(len_ads / nr_of_items_per_batch)

    ad_batches = _grouper(nr_of_items_per_batch, ad_ids)
    processed_ads_total = 0
    for i, ad_batch in enumerate(ad_batches):
        log.info('Processing batch %s/%s' % (i + 1, nr_of_batches))
        ad_details = loader.bulk_fetch_ad_details(ad_batch)

        _convert_and_save_to_elastic(ad_details.values(), es_index)
        processed_ads_total = processed_ads_total + len(ad_batch)

        log.info(f'Processed ads: {processed_ads_total}/{len_ads}')

    return doc_counter


def start(es_index=None):
    start_time = time.time()
    es_index, es_index_deleted = _setup_index(es_index)
    log.info("Starting ad import into index: %s" % es_index)
    last_timestamp = _check_last_timestamp(es_index)
    log.info("Timestamp to load from: %d" % last_timestamp)

    if not settings.participants_webs['onepartnergroup']:
        log.error("'onepartnergroup' is not set. Exit!")
        sys.exit(1)

    ad_ids = get_all_ads_url_from_onepartnergroup()
    _load_and_process_ads(ad_ids, es_index)
    number_total = len(ad_ids)

    elapsed_time = time.time() - start_time
    m, s = divmod(elapsed_time, 60)
    log.info("Processed %d docs in: %d minutes %5.2f seconds." % (number_total, m, s))


if __name__ == '__main__':
    configure_logging([__name__, 'importers'])
    log = logging.getLogger(__name__)
