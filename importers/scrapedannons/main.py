import concurrent.futures
import itertools
import math
import sys
import time
import logging
from multiprocessing import Value

from importers import settings
from importers.common import log
from importers.scrapedannons.converter import convert_ad
from jobtech.common.customlogging import configure_logging, os, json
from importers.repository import elastic
from importers.platsannons import enricher_mt_rest_multiple as enricher

configure_logging([__name__.split('.')[0], 'importers'])
log = logging.getLogger(__name__)


def _setup_index(es_index):
    if len(sys.argv) > 1:
        es_index = sys.argv[1]

    try:
        es_index = elastic.setup_indices(
            es_index,
            settings.ES_SCRAPED_ANNONS_PREFIX,
            settings.scrapedannons_mappings)
        log.info(f'Starting importer with batch: {settings.PG_BATCH_SIZE} for index: {es_index[0]}')
    except Exception as e:
        log.error(f"Elastic operations failed. Exit! {e}")
        sys.exit(1)
    return es_index[0]


def _grouper(n, iterable):
    iterable = iter(iterable)
    return iter(lambda: list(itertools.islice(iterable, n)), [])


def _enrich_and_save_to_elastic(raw_ads, es_index):
    log.info("Enriching ads with ML ...")
    enriched_ads = enricher.enrich(raw_ads, scraped=True, typeahead=False)
    log.info(f"Indexing: {len(enriched_ads)} enriched documents into: {es_index}")
    # Delete enriched ads description part
    for enriched_ad in enriched_ads:
        del enriched_ad['originalJobPosting']['description']
    # Bulk save cooked-list to elastic
    num_indexed = elastic.bulk_index(enriched_ads, es_index)
    return num_indexed


def bulk_fetch_ad_details(ad_ids, es_index):
    len_ad_batch = len(ad_ids)
    parallelism = settings.LA_DETAILS_PARALLELISM if len_ad_batch > 100 else 1
    log.info(f'Fetch ad details. Processes: {parallelism}, batch len: {len_ad_batch}')
    len_ads = len(ad_ids)
    nr_of_items_per_batch = min(500, len_ads)
    if nr_of_items_per_batch < 1:
        log.error("Failed to retrieve any ads. Exit!")
        sys.exit(1)

    ad_batches = _grouper(nr_of_items_per_batch, ad_ids)

    for i, ad_batch in enumerate(ad_batches):
        result_output = []
        for ad in ad_batch:
            if ad.get('id', ''):
                detailed_result = convert_ad(ad)
                result_output.append(detailed_result)
        log.info(f"Converted: {len(result_output)} ads to proper format ...")
        _enrich_and_save_to_elastic(result_output, es_index)

    return len_ads


def open_the_file(es_index):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    log.info(f"File to be loaded: {settings.SCRAPED_FILE}")

    data = []
    with open(dir_path + "/resources/" + settings.SCRAPED_FILE, 'r', encoding='utf-8') as data_file:
        for item in data_file.readlines():
            data.append(json.loads(item))
        ads = bulk_fetch_ad_details(data, es_index)
    return ads


def start(es_index=None):
    start_time = time.time()
    es_index = _setup_index(es_index)
    log.info("Starting ad import into index")
    len_ads = open_the_file(es_index)
    number_total = len_ads
    elapsed_time = time.time() - start_time
    m, s = divmod(elapsed_time, 60)
    log.info("Processed %d docs in: %d minutes %5.2f seconds." % (number_total, m, s))


if __name__ == '__main__':
    configure_logging([__name__, 'importers'])
    log = logging.getLogger(__name__)
