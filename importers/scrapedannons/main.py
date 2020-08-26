import concurrent.futures
import itertools
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


def _setup_index(es_index):
    if len(sys.argv) > 1:
        es_index = sys.argv[1]

    try:
        es_index = elastic.setup_indices(
            es_index,
            settings.ES_SCRAPED_ANNONS_PREFIX,
            settings.scrapedannons_mappings)
        log.info(f'Starting importer with batch: {settings.PG_BATCH_SIZE} for index: {es_index}')
    except Exception as e:
        log.error(f"Elastic operations failed. Exit! {e}")
        sys.exit(1)
    return es_index[0]


def _grouper(n, iterable):
    iterable = iter(iterable)
    return iter(lambda: list(itertools.islice(iterable, n)), [])


def _convert_and_save_to_elastic(raw_ads, es_index):
    log.info(f"Converting: {len(raw_ads)} ads to proper format ...")
    log.info("Enriching ads with ML ...")
    enriched_ads = enricher.enrich(raw_ads, True)
    log.info(f"Indexing: {len(enriched_ads)} enriched documents into: {es_index}")
    # Bulk save cooked-list to elastic
    num_indexed = elastic.bulk_index(enriched_ads, es_index)
    return num_indexed


def bulk_fetch_ad_details(ad_batch, es_index):
    len_ad_batch = len(ad_batch)
    parallelism = settings.LA_DETAILS_PARALLELISM if len_ad_batch > 99 else 1
    log.info(f'Fetch ad details. Processes: {parallelism}, batch len: {len_ad_batch}')

    global counter
    counter = Value('i', 0)
    result_output = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        for ad_meta in ad_batch:
            if ad_meta.get('id', ''):
                detailed_result = convert_ad(ad_meta)
                result_output.append(detailed_result)
    _convert_and_save_to_elastic(result_output, es_index)

    return result_output


def open_the_file(es_index):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_name = 'test_data.json'
    with open(dir_path + "/resources/" + file_name, 'r') as fil:
        data = json.load(fil)
        ads = bulk_fetch_ad_details(data, es_index)
    return ads


def start(es_index=None):
    start_time = time.time()
    es_index = _setup_index(es_index)
    log.info("Starting ad import into index")
    ad_ids = open_the_file(es_index)
    number_total = len(ad_ids)

    elapsed_time = time.time() - start_time
    m, s = divmod(elapsed_time, 60)
    log.info("Processed %d docs in: %d minutes %5.2f seconds." % (number_total, m, s))


if __name__ == '__main__':
    configure_logging([__name__, 'importers'])
    log = logging.getLogger(__name__)
