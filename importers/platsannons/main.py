import time
import logging
import sys
import math
import itertools
from datetime import datetime
from jobtech.common.customlogging import configure_logging
from importers import settings
from importers.platsannons import loader, converter, enricher_mt_rest_multiple as enricher
from importers.repository import elastic, postgresql
from importers.indexmaint.main import (set_platsannons_read_alias,
                                       set_platsannons_write_alias)

configure_logging([__name__.split('.')[0], 'importers'])
log = logging.getLogger(__name__)


def _setup_index(es_index):
    if len(sys.argv) > 1:
        es_index = sys.argv[1]

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
    # last_identifiers = elastic.get_ids_with_timestamp(last_timestamp,
    #                                                   es_index)
    return last_timestamp


def start(es_index=None):
    start_time = time.time()
    # Get, set and create elastic index
    es_index = _setup_index(es_index)
    log.info("Starting ad import into index: %s" % es_index)
    last_timestamp = _check_last_timestamp(es_index)
    doc_counter = 0
    # Check psql table existance
    # Load list of updated ad ids
    ad_ids = loader.load_list_of_updated_ads(last_timestamp)
    # Partition list into manageable chunks

    log.info('Fetching details for %s ads...' % len(ad_ids))
    nr_of_items_per_batch = int(settings.PG_BATCH_SIZE)
    nr_of_items_per_batch = min(nr_of_items_per_batch, len(ad_ids))
    nr_of_batches = math.ceil(len(ad_ids) / nr_of_items_per_batch)

    ad_batches = _grouper(nr_of_items_per_batch, ad_ids)
    
    processed_ads_total = 0
    failed_ads = []
    for i, ad_batch in enumerate(ad_batches):
        log.info('Processing batch %s/%s' % (i + 1, nr_of_batches))

        # Fetch ads from LA to raw-list
        log.info("Loading ad details.")
        ad_details, batch_failed_ads = loader.bulk_fetch_ad_details(ad_batch)

        doc_counter += (len(ad_details) - len(batch_failed_ads))

        for failed_ad in batch_failed_ads.copy():
            # On fail, check for ad in postgresql
            log.info("Working through list of failed ads")
            failed_id = failed_ad['annonsId']
            pgsql_ad = postgresql.fetch_ad(failed_id, settings.PG_PLATSANNONS_TABLE)
            if pgsql_ad:
                ad_details[failed_id] = pgsql_ad
                batch_failed_ads.remove(failed_ad)
            # On fail, keep ID in failed-list

        failed_ads.extend(batch_failed_ads)
        raw_ads = [raw_ad for raw_ad in list(ad_details.values())
                   if not raw_ad.get('avpublicerad', False)]
        deleted_ids = [raw_ad['annonsId'] for raw_ad in list(ad_details.values())
                       if raw_ad.get('avpublicerad', False)]
        # Save raw-list to postgresql
        postgresql.bulk(raw_ads, settings.PG_PLATSANNONS_TABLE)
        # Set expired on all removed ads
        postgresql.set_expired_for_ids(settings.PG_PLATSANNONS_TABLE, deleted_ids)

        # Loop over raw-list, convert and enrich into cooked-list
        converted_ads = [converter.convert_ad(raw_ad) for raw_ad in raw_ads]
        enriched_ads = enricher.enrich(converted_ads)
        # Bulk save cooked-list to elastic
        log.debug("Indexing %d documents into %s" % (len(enriched_ads), es_index))
        elastic.bulk_index(enriched_ads, es_index)
        processed_ads_total = processed_ads_total + len(ad_batch)

        log.info('Processed %s/%s ads' % (processed_ads_total, len(ad_ids)))

    # Iterate over failed-list, trying to find in LA, postgresql
    log.info("Last pass, trying to load failed ads from LA")
    recovered_ads = []
    for failed_ad in failed_ads.copy():
        recovered_ad = loader.load_details_from_la(failed_ad)
        if recovered_ad:
            recovered_ads.append(recovered_ad)
            failed_ads.remove(failed_ad)

    # Save any recovered ads
    if recovered_ads:
        converted_ads = [converter.convert_ad(recovered_ad)
                         for recovered_ad in recovered_ads]
        enriched_ads = enricher.enrich(converted_ads)
        elastic.bulk_index(enriched_ads, es_index)
    if failed_ads:
        log.warning("There are %d ads reported from stream that can't be downloaded.",
                    len(failed_ads))

    elapsed_time = time.time() - start_time
    log.info("Processed %d docs in: %s seconds." % (doc_counter, elapsed_time))


def _grouper(n, iterable):
    iterable = iter(iterable)
    return iter(lambda: list(itertools.islice(iterable, n)), [])


def start_daily_index():
    new_index_name = "platsannons-%s" % datetime.now().strftime('%Y%m%d-%H')
    start(new_index_name)
    set_platsannons_read_alias(new_index_name)
    set_platsannons_write_alias(new_index_name)


if __name__ == '__main__':
    configure_logging([__name__, 'importers'])
    log = logging.getLogger(__name__)
    start()
