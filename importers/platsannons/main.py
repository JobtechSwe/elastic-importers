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
        es_index, delete_index = elastic.setup_indices(es_index,
                                                       settings.ES_ANNONS_PREFIX,
                                                       settings.platsannons_mappings,
                                                       settings.platsannons_deleted_mappings)
        log.info('Starting importer %s with PG_BATCH_SIZE: %s for index %s'
                 % ('af-platsannons', settings.PG_BATCH_SIZE, es_index))
        log.info('Index for removed items: %s' % delete_index)
    except Exception as e:
        log.error("Elastic operations failed. Exit! %s" % str(e))
        sys.exit(1)
    return es_index, delete_index


def _check_last_timestamp(es_index):
    last_timestamp = elastic.get_last_timestamp(es_index)
    log.info("Last timestamp: %d (%s)" % (last_timestamp,
                                          datetime.fromtimestamp(last_timestamp
                                                                 / 1000)))
    return last_timestamp


def start(es_index=None):
    start_time = time.time()
    # Get, set and create elastic index
    es_index, es_index_deleted = _setup_index(es_index)
    log.info("Starting ad import into index: %s" % es_index)
    last_timestamp = _check_last_timestamp(es_index)
    doc_counter = 0

    if not settings.LA_FEED_URL:
        # Try to load cached ads from database
        if not settings.PG_DBNAME:
            raise Exception('No config found for db, neither for REST feed.')
        _load_from_postgresql(last_timestamp, es_index)
        return

    # Load list of updated ad ids
    ad_ids = loader.load_list_of_updated_ads(last_timestamp)

    log.info('Fetching details for %s ads...' % len(ad_ids))
    nr_of_items_per_batch = int(settings.PG_BATCH_SIZE)
    nr_of_items_per_batch = min(nr_of_items_per_batch, len(ad_ids))
    if nr_of_items_per_batch < 1:
        log.error("Failed to retrieve any ads. Exit!")
        sys.exit(1)
    nr_of_batches = math.ceil(len(ad_ids) / nr_of_items_per_batch)
    # Partition list into manageable chunks
    ad_batches = _grouper(nr_of_items_per_batch, ad_ids)

    processed_ads_total = 0
    failed_ads = []
    for i, ad_batch in enumerate(ad_batches):
        log.info('Processing batch %s/%s' % (i + 1, nr_of_batches))

        # Fetch ads from LA to raw-list
        ad_details, batch_failed_ads = loader.bulk_fetch_ad_details(ad_batch)

        doc_counter += (len(ad_details) - len(batch_failed_ads))
        log.info("Batch: %d/%d Failed ads: %d"
                 % (i+1, nr_of_batches, len(batch_failed_ads)))

        for failed_ad in batch_failed_ads.copy():
            # On fail, check for ad in postgresql
            failed_id = failed_ad['annonsId']
            log.warning("Working through list of failed ads, total in batch: %d."
                        "Failed id: %s" % (len(batch_failed_ads.copy), failed_ad))
            pgsql_ad = postgresql.fetch_ad(failed_id, settings.PG_PLATSANNONS_TABLE)
            if pgsql_ad:
                ad_details[failed_id] = pgsql_ad
                batch_failed_ads.remove(failed_ad)
                log.info("OK getting failed ad from db, id: %s" % failed_ad)
            else:
                log.warning("NOK getting failed ad from db, id: %s" % failed_ad)
            # On fail, keep ID in failed-list

        failed_ads.extend(batch_failed_ads)
        raw_ads = [raw_ad for raw_ad in list(ad_details.values())
                   if not raw_ad.get('removed', False)]
        deleted_ids = [raw_ad['annonsId'] for raw_ad in list(ad_details.values())
                       if raw_ad.get('removed', False)]
        # Save raw-list to postgresql
        postgresql.bulk(raw_ads, settings.PG_PLATSANNONS_TABLE)
        log.debug(f'Postgresql bulked ads (id, updatedAt): '
                  f'{", ".join(("(" + str(ad["annonsId"]) + ", " + str(ad["updatedAt"])) + ")" for ad in raw_ads)}')
        # Set expired on all removed ads
        postgresql.set_expired_for_ids(settings.PG_PLATSANNONS_TABLE, deleted_ids)

        _convert_and_save_to_elastic(ad_details.values(), es_index, es_index_deleted)
        processed_ads_total = processed_ads_total + len(ad_batch)

        log.info('Processed %s/%s ads' % (processed_ads_total, len(ad_ids)))

    # Iterate over failed-list, trying to find in LA, db
    recovered_ads = []
    if failed_ads:
        log.info("Last pass, trying to load failed ads from LA")
        for failed_ad in failed_ads.copy():
            recovered_ad = loader.load_details_from_la(failed_ad)
            if recovered_ad:
                log.info("Successfully downloaded previously failed ad from LA:"
                         " %s" % recovered_ad['id'])
                recovered_ads.append(recovered_ad)
                failed_ads.remove(failed_ad)
            else:
                log.warning("Unsuccessfully downloaded previously failed ad "
                            "from LA: %s" % recovered_ad['id'])

    # Save any recovered ads
    if recovered_ads:
        num_idxd_ads = _convert_and_save_to_elastic(recovered_ads, es_index,
                                                    es_index_deleted)
        log.info("Indexed recovered ads: %d" % num_idxd_ads)
    if failed_ads:
        log.warning("Ads reported from stream that can't be downloaded: %d",
                    len(failed_ads))

    elapsed_time = time.time() - start_time
    m, s = divmod(elapsed_time, 60)
    log.info("Processed %d docs in: %d minutes %5.2f seconds." % (doc_counter, m, s))


def _load_from_postgresql(last_timestamp, es_index):
    log.info("No REST feed configuration detected, using database as source.")
    doc_counter = 0
    last_identifiers = elastic.get_ids_with_timestamp(last_timestamp, es_index)

    while True:
        (last_identifiers, last_timestamp, platsannonser) = \
            postgresql.read_from_pg_since(last_identifiers, last_timestamp,
                                          settings.PG_PLATSANNONS_TABLE)
        current_doc_count = len(platsannonser)
        doc_counter += current_doc_count

        if platsannonser:
            log.info("Still working ... ads indexed so far: %d" % doc_counter)
            _convert_and_save_to_elastic(platsannonser, es_index, None)
        else:
            log.info("Indexed ads into elastic: %d" % doc_counter)
            break


def _convert_and_save_to_elastic(raw_ads, es_index, deleted_index):
    # Loop over raw-list, convert and enrich into cooked-list
    converted_ads = [converter.convert_ad(raw_ad)
                     for raw_ad in raw_ads]
    enriched_ads = enricher.enrich(converted_ads)
    log.info("Indexing: %d enriched documents into: %s" % (len(enriched_ads), es_index))
    # Bulk save cooked-list to elastic
    elastic.bulk_index(enriched_ads, es_index, deleted_index)
    return len(enriched_ads)


def _grouper(n, iterable):
    iterable = iter(iterable)
    return iter(lambda: list(itertools.islice(iterable, n)), [])


def start_daily_index():
    new_index_name = "%s-%s" % (settings.ES_ANNONS_PREFIX,
                                datetime.now().strftime('%Y%m%d-%H'))
    start(new_index_name)
    set_platsannons_read_alias(new_index_name)
    set_platsannons_write_alias(new_index_name)


if __name__ == '__main__':
    configure_logging([__name__, 'importers'])
    log = logging.getLogger(__name__)
    start()
