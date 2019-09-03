import sys
import logging
import time
from datetime import datetime
from jobtech.common.customlogging import configure_logging
from importers.repository import elastic, postgresql
from importers.platsannons import converter
from importers import settings
from importers import common
from importers.platsannons import enricher_mt_rest_multiple as enricher
from importers.platsannons import enricher_company_logo
from importers.indexmaint.main import (set_platsannons_read_alias,
                                       set_platsannons_write_alias)
from importers.platsannons import loader_platsannonser


configure_logging([__name__.split('.')[0], 'jobtech'])

log = logging.getLogger(__name__)
IMPORTER_NAME = 'af-platsannons'


def start(es_index=None):
    if len(sys.argv) > 1:
        es_index = sys.argv[1]

    start_time = time.time()
    try:
        es_index = elastic.setup_indices(es_index, settings.ES_ANNONS_PREFIX,
                                         settings.platsannons_mappings)
        log.info('Starting importer %s with PG_BATCH_SIZE: %s for index %s'
                 % (IMPORTER_NAME, settings.PG_BATCH_SIZE, es_index))
        last_timestamp = elastic.get_last_timestamp(es_index)
        log.info("Last timestamp: %d (%s)" % (last_timestamp,
                                              datetime.fromtimestamp(
                                                  (last_timestamp+1)/1000)
                                              ))
        last_identifiers = elastic.get_ids_with_timestamp(last_timestamp,
                                                          es_index)
    except Exception as e:
        log.error("Elastic operations failed: %s" % str(e))
        sys.exit(1)

    doc_counter = 0

    updated_ad_ids = loader_platsannonser.fetch_updated_ads(last_timestamp, last_identifiers)
    log.info('Found %s updated ad ids to handle...', len(updated_ad_ids))
    while len(updated_ad_ids) > 0:
        _update_removed_ads(updated_ad_ids)

        updated_published_ad_ids = [ad for ad in updated_ad_ids
                                    if ad['avpublicerad'] is False]

        if len(updated_published_ad_ids) > 0:
            log.info('Found %s updated published ads' % len(updated_published_ad_ids))
            # Save published ads
            fetch_details_and_save(updated_published_ad_ids)

        # Check if there are more ads to fetch and save
        last_ids_temp, last_ts_temp = get_system_status_platsannonser()
        if set(last_ids) == set(last_ids_temp) and last_ts == last_ts_temp:
            log.info("Same ids and timestamps as last try, aborting.")
            updated_ad_ids = []
        else:
            last_ts = last_ts_temp
            last_ids = last_ids_temp.copy()
            updated_ad_ids = loader_platsannonser.fetch_updated_ads(last_ts, last_ids)
            log.info('Found %s updated ad ids to handle...', len(updated_ad_ids))


def _update_removed_ads(updated_ad_ids):
    removed_ad_ids = [ad for ad in updated_ad_ids if ad['avpublicerad'] is True]
    if len(removed_ad_ids) > 0:
        log.info('Found %s removed ads' % len(removed_ad_ids))
        for removed_ad_id in removed_ad_ids:
            ad_id = removed_ad_id['annonsId']
            ad_timestamp = removed_ad_id['uppdateradTid']
            ad_to_update = postgresql.fetch_ad(ad_id, settings.PG_PLATSANNONS_TABLE)

            if ad_to_update:
                doc = ad_to_update[1]
                doc['avpublicerad'] = True
                get_and_set_removed_date(ad_id, ad_timestamp, doc)

                log.info('Updating removed ad id %s (timestamp: %s) in postgres'
                         % (ad_id, ad_timestamp))
                postgresql.update_ad(ad_id, doc, ad_timestamp,
                                     settings.PG_PLATSANNONS_TABLE)
            else:
                log.info('Could not find removed ad id %s (timestamp: %s) '
                         'in postgres, skipping update' % (
                             ad_id, ad_timestamp))



def old_start(es_index=None):
    if len(sys.argv) > 1:
        es_index = sys.argv[1]

    start_time = time.time()
    try:
        es_index = elastic.setup_indices(es_index, settings.ES_ANNONS_PREFIX,
                                         settings.platsannons_mappings)
        log.info('Starting importer %s with PG_BATCH_SIZE: %s for index %s'
                 % (IMPORTER_NAME, settings.PG_BATCH_SIZE, es_index))
        last_timestamp = elastic.get_last_timestamp(es_index)
        log.info("Last timestamp: %d (%s)" % (last_timestamp,
                                              datetime.fromtimestamp(
                                                  (last_timestamp+1)/1000)
                                              ))
        last_identifiers = elastic.get_ids_with_timestamp(last_timestamp,
                                                          es_index)
    except Exception as e:
        log.error("Elastic operations failed: %s" % str(e))
        sys.exit(1)

    doc_counter = 0

    while True:
        (last_identifiers, last_timestamp, platsannonser) = \
            postgresql.read_from_pg_since(last_identifiers, last_timestamp,
                                          settings.PG_PLATSANNONS_TABLE, converter)
        current_doc_count = len(platsannonser)
        doc_counter += current_doc_count

        if platsannonser:
            try:
                platsannonser = enricher_company_logo.enrich(platsannonser)
                enriched_ads = enricher.enrich(platsannonser,
                                               parallelism=settings.ENRICHER_PROCESSES)
                elastic.bulk_index(enriched_ads, es_index)
                log.info("Indexed %d docs so far." % doc_counter)
            except Exception as e:
                log.error("Import failed", e)
                sys.exit(1)
            common.log_import_metrics(log, IMPORTER_NAME, current_doc_count)
            # if doc_counter >= 9900:
            #     break
        else:
            break

    elapsed_time = time.time() - start_time

    log.info("Indexed %d docs in: %s seconds." % (doc_counter, elapsed_time))


def start_daily_index():
    new_index_name = "platsannons-%s" % datetime.now().strftime('%Y%m%d-%H')
    start(new_index_name)
    set_platsannons_read_alias(new_index_name)
    set_platsannons_write_alias(new_index_name)


if __name__ == '__main__':
    start()
