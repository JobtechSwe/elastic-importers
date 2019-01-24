import sys
import logging
import time
from datetime import datetime
from importers.repository import elastic, postgresql
from importers.platsannons import converter
from importers import settings
from importers import common
# from importers.platsannons import enricher_mt_rest_multiple

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)

log = logging.getLogger(__name__)
IMPORTER_NAME = 'af-platsannons'


def start():
    start_time = time.time()
    try:
        es_index = elastic.setup_indices(sys.argv, settings.ES_ANNONS_PREFIX,
                                         settings.platsannons_mappings)
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
                # enriched_platsannonser = enricher_mt_rest_multiple.enrich(platsannonser, parallelism=8)
                # elastic.bulk_index(enriched_platsannonser, es_index)
                elastic.bulk_index(platsannonser, es_index)
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


if __name__ == '__main__':
    start()
