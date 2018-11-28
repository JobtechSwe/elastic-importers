import logging
import time
from datetime import datetime
from importers.repository import elastic, postgresql
from importers.platsannons import converter
from importers import settings
from importers import common

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)

log = logging.getLogger(__name__)
IMPORTER_NAME = 'af-platsannons'

def start():
    start_time = time.time()
    if not elastic.index_exists(settings.ES_ANNONS_INDEX):
        log.info("Creating index %s" % settings.ES_ANNONS_INDEX)
        elastic.create_index(settings.ES_ANNONS_INDEX, settings.platsannons_mappings)
    last_timestamp = elastic.get_last_timestamp(settings.ES_ANNONS_INDEX)
    log.info("Last timestamp: %d (%s)" % (last_timestamp,
                                          datetime.fromtimestamp(
                                              (last_timestamp+1)/1000)
                                          ))
    last_identifiers = elastic.get_ids_with_timestamp(last_timestamp,
                                                      settings.ES_ANNONS_INDEX)
    doc_counter = 0

    while True:
        (last_identifiers, last_timestamp, platsannonser) = \
            postgresql.read_from_pg_since(last_identifiers,
                                          last_timestamp, 'platsannonser', converter)
        current_doc_count = len(platsannonser)
        doc_counter += current_doc_count

        if platsannonser:
            log.debug("Indexed %d docs so far." % doc_counter)
            elastic.bulk_index(platsannonser, settings.ES_ANNONS_INDEX)
            common.log_import_metrics(log, IMPORTER_NAME, current_doc_count)
        else:
            break

    elapsed_time = time.time() - start_time

    log.info("Indexed %d docs in: %s seconds." % (doc_counter, elapsed_time))


if __name__ == '__main__':
    start()
