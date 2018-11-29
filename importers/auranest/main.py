import sys
import time
import logging
from importers.repository import elastic, postgresql
from importers.auranest import enricher
from importers import settings
from importers import common

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)

log = logging.getLogger(__name__)

IMPORTER_NAME = 'auranest'


def start():
    log.info("Starting auranest import")
    start_time = time.time()
    es_index = elastic.setup_indices(sys.argv, settings.ES_AURANEST_PREFIX,
                                     settings.auranest_mappings)
    last_timestamp = elastic.get_last_timestamp(es_index)
    log.debug("Last timestamp: %d" % last_timestamp)
    last_identifiers = elastic.get_ids_with_timestamp(last_timestamp,
                                                      es_index)
    doc_counter = 0

    while True:
        (last_identifiers, last_timestamp, annonser) = \
            postgresql.read_from_pg_since(last_identifiers,
                                          last_timestamp, 'auranest')
        current_doc_count = len(annonser)
        doc_counter += current_doc_count
        log.debug("Read %d ads" % doc_counter)

        if annonser:
            enhanced = enricher.enrich(annonser)
            log.debug("Indexed %d docs so far." % doc_counter)
            elastic.bulk_index(enhanced, es_index)
            common.log_import_metrics(log, IMPORTER_NAME, current_doc_count)
        else:
            break

    elapsed_time = time.time() - start_time

    log.info("Indexed %d docs in: %s seconds." % (doc_counter, elapsed_time))


if __name__ == '__main__':
    start()
