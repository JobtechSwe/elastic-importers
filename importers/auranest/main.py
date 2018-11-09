import time
import logging
from importers.repository import elastic, postgresql
from importers.auranest import enricher
from importers import settings

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)

log = logging.getLogger(__name__)


def start():
    start_time = time.time()
    if not elastic.index_exists(settings.ES_AURANEST_INDEX):
        elastic.create_index(settings.ES_AURANEST_INDEX, settings.auranest_mappings)
    last_timestamp = elastic.get_last_timestamp(settings.ES_AURANEST_INDEX)
    print("Last timestamp: %d" % last_timestamp)
    last_identifiers = elastic.get_ids_with_timestamp(last_timestamp,
                                                      settings.ES_AURANEST_INDEX)
    doc_counter = 0

    while True:
        (last_identifiers, last_timestamp, annonser) = \
            postgresql.read_from_pg_since(last_identifiers,
                                          last_timestamp, 'auranest')
        doc_counter += len(annonser)
        print("Read %d ads" % doc_counter)
        if annonser:
            enhanced = enricher.enrich(annonser)
            log.info("Indexed %d docs so far." % doc_counter)
            elastic.bulk_index(enhanced, settings.ES_AURANEST_INDEX)
        else:
            break

    elapsed_time = time.time() - start_time

    print("Indexed %d docs in: %s seconds." % (doc_counter, elapsed_time))


if __name__ == '__main__':
    start()
