import sys
import time
import logging
import json
from importers.repository import elastic, postgresql
from importers import settings
from importers import common
from importers.auranest import enricher_mt_rest_multiple as enr

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)

log = logging.getLogger(__name__)

IMPORTER_NAME = 'auranest'


def start():
    log.info('Starting importer %s with PG_BATCH_SIZE: %s'
             % (IMPORTER_NAME, settings.PG_BATCH_SIZE))
    start_time = time.time()
    try:
        es_index = elastic.setup_indices(sys.argv, settings.ES_AURANEST_PREFIX,
                                         settings.auranest_mappings)
        last_timestamp = elastic.get_last_timestamp(es_index)
        log.debug("Last timestamp: %d" % last_timestamp)
        last_identifiers = elastic.get_ids_with_timestamp(last_timestamp,
                                                          es_index)
    except Exception as e:
        log.error("Failed to setup elastic: %s" % str(e))
        sys.exit(1)

    doc_counter = 0

    while True:
        (last_identifiers, last_timestamp, annonser) = \
            postgresql.read_from_pg_since(last_identifiers, last_timestamp,
                                          settings.PG_AURANEST_TABLE)
        print("RAW AD")
        print(json.dumps(annonser[0], indent=2))
        # sys.exit(0)
        current_doc_count = len(annonser)
        doc_counter += current_doc_count
        log.debug("Read %d ads" % doc_counter)

        if annonser:
            try:
                trim_auranest_ids(annonser)
                enriched_annonser = enr.enrich(annonser,
                                               parallelism=settings.ENRICHER_PROCESSES)
                print("ENRICHED AD")
                print(json.dumps(enriched_annonser[0], indent=2))
                sys.exit(0)

                # enriched_annonser = enricher.enrich(annonser)
                elastic.bulk_index(enriched_annonser, es_index)
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


def trim_auranest_ids(annonser):
    # Some id:s contains a lot of trailing whitespace,
    # needs to be removed before storing in Elastic.
    for annons in annonser:
        doc_id = annons.get('id', '')
        trimmed_doc_id = doc_id.strip()
        annons['id'] = trimmed_doc_id

        group_prop = annons.get('group', {})
        group_id = group_prop.get('id', '')
        trimmed_group_id = group_id.strip()
        if group_prop:
            group_prop['id'] = trimmed_group_id


if __name__ == '__main__':
    start()
