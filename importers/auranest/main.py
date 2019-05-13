import sys
import time
import logging
import itertools
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
        current_doc_count = len(annonser)
        doc_counter += current_doc_count
        log.debug("Read %d ads" % doc_counter)

        if annonser:
            try:
                trim_auranest_ids(annonser)
                enriched_annonser = enr.enrich(annonser,
                                               parallelism=settings.ENRICHER_PROCESSES)
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


def glitchfix():
    log.info('Starting glitchfix for %s with PG_BATCH_SIZE: %s'
             % (IMPORTER_NAME, settings.PG_BATCH_SIZE))
    start_time = time.time()

    try:
        es_index = elastic.setup_indices(sys.argv, settings.ES_AURANEST_PREFIX,
                                         settings.auranest_mappings)
    except Exception as e:
        log.error("Failed to setup elastic: %s" % str(e))
        sys.exit(1)

    doc_counter = 0

    glitch_ids = elastic.get_glitch_jobtechjobs_ids()
    log.info("Found %s glitchfix ads in Elastic" % len(glitch_ids))

    if len(glitch_ids) > 0:
        log.info("Processing %s ads with a batchsize of %s" % (len(glitch_ids), settings.PG_BATCH_SIZE))

        id_batches = grouper(int(settings.PG_BATCH_SIZE), glitch_ids)

        for i, id_batch in enumerate(id_batches):
            id_batch_ids = [id for id in id_batch]
            postgres_glitch_ads = postgresql.read_docs_with_ids(settings.PG_AURANEST_TABLE, id_batch_ids)

            glitch_ads = [ad for ad in postgres_glitch_ads if ad['source'] and ad['source']['removedAt']]
            current_doc_count = len(glitch_ads)
            doc_counter += current_doc_count
            log.info('Found %s/%s glitchfix ads in postgres' % (current_doc_count, len(postgres_glitch_ads)))
            if current_doc_count > 0:
                try:
                    trim_auranest_ids(glitch_ads)
                    enriched_annonser = enr.enrich(glitch_ads,
                                                   parallelism=settings.ENRICHER_PROCESSES)
                    elastic.bulk_index(enriched_annonser, es_index)
                    log.info("Indexed %d glitchfix docs so far." % doc_counter)
                except Exception as e:
                    log.error("Glitchfix import failed", e)
                    sys.exit(1)

    elapsed_time = time.time() - start_time
    log.info("Glitchfix - indexed %d docs in: %s seconds." % (doc_counter, elapsed_time))


def grouper(n, iterable):
    iterable = iter(iterable)
    return iter(lambda: list(itertools.islice(iterable, n)), [])


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
