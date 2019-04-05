import logging
from multiprocessing import Value
import requests
import math
# from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
# from requests_futures.sessions import FuturesSession
from importers import settings
from importers.common import grouper

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)
log = logging.getLogger(__name__)

timeout_enrich_api = 90


def init_process(args):
    global counter
    counter = args


def enrich(annonser, parallelism=1):
    log.info('Running enrich with %s processes' % str(parallelism))
    log.info('Enriching documents calling: %s'
             % settings.URL_ENRICH_TEXTDOCS_BINARY_SERVICE)

    global counter
    counter = Value('i', 0)

    # Prepare input
    annonser_input_data = []
    for annons in annonser:
        doc_id = str(annons.get('id', ''))
        doc_headline = get_doc_headline_input(annons)
        doc_text = annons.get('beskrivning', {}).get('annonstext', '')
        if doc_id == '':
            raise ValueError('Document has no id, enrichment is not possible, headline: '
                             % (doc_headline))

        input_doc_params = {
            settings.ENRICHER_PARAM_DOC_ID: doc_id,
            settings.ENRICHER_PARAM_DOC_HEADLINE: doc_headline,
            settings.ENRICHER_PARAM_DOC_TEXT: doc_text
        }

        annonser_input_data.append(input_doc_params)

    nr_of_items_per_batch = len(annonser) // parallelism
    nr_of_items_per_batch = int(math.ceil(nr_of_items_per_batch / 1000.0)) * 1000
    nr_of_items_per_batch = min(nr_of_items_per_batch, len(annonser), 100)
    if nr_of_items_per_batch == 0:
        nr_of_items_per_batch = len(annonser)
    log.info('nr_of_items_per_batch: %s' % nr_of_items_per_batch)

    annons_batches = grouper(nr_of_items_per_batch, annonser_input_data)

    batch_indatas = []
    for i, annons_batch in enumerate(annons_batches):
        annons_batch_indatas = [annons_indata for annons_indata in annons_batch]
        batch_indata = {
                "documents_input": annons_batch_indatas
        }
        batch_indatas.append(batch_indata)

    enrich_results_data = execute_calls(batch_indatas, parallelism)
    log.info('Enriched %s/%s documents' % (len(enrich_results_data), len(annonser)))

    for annons in annonser:
        doc_id = str(annons.get('id', ''))
        enriched_output = enrich_results_data[doc_id]

        enrich_doc(annons, enriched_output)

    return annonser


def get_doc_headline_input(annons):
    # Add occupation from structured data in headline.
    doc_headline1 = annons.get('yrkesroll', {}).get('term', '')
    if(doc_headline1 is None):
        doc_headline1 = ''

    doc_headline2 = annons.get('rubrik', '')
    if doc_headline1 != '' and doc_headline1 != doc_headline2:
        doc_headline = doc_headline1 + ' ' + doc_headline2
    else:
        doc_headline = doc_headline2

    return doc_headline


def get_enrich_result(batch_indata, timeout):
    headers = {'Content-Type': 'application/json'}
    r = requests.post(url=settings.URL_ENRICH_TEXTDOCS_BINARY_SERVICE,
                      headers=headers, json=batch_indata, timeout=timeout)
    r.raise_for_status()
    return r.json()


def execute_calls(batch_indatas, parallelism):
    global timeout_enrich_api
    global counter
    enriched_output = {}
    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        # Start the load operations and mark each future with its URL
        future_to_enrich_result = {executor.submit(get_enrich_result, batch_indata,
                                                   timeout_enrich_api): batch_indata
                                   for batch_indata in batch_indatas}
        for future in concurrent.futures.as_completed(future_to_enrich_result):
            try:
                enriched_result = future.result()
                for resultrow in enriched_result:
                    enriched_output[resultrow[settings.ENRICHER_PARAM_DOC_ID]] = resultrow
                    # += operation is not atomic, so we need to get a lock:
                    with counter.get_lock():
                        counter.value += 1
                        if counter.value % 100 == 0:
                            log.info("enrichtextdocumentsbinary - Processed %s docs"
                                     % (str(counter.value)))
            except Exception as exc:
                log.error('Enrichment call generated an exception: %s' % (str(exc)))
                raise exc

    return enriched_output


def enrich_doc(annons, enriched_output):
    annons['keywords'] = {}
    enriched_candidates = enriched_output['enriched_candidates']
    new_occupations = [candidate['concept_label'].lower()
                       for candidate in enriched_candidates['occupations']]
    annons['keywords']['occupation'] = new_occupations
    new_skills = [candidate['concept_label'].lower()
                  for candidate in enriched_candidates['competencies']]
    annons['keywords']['skill'] = new_skills
    new_traits = [candidate['concept_label'].lower()
                  for candidate in enriched_candidates['traits']]
    annons['keywords']['trait'] = new_traits

    return annons
