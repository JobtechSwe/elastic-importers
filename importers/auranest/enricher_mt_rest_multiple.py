import logging
from multiprocessing import Value
import requests
import math
import concurrent.futures
from importers import settings
from importers.common import grouper

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)
log = logging.getLogger(__name__)

timeout_enrich_api = 90

def enrich(annonser, parallelism=1):
    # enricher_mt.py - 2019-01-09 11:19:52+0100|INFO|importers.platsannons.main|MESSAGE: Indexed 999 docs in: 23.753832817077637 seconds.
    log.info('Running enrich with %s processes' % str(parallelism))
    log.info('Enriching documents calling: %s' % settings.URL_ENRICH_TEXTDOCS_BINARY_SERVICE)
    global counter
    counter = Value('i', 0)

    # Prepare input
    annonser_input_data = []
    for annons in annonser:
        doc_id = annons.get('id', '')
        # Some id:s contains a lot of trailing whitespace.
        doc_id = doc_id.strip()
        annons['id'] = doc_id
        # group_id = annons.get('group', {}).get('id')
        # group_id = group_id.strip()
        # annons['id'] = group_id

        doc_headline = get_doc_headline_input(annons)

        if doc_id == '':
            raise ValueError('Document has no id, enrichment is not possible, headline: %s' % (doc_headline))

        doc_text = annons.get('content', {}).get('text', '')
        if clean_text(doc_text) == '':
            log.warning('doc_text is empty for id: %s' % doc_id)
            doc_text = ''

        if doc_headline == '' and doc_text == '':
            log.warning('Can\'t enrich document with id: %s since it has no headline or text.' % (doc_id))
            annons['occupations'] = []
            annons['skills'] = []
            annons['traits'] = []
        else:
            input_doc_params = {
                settings.ENRICHER_PARAM_DOC_ID: doc_id,
                settings.ENRICHER_PARAM_DOC_HEADLINE: doc_headline,
                settings.ENRICHER_PARAM_DOC_TEXT: doc_text
            }
            # print(input_doc_params)
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
        if doc_id in enrich_results_data:
            enriched_output = enrich_results_data[doc_id]
            enrich_doc(annons, enriched_output)
    return annonser


def get_doc_headline_input(annons):
    # If 'freetext' differs from 'header', 'freetext' is the occupationtitle (taxonomy) from an AF-ad.
    doc_headline1 = annons.get('title', {}).get('freetext', '')
    if(doc_headline1 is None):
        doc_headline1 = ''
    doc_headline2 = annons.get('header', '')
    if(doc_headline2 is None):
        doc_headline2 = ''

    if doc_headline1 != '' and doc_headline1 != doc_headline2:
        doc_headline = doc_headline1 + ' ' + doc_headline2
    else:
        doc_headline = doc_headline2

    if clean_text(doc_headline) == '':
        doc_headline = ''
    return doc_headline

def clean_text(text):
    if text is None:
        text = ''
    cleaned_text = text.strip()
    return cleaned_text


def get_enrich_result(batch_indata, timeout):
    headers = {'Content-Type': 'application/json'}
    # log.debug('len(batch_indata[documents_input])', len(batch_indata['documents_input']))
    # log.info('get_enrich_result - Sending %s ads (documents_input) for enrichment' % (len(batch_indata['documents_input'])))
    # log.debug('Enriching Id: %s' % (input_doc_params[NarvalEnricher.PARAM_DOC_ID]))
    r = requests.post(url=settings.URL_ENRICH_TEXTDOCS_BINARY_SERVICE, headers=headers, json = batch_indata, timeout=timeout)
    r.raise_for_status()
    return r.json()



def execute_calls(batch_indatas, parallelism):
    global timeout_enrich_api
    global counter
    enriched_output = {}
    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        # Start the load operations and mark each future with its URL
        future_to_enrich_result = {executor.submit(get_enrich_result, batch_indata, timeout_enrich_api): batch_indata for batch_indata in batch_indatas}
        for future in concurrent.futures.as_completed(future_to_enrich_result):
            try:
                enriched_result = future.result()
                for resultrow in enriched_result:
                    enriched_output[resultrow[settings.ENRICHER_PARAM_DOC_ID]] = resultrow
                    # += operation is not atomic, so we need to get a lock:
                    with counter.get_lock():
                        counter.value += 1
                        if counter.value % 100 == 0:
                            log.info("enrich_docs - Processed %s docs" % (str(counter.value)))
            except Exception as exc:
                log.error('Call generated an exception: %s' % (str(exc)))
                raise exc


    return enriched_output

def enrich_doc(annons, enriched_output):
    enriched_candidates = enriched_output['enriched_candidates']
    new_occupations = [candidate['concept_label'].lower() for candidate in enriched_candidates['occupations']]
    annons['occupations'] = new_occupations
    new_skills = [candidate['concept_label'].lower() for candidate in enriched_candidates['competencies']]
    annons['skills'] = new_skills
    new_traits = [candidate['concept_label'].lower() for candidate in enriched_candidates['traits']]
    annons['traits'] = new_traits

    return annons

