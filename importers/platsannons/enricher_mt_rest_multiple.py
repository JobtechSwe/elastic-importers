import logging
from multiprocessing import Value
import requests
import math
import time
import sys
import concurrent.futures
from importers import settings
from importers.common import grouper
from importers.platsannons.converter import get_null_safe_value

log = logging.getLogger(__name__)

TIMEOUT_ENRICH_API = 90
counter = None
RETRIES = 10


def enrich(annonser, scraped=False):
    len_annonser = len(annonser)
    parallelism = settings.ENRICHER_PROCESSES if len_annonser > 99 else 1
    log.info(f'Enriching docs: {len_annonser} processes: {parallelism} calling: {settings.URL_ENRICH_TEXTDOCS_SERVICE} ')

    global counter
    counter = Value('i', 0)

    # Prepare input
    annonser_input_data = []
    for annons in annonser:
        doc_id = str(annons.get('id', ''))
        doc_headline = get_doc_headline_input(annons)
        if scraped:
            doc_text = annons.get('originalJobPosting', {}).get('description', {}).get('text_formatted', '')
        else:
            doc_text = annons.get('description', {}).get('text_formatted', '')

        if not doc_text:
            log.debug("No enrich - empty description for id: %s, moving on to the next one." % doc_id)
            continue
        if doc_id == '':
            log.error("Value error - no id, headline: %s" % str(doc_headline))
            raise ValueError('Document has no id, enrichment is not possible, headline: ' % doc_headline)

        input_doc_params = {
            settings.ENRICHER_PARAM_DOC_ID: doc_id,
            settings.ENRICHER_PARAM_DOC_HEADLINE: doc_headline,
            settings.ENRICHER_PARAM_DOC_TEXT: doc_text
        }

        annonser_input_data.append(input_doc_params)

    nr_of_items_per_batch = len_annonser // parallelism
    nr_of_items_per_batch = int(math.ceil(nr_of_items_per_batch / 1000.0)) * 1000
    nr_of_items_per_batch = min(nr_of_items_per_batch, len_annonser, 100)
    if nr_of_items_per_batch == 0:
        nr_of_items_per_batch = len_annonser
    log.info('Items per batch: %s' % nr_of_items_per_batch)

    annons_batches = grouper(nr_of_items_per_batch, annonser_input_data)

    batch_indatas = []
    for i, annons_batch in enumerate(annons_batches):
        annons_batch_indatas = [annons_indata for annons_indata in annons_batch]
        batch_indata = {
            "include_terms_info": True,
            "include_sentences": True,
            "documents_input": annons_batch_indatas
        }
        batch_indatas.append(batch_indata)

    enrich_results_data = execute_calls(batch_indatas, parallelism)
    log.info('Enriched %s/%s documents' % (len(enrich_results_data), len_annonser))

    for annons in annonser:
        doc_id = str(annons.get('id', ''))
        if doc_id in enrich_results_data:
            enriched_output = enrich_results_data[doc_id]
            enrich_doc(annons, enriched_output)

    return annonser


def get_doc_headline_input(annons):
    sep = ' | '
    # Add occupation from structured data in headline.
    doc_headline_occupation = annons.get('occupation', {}).get('label', '')
    if doc_headline_occupation is None:
        doc_headline_occupation = ''

    wp_address_node = annons.get('workplace_address', {})
    wp_address_input = ''
    if wp_address_node:
        wp_city = get_null_safe_value(wp_address_node, 'city', '')
        wp_municipality = get_null_safe_value(wp_address_node, 'municipality', '')
        wp_region = get_null_safe_value(wp_address_node, 'region', '')
        wp_country = get_null_safe_value(wp_address_node, 'country', '')
        wp_address_input = wp_city + sep + wp_municipality + sep + wp_region + sep + wp_country

    doc_headline = get_null_safe_value(annons, 'headline', '')

    doc_headline_input = wp_address_input + sep + doc_headline_occupation + sep + doc_headline

    return doc_headline_input


def get_enrich_result(batch_indata, timeout):
    headers = {'Content-Type': 'application/json', 'api-key': settings.API_KEY_ENRICH_TEXTDOCS}
    for retry in range(RETRIES):
        try:
            r = requests.post(url=settings.URL_ENRICH_TEXTDOCS_SERVICE, headers=headers, json=batch_indata, timeout=timeout)
            r.raise_for_status()
        except requests.HTTPError as e:
            log.warning(f"get_enrich_result() retrying after error: {e}")
            time.sleep(0.3)
        else:  # no error
            return r.json()
    log.error(f"get_enrich_result failed after: {RETRIES} retries with error. Exit!")
    sys.exit(1)


def execute_calls(batch_indatas, parallelism):
    global counter
    enriched_output = {}
    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as executor:
        # Start the load operations and mark each future with its URL
        future_to_enrich_result = {executor.submit(get_enrich_result, batch_indata,
                                                   TIMEOUT_ENRICH_API): batch_indata
                                   for batch_indata in batch_indatas}
        for future in concurrent.futures.as_completed(future_to_enrich_result):
            try:
                enriched_result = future.result()
                for resultrow in enriched_result:
                    enriched_output[resultrow[settings.ENRICHER_PARAM_DOC_ID]] = resultrow
                    # += operation is not atomic, so we need to get a lock:
                    with counter.get_lock():
                        counter.value += 1
                        if counter.value % 1000 == 0:
                            log.info("enrichtextdocuments - Processed %s docs"
                                     % (str(counter.value)))
            except Exception as exc:
                log.error('Enrichment call generated an exception: %s' % (str(exc)))
                raise exc

    return enriched_output


def enrich_doc(annons, enriched_output):
    if 'keywords' not in annons:
        annons['keywords'] = {}

    process_enriched_candidates(annons, enriched_output)
    process_enriched_candidates_typeahead_terms(annons, enriched_output)

    return annons


SOURCE_TYPE_OCCUPATION = "occupations"
SOURCE_TYPE_SKILL = "competencies"
SOURCE_TYPE_TRAIT = "traits"
SOURCE_TYPE_LOCATION = "geos"

TARGET_TYPE_OCCUPATION = "occupation"
TARGET_TYPE_SKILL = "skill"
TARGET_TYPE_TRAIT = "trait"
TARGET_TYPE_LOCATION = "location"
TARGET_TYPE_COMPOUND = "compound"


def process_enriched_candidates(annons, enriched_output):
    enriched_candidates = enriched_output['enriched_candidates']
    fieldname = 'enriched'

    candidate_prop_name = 'concept_label'
    if fieldname not in annons['keywords']:
        annons['keywords'][fieldname] = {}

    enriched_node = annons['keywords'][fieldname]

    enriched_node[TARGET_TYPE_OCCUPATION] = list(set([candidate[candidate_prop_name].lower()
                                                      for candidate in filter_candidates(enriched_candidates,
                                                                                         SOURCE_TYPE_OCCUPATION,
                                                                                         settings.ENRICH_THRESHOLD_OCCUPATION)]))

    enriched_node[TARGET_TYPE_SKILL] = list(set([candidate[candidate_prop_name].lower()
                                                 for candidate in
                                                 filter_candidates(enriched_candidates, SOURCE_TYPE_SKILL,
                                                                   settings.ENRICH_THRESHOLD_SKILL)]))

    enriched_node[TARGET_TYPE_TRAIT] = list(set([candidate[candidate_prop_name].lower()
                                                 for candidate in
                                                 filter_candidates(enriched_candidates, SOURCE_TYPE_TRAIT,
                                                                   settings.ENRICH_THRESHOLD_TRAIT)]))

    enriched_node[TARGET_TYPE_LOCATION] = list(set([candidate[candidate_prop_name].lower()
                                                    for candidate in
                                                    filter_candidates(enriched_candidates, SOURCE_TYPE_LOCATION,
                                                                      settings.ENRICH_THRESHOLD_GEO)]))

    enriched_node[TARGET_TYPE_COMPOUND] = enriched_node[TARGET_TYPE_OCCUPATION] \
                                          + enriched_node[TARGET_TYPE_SKILL] \
                                          + enriched_node[TARGET_TYPE_LOCATION]


def process_enriched_candidates_typeahead_terms(annons, enriched_output):
    enriched_candidates = enriched_output['enriched_candidates']
    fieldname = 'enriched_typeahead_terms'

    if fieldname not in annons['keywords']:
        annons['keywords'][fieldname] = {}

    enriched_typeahead_node = annons['keywords'][fieldname]

    enriched_typeahead_node[TARGET_TYPE_OCCUPATION] = filter_valid_typeahead_terms(
        filter_candidates(enriched_candidates, SOURCE_TYPE_OCCUPATION, settings.ENRICH_THRESHOLD_OCCUPATION))

    enriched_typeahead_node[TARGET_TYPE_SKILL] = filter_valid_typeahead_terms(
        filter_candidates(enriched_candidates, SOURCE_TYPE_SKILL, settings.ENRICH_THRESHOLD_SKILL))

    enriched_typeahead_node[TARGET_TYPE_TRAIT] = filter_valid_typeahead_terms(
        filter_candidates(enriched_candidates, SOURCE_TYPE_TRAIT, settings.ENRICH_THRESHOLD_TRAIT))

    enriched_typeahead_node[TARGET_TYPE_LOCATION] = filter_valid_typeahead_terms(
        filter_candidates(enriched_candidates, SOURCE_TYPE_LOCATION, settings.ENRICH_THRESHOLD_GEO))

    enriched_typeahead_node[TARGET_TYPE_COMPOUND] = enriched_typeahead_node[TARGET_TYPE_OCCUPATION] \
                                                    + enriched_typeahead_node[TARGET_TYPE_SKILL] \
                                                    + enriched_typeahead_node[TARGET_TYPE_LOCATION]


def filter_valid_typeahead_terms(candidates):
    typeahead_for_type = set()
    for cand in candidates:
        typeahead_for_type.add(cand['concept_label'].lower())
        if not cand['term_misspelled']:
            typeahead_for_type.add(cand['term'].lower())
    return list(typeahead_for_type)


def filter_candidates(enriched_candidates, type_name, prediction_threshold):
    candidates = [candidate
                  for candidate in enriched_candidates[type_name]
                  if candidate['prediction'] >= prediction_threshold]
    if type_name == SOURCE_TYPE_OCCUPATION or type_name == SOURCE_TYPE_LOCATION:
        # Check if candidates are in ad headline and add  them as enriched
        # even though the prediction value is below threshold.
        candidates = candidates + [candidate
                                   for candidate in enriched_candidates[type_name]
                                   if candidate['sentence_index'] == 0 and candidate not in candidates]

    return candidates
