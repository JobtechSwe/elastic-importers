from importers.repository import elastic
from valuestore.taxonomy import get_term as gt, get_entity as ge, find_legacy_ams_taxonomy_id_by_concept_id as flatc
from elasticsearch import RequestError
import logging

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)
log = logging.getLogger(__name__)

tax_value_cache = {}


def get_term(tax_type, tax_id):
    return gt(elastic.es, tax_type, tax_id)


def get_entity(tax_type, tax_id, not_found_response=None):
    key = f"entity-{tax_type}-{tax_id}-{not_found_response}"
    if key not in tax_value_cache:
        try:
            value = ge(elastic.es, tax_type, tax_id, not_found_response)
        except RequestError:
            log.warning(f'Taxonomy RequestError for request with arguments type: {tax_type} and id: {tax_id}')
            value = not_found_response
            log.info(f"(get_entity) set value: {value}")
        if value:
            tax_value_cache[key] = value
        else:
            tax_value_cache[key] = {}
            log.warning(f"(get_entity) set empty to tax_value_cache[key]: [{key}]")
    cached = dict(tax_value_cache.get(key, {}))
    log.debug(f"(get_entity) returns cached: {cached}")
    return cached


def get_legacy_by_concept_id(tax_type, concept_id, not_found_response=None):
    key = f"concept-{tax_type}-{concept_id}-{not_found_response}"
    if key not in tax_value_cache:
        try:
            value = flatc(elastic.es, tax_type, concept_id, not_found_response)
        except RequestError as e:
            log.warning(f'Taxonomy RequestError for request with arguments type: {tax_type} and id: {concept_id}. {e}')
            value = not_found_response
            log.info(f"(get_legacy_by_concept_id) set value: {not_found_response}")
        if value:
            tax_value_cache[key] = value
        else:
            tax_value_cache[key] = {}
            log.warning(f"(get_legacy_by_concept_id) set empty value to tax_value_cache[key]: [{key}]")
    cached = dict(tax_value_cache.get(key, {}))
    log.debug(concept_id, tax_type)
    log.debug(f"(get_legacy_by_concept_id) returns cached: {cached}")
    return cached

