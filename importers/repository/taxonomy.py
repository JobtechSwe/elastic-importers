from importers.repository import elastic
from valuestore.taxonomy import get_term as gt, get_entity as ge, find_concept_by_legacy_ams_taxonomy_id as \
    fcbla, find_legacy_ams_taxonomy_id_by_concept_id as flatc ,find_info_by_city_name as fibcn
from elasticsearch import RequestError
import logging

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)

log = logging.getLogger(__name__)

tax_value_cache = {}


def get_term(taxtype, taxid):
    return gt(elastic.es, taxtype, taxid)


def get_entity(taxtype, taxid, not_found_response=None):
    key = "entity-%s-%s-%s" % (taxtype, taxid, str(not_found_response))
    if key not in tax_value_cache:
        try:
            value = ge(elastic.es, taxtype, taxid, not_found_response)
        except RequestError:
            log.warning('Taxonomy RequestError for request with arguments type: {taxtype} and id: {taxid}')
            value = not_found_response
            log.info("(get_entity) set value: %s" % str(not_found_response))
        if value:
            tax_value_cache[key] = value
        else:
            tax_value_cache[key] = {}
            log.warning("(get_entity) set empty{} to tax_value_cache[key]: %s[%s]" % (tax_value_cache, key))
    cached = dict(tax_value_cache.get(key, {}))
    log.debug("(get_entity) returns cached: %s" % cached)
    return cached


def get_concept_by_legacy_id(taxtype, legacy_id, not_found_response=None):
    key = "concept-%s-%s-%s" % (str(taxtype), legacy_id, str(not_found_response))
    if key not in tax_value_cache:
        try:
            value = fcbla(elastic.es, taxtype, legacy_id, not_found_response)
        except RequestError:
            log.warning('Taxonomy RequestError for request with arguments type: {taxtype} and id: {legacy_id}')
            value = not_found_response
            log.info("(get_concept_by_legacy_id) set value: %s" % str(not_found_response))
        if value:
            tax_value_cache[key] = value
        else:
            tax_value_cache[key] = {}
            log.warning("(get_concept_by_legacy_id) set empty{} to tax_value_cache[key]: %s[%s]" % (tax_value_cache, key))
    cached = dict(tax_value_cache.get(key, {}))
    log.debug("(get_concept_by_legacy_id) returns cached: %s" % cached)
    return cached


def get_legacy_by_concept_id(taxtype, concept_id, not_found_response=None):
    key = "concept-%s-%s-%s" % (str(taxtype), concept_id, str(not_found_response))
    if key not in tax_value_cache:
        try:
            value = flatc(elastic.es, taxtype, concept_id, not_found_response)
        except RequestError:
            log.warning('Taxonomy RequestError for request with arguments type: {taxtype} and id: {legacy_id}')
            value = not_found_response
            log.info("(get_legacy_by_concept_id) set value: %s" % str(not_found_response))
        if value:
            tax_value_cache[key] = value
        else:
            tax_value_cache[key] = {}
            log.warning("(get_legacy_by_concept_id) set empty{} to tax_value_cache[key]: %s[%s]" % (tax_value_cache, key))
    cached = dict(tax_value_cache.get(key, {}))
    log.debug(concept_id, taxtype)
    log.debug("(get_legacy_by_concept_id) returns cached: %s" % cached)
    return cached


# use city need to get all taxonomy info
def get_info_by_city_name(city, not_found_response=None):
    value = None
    if city:
        try:
            value = fibcn(elastic.es, city, not_found_response)
        except RequestError:
            log.warning('Taxonomy RequestError for request with city name: {city}')
            value = not_found_response
            log.info("(find_info_by_city_name) set value: %s" % str(not_found_response))
    return value
