from importers.repository import elastic
from valuestore.taxonomy import get_term as gt, get_entity as ge, find_concept_by_legacy_ams_taxonomy_id as fcbla

tax_value_cache = {}


def get_term(taxtype, taxid):
    return gt(elastic.es, taxtype, taxid)


def get_entity(taxtype, taxid, not_found_response=None):
    key = "entity-%s-%s-%s" % (taxtype, taxid, str(not_found_response))
    if key not in tax_value_cache:
        value = ge(elastic.es, taxtype, taxid, not_found_response)
        if value:
            tax_value_cache[key] = value
        else:
            tax_value_cache[key] = {}
    return dict(tax_value_cache[key])


def get_concept_by_legacy_id(taxtype, legacy_id, not_found_response=None):
    key = "concept-%s-%s-%s" % (str(taxtype), legacy_id, str(not_found_response))
    if key not in tax_value_cache:
        value = fcbla(elastic.es, taxtype, legacy_id, not_found_response)
        if value:
            tax_value_cache[key] = value
        else:
            tax_value_cache[key] = {}
    cached = dict(tax_value_cache.get(key, {}))
    return cached
