from importers.repository import elastic
from valuestore.taxonomy import get_term as gt, get_entity as ge, find_concept_by_legacy_ams_taxonomy_id as fcbla, taxtype_legend as ttl


def get_term(taxtype, taxid):
    return gt(elastic.es, taxtype, taxid)


def get_entity(taxtype, taxid, not_found_response=None):
    return ge(elastic.es, taxtype, taxid, not_found_response)

def get_concept_by_legacy_id(taxtype, legacy_id, not_found_response=None):
    translated_taxtype = ttl[taxtype]
    return fcbla(elastic.es, translated_taxtype, legacy_id, not_found_response)