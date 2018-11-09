from importers.repository import elastic
from valuestore.taxonomy import get_term as gt, get_entity as ge


def get_term(taxtype, taxid):
    return gt(elastic.es, taxtype, taxid)


def get_entity(taxtype, taxid, not_found_response=None):
    return ge(elastic.es, taxtype, taxid, not_found_response)
