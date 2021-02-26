import sys
import logging
from elasticsearch.exceptions import NotFoundError

import importers.mappings
from importers.repository import elastic
from importers import settings

log = logging.getLogger(__name__)


def set_platsannons_read_alias(idxname=None):
    if not idxname and len(sys.argv) > 1:
        idxname = sys.argv[1]
    if not idxname:
        log.error("Must provide name of read index to alias against. Exit!")
        sys.exit(1)

    aliasname = "%s%s" % (settings.ES_ANNONS_PREFIX, settings.READ_INDEX_SUFFIX)
    change_alias([idxname], aliasname)
    deleted_index = "%s%s" % (settings.ES_ANNONS_PREFIX, settings.DELETED_INDEX_SUFFIX)
    stream_alias = "%s%s" % (settings.ES_ANNONS_PREFIX, settings.STREAM_INDEX_SUFFIX)
    change_alias([idxname, deleted_index], stream_alias)


def set_platsannons_write_alias(idxname=None):
    if len(sys.argv) > 1:
        idxname = sys.argv[1]
    if not idxname:
        log.error("Must provide name of write index to alias against. Exit!")
        sys.exit(1)

    change_alias([idxname], settings.ES_ANNONS_INDEX)


def create_platsannons_index():
    if len(sys.argv) < 1:
        log.error("Must provide name of index to alias against. Exit!")
        sys.exit(1)

    idxname = sys.argv[1]
    elastic.create_index(idxname, importers.mappings.platsannons_mappings)


def change_alias(idxnames, aliasname):
    log.info(f"Setting alias: {aliasname} to point to: {idxnames}")
    try:
        if elastic.alias_exists(aliasname):
            oldindices = list(elastic.get_alias(aliasname).keys())
            elastic.update_alias(idxnames, oldindices, aliasname)
        else:
            elastic.add_indices_to_alias(idxnames, aliasname)
    except NotFoundError as e:
        log.error(f"Can't create alias: {aliasname}. Indices not found: {idxnames}. {e} Exit!")
        sys.exit(1)


def check_index_size_before_switching_alias(new_index_name):
    alias_name = "%s%s" % (settings.ES_ANNONS_PREFIX, settings.READ_INDEX_SUFFIX)
    if not elastic.alias_exists(alias_name):
        log.info(f"Alias {alias_name} does not exist, can't compare to old data, continuing")
        return True
    current_index = elastic.get_index_name_for_alias(alias_name)
    current_number = elastic.document_count(current_index)
    new_number = elastic.document_count(new_index_name)
    if int(new_number) < int(current_number) * settings.NEW_ADS_COEF:
        log.error(f"Too FEW ads in import. New: {new_number} current: {current_number}, coefficient: {settings.NEW_ADS_COEF}")
        return False
    else:
        log.info(f'OK number of ads in import. New: {new_number} current: {current_number}')
        return True


if __name__ == '__main__':
    print("Use set_platsannons_read_alias or set_platsannons_write_alias for wanted dataset.")
