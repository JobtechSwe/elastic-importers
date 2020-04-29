import sys
import logging
from elasticsearch.exceptions import NotFoundError
from importers.repository import elastic
from importers import settings

WRITE_SUFFIX = '-write'
READ_SUFFIX = '-read'

log = logging.getLogger(__name__)


def set_platsannons_read_alias(idxname=None):
    if not idxname and len(sys.argv) > 1:
        idxname = sys.argv[1]
    if not idxname:
        log.error("Must provide name of index to alias against. Exit!")
        sys.exit(1)

    aliasname = "%s%s" % (settings.ES_ANNONS_PREFIX, READ_SUFFIX)
    change_alias([idxname], aliasname)
    deleted_index = "%s%s" % (settings.ES_ANNONS_PREFIX,
                              settings.DELETED_INDEX_SUFFIX)
    stream_alias = "%s%s" % (settings.ES_ANNONS_PREFIX,
                             settings.STREAM_INDEX_SUFFIX)
    change_alias([idxname, deleted_index], stream_alias)


def set_platsannons_write_alias(idxname=None):
    if len(sys.argv) > 1:
        idxname = sys.argv[1]
    if not idxname:
        log.error("Must provide name of index to alias against. Exit!")
        sys.exit(1)

    change_alias([idxname], settings.ES_ANNONS_INDEX)


def create_platsannons_index():
    if len(sys.argv) < 1:
        log.error("Must provide name of index to alias against. Exit!")
        sys.exit(1)

    idxname = sys.argv[1]
    elastic.create_index(idxname, settings.platsannons_mappings)


def change_alias(idxnames, aliasname):
    log.info(f"Setting alias: {aliasname} to point to: {idxnames}")
    try:
        if elastic.alias_exists(aliasname):
            oldindices = list(elastic.get_alias(aliasname).keys())
            elastic.update_alias(idxnames, oldindices, aliasname)
            log.info("Update_alias. Index names: %s. Old indices: %s. Alias name: %s" % (idxnames, oldindices, aliasname))
        else:
            elastic.add_indices_to_alias(idxnames, aliasname)
            log.info("Add_indices. Index names: %s. Alias name: %s" % (idxnames, aliasname))
    except NotFoundError:
        log.error("Error: Can't create alias: %s. Index: %s not found. Exit!"
                  % (aliasname, idxnames))
        sys.exit(1)


if __name__ == '__main__':
    print("Use methods 'set_read_alias' or 'set_write_alias' for wanted dataset.")
