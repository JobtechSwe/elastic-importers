import sys
import logging
from elasticsearch.exceptions import NotFoundError

from importers.repository import elastic
from importers import settings

log = logging.getLogger(__name__)


def set_platsannons_read_alias(idx_name=None):
    if not idx_name and len(sys.argv) > 1:
        idx_name = sys.argv[1]
    if not idx_name:
        log.error("Must provide name of read index to alias against. Exit!")
        sys.exit(1)

    aliasname = "%s%s" % (settings.ES_ANNONS_PREFIX, settings.READ_INDEX_SUFFIX)
    change_alias([idx_name], aliasname)
    deleted_index = "%s%s" % (settings.ES_ANNONS_PREFIX, settings.DELETED_INDEX_SUFFIX)
    stream_alias = "%s%s" % (settings.ES_ANNONS_PREFIX, settings.STREAM_INDEX_SUFFIX)
    change_alias([idx_name, deleted_index], stream_alias)


def set_platsannons_write_alias(idx_name=None):
    if len(sys.argv) > 1:
        idx_name = sys.argv[1]
    if not idx_name:
        log.error("Must provide name of write index to alias against. Exit!")
        sys.exit(1)

    change_alias([idx_name], settings.ES_ANNONS_INDEX)


def change_alias(idx_names, alias_name):
    log.info(f"Setting alias: {alias_name} to point to: {idx_names}")
    try:
        if elastic.alias_exists(alias_name):
            oldindices = list(elastic.get_alias(alias_name).keys())
            elastic.update_alias(idx_names, oldindices, alias_name)
        else:
            elastic.add_indices_to_alias(idx_names, alias_name)
    except NotFoundError as e:
        log.error(f"Can't create alias: {alias_name}. Indices not found: {idx_names}. {e} Exit!")
        sys.exit(1)


def check_index_size_before_switching_alias(new_index_name):
    alias_name = "%s%s" % (settings.ES_ANNONS_PREFIX, settings.READ_INDEX_SUFFIX)
    if not elastic.alias_exists(alias_name):
        log.info(f"Alias {alias_name} does not exist, can't compare to old data, continuing")
        return True
    current_index = elastic.get_index_name_for_alias(alias_name)
    current_number = elastic.number_of_not_removed_ads(current_index)
    new_number = elastic.document_count(new_index_name)
    if int(new_number) < int(current_number) * settings.NEW_ADS_COEF:
        log.error(f"Too FEW ads in import. New: {new_number} current: {current_number}, coefficient: {settings.NEW_ADS_COEF}")
        return False
    else:
        log.info(f'OK number of ads in import. New: {new_number} current: {current_number}, coefficient: {settings.NEW_ADS_COEF}')
        return True


if __name__ == '__main__':
    print("Use set_platsannons_read_alias or set_platsannons_write_alias for wanted dataset.")
