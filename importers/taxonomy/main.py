import logging
from jobtech.common.customlogging import configure_logging

import importers.settings
from importers.taxonomy import taxonomy_settings
from importers.repository import elastic

from importers.taxonomy.fetch_values_from_taxonomy import fetch_and_convert_values

configure_logging([__name__.split('.')[0], 'jobtech'])
log = logging.getLogger(__name__)


def check_if_taxonomy_index_exists():
    expected_index_name = importers.settings.ES_TAX_INDEX
    log.info('Expected index based on taxonomy version: %s' % expected_index_name)
    try:
        index_exists = elastic.index_exists(expected_index_name)
    except Exception as e:
        log.error('Failed to check index existence on elastic', e)
        raise
    return expected_index_name, index_exists


def update_search_engine_valuestore(indexname, indexexists, values):
    # Create and/or update valuestore index
    try:
        log.info("Creating index: %s and loading taxonomy" % indexname)
        elastic.create_index(indexname, taxonomy_settings.TAXONOMY_INDEX_CONFIGURATION)
        elastic.bulk_index(values, indexname, ['type', 'concept_id'])
    except Exception as e:
        log.error('Failed to load values into search engine', e)
        raise
    # Create and/or assign index to taxonomy alias and
    # assign old index to archive alias
    try:
        if elastic.alias_exists(importers.settings.ES_TAX_INDEX_ALIAS):
            log.info("Updating alias: %s" % importers.settings.ES_TAX_INDEX_ALIAS)
            alias = elastic.get_alias(importers.settings.ES_TAX_INDEX_ALIAS)
            elastic.update_alias(
                [indexname], list(alias.keys()), importers.settings.ES_TAX_INDEX_ALIAS)
            if not indexexists:
                if elastic.alias_exists(importers.settings.ES_TAX_ARCHIVE_ALIAS):
                    log.info("Adding index: %s to archive alias: %s" % (indexname,
                                                                        importers.settings.ES_TAX_ARCHIVE_ALIAS))
                    elastic.add_indices_to_alias(list(alias.keys()),
                                                 importers.settings.ES_TAX_ARCHIVE_ALIAS)
                else:
                    log.info("Creating alias: %s and adding index: %s" % (
                    importers.settings.ES_TAX_ARCHIVE_ALIAS, indexname))
                    elastic.put_alias(
                        list(alias.keys()), importers.settings.ES_TAX_ARCHIVE_ALIAS)
        else:
            log.info("Creating alias: %s and inserting index: %s" % (importers.settings.ES_TAX_INDEX_ALIAS, indexname))
            elastic.put_alias([indexname], importers.settings.ES_TAX_INDEX_ALIAS)
    except Exception as e:
        log.error('Failed to update aliases', e)
        raise


def start():
    (indexname, indexexist) = check_if_taxonomy_index_exists()
    values = fetch_and_convert_values()
    update_search_engine_valuestore(indexname, indexexist, values)
    log.info("Import-taxonomy finished")


if __name__ == '__main__':
    start()
