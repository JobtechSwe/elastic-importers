import logging
from datetime import datetime

from jobtech.common.customlogging import configure_logging

import importers.settings
from importers.taxonomy import taxonomy_settings
from importers.repository import elastic

from importers.taxonomy.fetch_values_from_taxonomy import fetch_and_convert_values, fetch_taxonomy_version

configure_logging([__name__.split('.')[0], 'jobtech'])
log = logging.getLogger(__name__)


def check_if_taxonomy_update():
    version_timestamp = fetch_taxonomy_version()
    if version_timestamp:
        version_date = ''.join(version_timestamp[:10].split('-'))
    else:
        version_date = 0
    index_name = elastic.get_index_name_for_alias(importers.settings.ES_TAX_INDEX_ALIAS)
    if index_name:
        index_timestamp = index_name.split('-')[-1]
    else:
        index_timestamp = 0

    if version_date and int(version_date) > int(index_timestamp):
        return True
    else:
        return False

def update_taxonomy_index(indexname, values):
    try:
        log.info(f"Creating index: {indexname} and loading taxonomy")
        elastic.create_index(indexname, taxonomy_settings.TAXONOMY_INDEX_CONFIGURATION)
        elastic.bulk_index(values, indexname, ['type', 'concept_id'])
    except Exception as e:
        log.error('Failed to create index', e)
        raise
    # Create and/or assign index to taxonomy alias and
    # assign old index to archive alias
    try:
        if elastic.alias_exists(importers.settings.ES_TAX_INDEX_ALIAS):
            log.info("Updating alias: %s" % importers.settings.ES_TAX_INDEX_ALIAS)
            alias = elastic.get_alias(importers.settings.ES_TAX_INDEX_ALIAS)
            elastic.update_alias(
                [indexname], list(alias.keys()), importers.settings.ES_TAX_INDEX_ALIAS)
        else:
            log.info("Creating alias: %s and inserting index: %s" % (importers.settings.ES_TAX_INDEX_ALIAS, indexname))
            elastic.put_alias([indexname], importers.settings.ES_TAX_INDEX_ALIAS)
    except Exception as e:
        log.error('Failed to update aliases', e)
        raise


def start():
    if check_if_taxonomy_update():
        new_index_name = "%s-%s" % (importers.settings.ES_TAX_INDEX, datetime.now().strftime('%Y%m%d-%H%M'))
        log.info(f"Start creating new taxonomy index: {new_index_name}")
        values = fetch_and_convert_values()
        update_taxonomy_index(new_index_name, values)
        log.info("Import-taxonomy finished")
    else:
        log.info("No taxonomy update, No need to update")


if __name__ == '__main__':
    start()
