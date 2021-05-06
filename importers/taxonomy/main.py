import logging
from datetime import datetime

import importers.settings
from jobtech.common.customlogging import configure_logging
from importers.repository import elastic
from importers.taxonomy import taxonomy_settings
from importers.taxonomy.fetch_values_from_taxonomy import fetch_and_convert_values, fetch_taxonomy_version

configure_logging([__name__.split('.')[0], 'jobtech'])
log = logging.getLogger(__name__)


def check_if_taxonomy_update(version, version_timestamp):
    if version_timestamp:
        version_date = ''.join(version_timestamp[:10].split('-'))
    else:
        version_date = 0
    index_name = elastic.get_index_name_for_alias(importers.settings.ES_TAX_INDEX_ALIAS)
    if index_name:
        try:
            index_timestamp = index_name.split('-')[2]
        except IndexError as e:  # Taxonomy alias points to index with old naming style
            index_timestamp = 0
            log.warning(f"Set timestamp: {index_timestamp} to index: {index_name}. Index timestamp error: {e}")
    else:
        index_timestamp = 0
        log.warning(f"Set timestamp: {index_timestamp}. Check ES_TAX_INDEX_ALIAS env var!")

    if version_date and int(version_date) >= int(index_timestamp):
        log.info(f"Updating taxonomy. Taxonomy timestamp: {version_timestamp} is newer than index: {index_name}")
        return True
    else:
        log.info(f"No taxonomy update. Current index: {index_name} is created from latest taxonomy version: {version}, {version_timestamp}")
        return False


def update_taxonomy_index(index_name, values):
    try:
        log.info(f"Creating index: {index_name} and loading taxonomy")
        elastic.create_index(index_name, taxonomy_settings.TAXONOMY_INDEX_CONFIGURATION)
        elastic.bulk_index(values, index_name, ['type', 'concept_id'])
    except Exception as e:
        log.error('Failed to create index', e)
        raise
    # Create and/or assign index to taxonomy alias and
    # assign old index to archive alias
    try:
        if elastic.alias_exists(importers.settings.ES_TAX_INDEX_ALIAS):
            log.info(f"Updating alias: {importers.settings.ES_TAX_INDEX_ALIAS}")
            alias = elastic.get_alias(importers.settings.ES_TAX_INDEX_ALIAS)
            elastic.update_alias(
                [index_name], list(alias.keys()), importers.settings.ES_TAX_INDEX_ALIAS)
        else:
            log.info(f"Creating alias: {importers.settings.ES_TAX_INDEX_ALIAS} and inserting index: {index_name}")
            elastic.put_alias([index_name], importers.settings.ES_TAX_INDEX_ALIAS)
    except Exception as e:
        log.error(f'Failed to update aliases. {e}')
        raise


def start():
    version, version_timestamp = fetch_taxonomy_version()
    if check_if_taxonomy_update(version, version_timestamp):
        new_index_name = f"{importers.settings.ES_TAX_INDEX}-v{version}-{datetime.now().strftime('%Y%m%d-%H%M')}"
        log.info(f"Start creating new taxonomy index: {new_index_name}, version: {version}")
        values = fetch_and_convert_values()
        update_taxonomy_index(new_index_name, values)
        log.info("Import-taxonomy finished")
    else:
        log.info("No taxonomy update, current index is up to date")


if __name__ == '__main__':
    start()
