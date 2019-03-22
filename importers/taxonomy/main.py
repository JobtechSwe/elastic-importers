import logging
from importers.taxonomy import settings, taxonomy_service
from importers.repository import elastic
import pickle
import os


logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)
log = logging.getLogger(__name__)


def check_if_taxonomyversion_already_exists():
    try:
        tax_versions = taxonomy_service.get_taxonomy_version()
    except Exception as e:
        log.error('Failed to get taxonomy version from taxonomy service', e)
        raise
    highest_version = max([v['BastaxonomiId'] for v in tax_versions])
    expected_index_name = settings.ES_TAX_INDEX_BASE + str(highest_version)
    log.info(
        'Expected index name based on taxonomy service version is {}'.format(
            expected_index_name))
    try:
        index_exists = elastic.index_exists(expected_index_name)
    except Exception as e:
        log.error('Failed to check index existence on elastic', e)
        raise
    return (expected_index_name, index_exists)


def unpickle_values():
    with open(settings.resources_folder + "values.pickle", "rb") as fin:
        data = pickle.load(fin)
        return data


def update_search_engine_valuestore(indexname, indexexists, values):
    # Create and/or update valuestore index
    try:
        log.info("creating index {} and loading taxonomy".format(indexname))
        elastic.create_index(indexname, settings.TAXONOMY_INDEX_CONFIGURATION)
        elastic.bulk_index(values, indexname, ['type', 'concept_id'])
    except Exception as e:
        log.error('Failed to load values into search engine', e)
        raise
    # Create and/or assign index to taxonomy alias and
    # assign old index to archive alias
    try:
        if (elastic.alias_exists(settings.ES_TAX_INDEX_ALIAS)):
            log.info("updating alias {}".format(settings.ES_TAX_INDEX_ALIAS))
            alias = elastic.get_alias(settings.ES_TAX_INDEX_ALIAS)
            elastic.update_alias(
                indexname, list(alias.keys()), settings.ES_TAX_INDEX_ALIAS)
            if (not indexexists):
                if (elastic.alias_exists(settings.ES_TAX_ARCHIVE_ALIAS)):
                    log.info("Adding index {} to archive alias {}".format(indexname, settings.ES_TAX_ARCHIVE_ALIAS))
                    elastic.add_indices_to_alias(list(alias.keys()),
                                                 settings.ES_TAX_ARCHIVE_ALIAS)
                else:
                    log.info("Creating {} alias and adding index {}".format(settings.ES_TAX_ARCHIVE_ALIAS))
                    elastic.put_alias(
                        list(alias.keys()), settings.ES_TAX_ARCHIVE_ALIAS)
        else:
            log.info("creating alias {} and inserting index {}".format(settings.ES_TAX_INDEX_ALIAS, indexname))
            elastic.put_alias([indexname], settings.ES_TAX_INDEX_ALIAS)
    except Exception as e:
        log.error('Failed to update aliases', e)
        raise


def start():
    (indexname, indexexist) = check_if_taxonomyversion_already_exists()
    values = unpickle_values()
    update_search_engine_valuestore(indexname, indexexist, values)
    log.info("import-taxonomy from pickles finished")


if __name__ == '__main__':
    start()
