import logging
import json
from importers.taxonomy import settings, taxonomy_service, converter
from importers.repository import elastic
from pkg_resources import resource_string


logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)
log = logging.getLogger(__name__)


def fetch_full_taxonomy():
    try:
        log.info("Fetchning taxonomy")
        taxonomy_jobfields = taxonomy_service.get_all_job_fields()
        taxonomy_jobgroups = taxonomy_service.get_all_job_groups()
        taxonomy_jobterms = taxonomy_service.get_all_job_terms()
        taxonomy_countries = taxonomy_service.get_all_countries()
        taxonomy_regions = taxonomy_service.get_all_regions()
        taxonomy_municipalities = taxonomy_service.get_all_municipalities()
        taxonomy_languages = taxonomy_service.get_all_languages()
        taxonomy_work_time_extent = taxonomy_service.get_all_work_time_extent()
        taxonomy_skills = taxonomy_service.get_all_skills()
        taxonomy_employmenttypes = taxonomy_service.get_all_employment_types()
        taxonomy_drivinglicence = taxonomy_service.get_all_driving_licences()
        taxonomy_wagetype = taxonomy_service.get_all_wage_type()
        taxonomy_education_level = taxonomy_service.get_all_education_levels()
        taxonomy_education_field = taxonomy_service.get_all_education_fields()
        taxonomy_duration = taxonomy_service.get_all_duration()
        taxonomy_occupation_experience = taxonomy_service.get_all_occupation_experience()
    except Exception as e:
        log.error('Failed to fetch valuesets from Taxonomy Service', e)
        raise
    # Load places
    file_places = json.loads(resource_string('importers.taxonomy.resources',
                                             'platser.json').decode('utf-8'))
    log.info("Converting taxonomy to elastic format")
    (valuestore_jobterm,
     valuestore_jobgroup, valuestore_jobfield) = converter.create_valuestore_jobs(
         taxonomy_jobterms, taxonomy_jobgroups, taxonomy_jobfields)
    (valuestore_places, valuestore_municipalities,
     valuestore_regions,
     valuestore_countries) = converter.create_valuestore_geo(file_places,
                                                             taxonomy_municipalities,
                                                             taxonomy_regions,
                                                             taxonomy_countries)
    valuestore_languages = converter.create_valuestore_languages(taxonomy_languages)
    valuestore_work_time_extent = converter.create_valuestore_work_time_extent(
        taxonomy_work_time_extent)
    valuestore_skills = converter.create_valuestore_skills(taxonomy_skills)
    valuestore_employmenttypes = converter.create_valuestore_employment_types(
        taxonomy_employmenttypes)
    valuestore_drivinglicences = converter.create_valuestore_driving_licence(
        taxonomy_drivinglicence)
    valuestore_wagetype = converter.create_valuestore_wagetype(taxonomy_wagetype)
    valuestore_education_level = converter.create_valuestore_education_level(taxonomy_education_level)
    valuestore_education_field = converter.create_valuestore_education_field(taxonomy_education_field)
    valuestore_duration = converter.create_valuestore_duration(taxonomy_duration)
    valuestore_occupation_experience = converter.create_valuestore_occupation_experience(taxonomy_occupation_experience)
    return (
        list(valuestore_jobterm.values())
        + list(valuestore_jobgroup.values())
        + list(valuestore_jobfield.values())
        + list(valuestore_places.values())
        + list(valuestore_municipalities.values())
        + list(valuestore_regions.values())
        + list(valuestore_countries.values())
        + list(valuestore_languages.values())
        + list(valuestore_work_time_extent.values())
        + list(valuestore_skills.values())
        + list(valuestore_employmenttypes.values())
        + list(valuestore_drivinglicences.values())
        + list(valuestore_wagetype.values())
        + list(valuestore_education_level.values())
        + list(valuestore_education_field.values())
        + list(valuestore_duration.values())
        + list(valuestore_occupation_experience.values())
    )


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
    return expected_index_name, index_exists


def update_search_engine_valuestore(indexname, indexexists, values):
    # Create and/or update valuestore index
    try:
        if not elastic.index_exists(indexname):
            log.info("creating index {} and loading taxonomy".format(indexname))
            elastic.create_index(indexname, settings.TAXONOMY_INDEX_CONFIGURATION)
        elastic.bulk_index(values, indexname, ['type', 'id'])
    except Exception as e:
        log.error('Failed to load values into search engine', e)
        raise
    # Create and/or assign index to taxonomy alias and
    # assign old index to archive alias
    try:
        if elastic.alias_exists(settings.ES_TAX_INDEX_ALIAS):
            log.info("updating alias {}".format(settings.ES_TAX_INDEX_ALIAS))
            alias = elastic.get_alias(settings.ES_TAX_INDEX_ALIAS)
            elastic.update_alias(
                indexname, list(alias.keys()), settings.ES_TAX_INDEX_ALIAS)
            if not indexexists:
                if elastic.alias_exists(settings.ES_TAX_ARCHIVE_ALIAS):
                    log.info("Adding index {} to archive alias {}".format(indexname,
                                                                          settings.ES_TAX_ARCHIVE_ALIAS))
                    elastic.add_indices_to_alias(list(alias.keys()),
                                                 settings.ES_TAX_ARCHIVE_ALIAS)
                else:
                    log.info("Creating {} alias and adding index {}".format(settings.ES_TAX_ARCHIVE_ALIAS,
                                                                            alias.keys()))
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
    values = fetch_full_taxonomy()
    update_search_engine_valuestore(indexname, indexexist, values)
    log.info("import-taxonomy finished")


if __name__ == '__main__':
    start()
