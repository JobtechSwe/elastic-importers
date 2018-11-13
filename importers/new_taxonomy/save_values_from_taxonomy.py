import logging
import json
from importers.new_taxonomy import settings, taxonomy_service, converter
from pkg_resources import resource_string
import pickle
from importers.new_taxonomy import json_converter
import os


logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)
log = logging.getLogger(__name__)
concept_id_counter = 0  # 100000001


def fetch_full_taxonomy():
    try:
        log.info("Fetchning taxonomy")
        taxonomy_jobfields = taxonomy_service.get_all_job_fields()
        taxonomy_jobfields = add_concept_id(taxonomy_jobfields)  # adding concept_id
        taxonomy_jobgroups = taxonomy_service.get_all_job_groups()
        taxonomy_jobgroups = add_concept_id(taxonomy_jobgroups)  # adding concept_id
        taxonomy_jobterms = taxonomy_service.get_all_job_terms()
        taxonomy_jobterms = add_concept_id(taxonomy_jobterms)  # adding concept_id
        taxonomy_countries = taxonomy_service.get_all_countries()
        taxonomy_countries = add_concept_id(taxonomy_countries)  # adding concept_id
        taxonomy_regions = taxonomy_service.get_all_regions()
        taxonomy_regions = add_concept_id(taxonomy_regions)  # adding concept_id
        taxonomy_municipalities = taxonomy_service.get_all_municipalities()
        taxonomy_municipalities = add_concept_id(taxonomy_municipalities)  # adding concept_id
        taxonomy_languages = taxonomy_service.get_all_languages()
        taxonomy_languages = add_concept_id(taxonomy_languages)  # adding concept_id
        taxonomy_work_time_extent = taxonomy_service.get_all_work_time_extent()
        taxonomy_work_time_extent = add_concept_id(taxonomy_work_time_extent)  # adding concept_id
        taxonomy_skills = taxonomy_service.get_all_skills()
        taxonomy_skills = add_concept_id(taxonomy_skills)  # adding concept_id
        taxonomy_employmenttypes = taxonomy_service.get_all_employment_types()
        taxonomy_employmenttypes = add_concept_id(taxonomy_employmenttypes)  # adding concept_id
        taxonomy_drivinglicence = taxonomy_service.get_all_driving_licences()
        taxonomy_drivinglicence = add_concept_id(taxonomy_drivinglicence)  # adding concept_id
        taxonomy_wagetype = taxonomy_service.get_all_wage_type()
        taxonomy_wagetype = add_concept_id(taxonomy_wagetype)  # adding concept_id
        taxonomy_education_level = taxonomy_service.get_all_education_levels()
        taxonomy_education_level = add_concept_id(taxonomy_education_level)  # adding concept_id
        taxonomy_education_field = taxonomy_service.get_all_education_fields()
        taxonomy_education_field = add_concept_id(taxonomy_education_field)  # adding concept_id
        taxonomy_duration = taxonomy_service.get_all_duration()
        taxonomy_duration = add_concept_id(taxonomy_duration)  # adding concept_id
        taxonomy_occupation_experience = taxonomy_service.get_all_occupation_experience()
        taxonomy_occupation_experience = add_concept_id(taxonomy_occupation_experience)  # adding concept_id
    except Exception as e:
        log.error('Failed to fetch valuesets from Taxonomy Service', e)
        raise
    # Load places
    file_places = json.loads(resource_string('importers.taxonomy.resources',
                                             'platser.json').decode('utf-8'))
    file_places = add_concept_id(file_places)
    log.info("Converting taxonomy to elastic format")
    (valuestore_jobterm,
     valuestore_jobgroup,
     valuestore_jobfield) = converter.create_valuestore_jobs(taxonomy_jobterms,
                                                             taxonomy_jobgroups,
                                                             taxonomy_jobfields)
    (valuestore_places,
     valuestore_municipalities,
     valuestore_regions,
     valuestore_countries) = converter.create_valuestore_geo(file_places,
                                                             taxonomy_municipalities,
                                                             taxonomy_regions,
                                                             taxonomy_countries)
    valuestore_languages = converter.create_valuestore_languages(taxonomy_languages)
    valuestore_work_time_extent = converter.create_valuestore_work_time_extent(taxonomy_work_time_extent)
    valuestore_skills = converter.create_valuestore_skills(taxonomy_skills)
    valuestore_employmenttypes = converter.create_valuestore_employment_types(taxonomy_employmenttypes)
    valuestore_drivinglicences = converter.create_valuestore_driving_licence(taxonomy_drivinglicence)
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


def add_concept_id(value_category):
    global concept_id_counter
    uuid_list = get_uuids()
    for i in value_category:
        i["uuid_id"] = uuid_list[concept_id_counter]
        concept_id_counter += 1
    return value_category


def get_uuids():
    with open(settings.resources_folder + "uuid.txt", "r") as fin:
        data = fin.read()
        data = data.replace('"', '')
        uuid_list = data.split("\n")
        return uuid_list


def pickle_values(all_values):
    with open(settings.resources_folder + "values.pickle", "wb") as fout:
        pickle.dump(all_values, fout)


def start():
    all_values = fetch_full_taxonomy()
    #print(len(all_values))
    #print(concept_id_counter)
    pickle_values(all_values)
    json_converter.save_concept_to_taxonomy_as_json(all_values)
    json_converter.save_taxonomy_to_concept_as_json(all_values)


if __name__ == '__main__':
    start()
