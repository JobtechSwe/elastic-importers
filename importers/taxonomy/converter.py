import logging
from collections import OrderedDict
from valuestore.taxonomy import JobtechTaxonomy as jt


logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)
log = logging.getLogger(__name__)


def create_valuestore_jobs(taxonomy_jobterms, taxonomy_jobgroups,
                           taxonomy_jobfields):
    jobfields = {
        field['LocaleFieldID']: OrderedDict(
            [('legacy_ams_taxonomy_id', str(field['LocaleFieldID'])),
             ('type', jt.OCCUPATION_FIELD),
             ('label', field['Term']),
             ('concept_id', str(field['uuid_id'])),
             ('description', field['Description']),
             ('legacy_ams_taxonomy_num_id', int(field['LocaleFieldID']))])
        for field in taxonomy_jobfields
    }
    jobgroups = {
        field['LocaleCode']: OrderedDict(
            [('legacy_ams_taxonomy_id', str(field['LocaleCode'])),
             ('type', jt.OCCUPATION_GROUP),
             ('label', field['Term']),
             ('concept_id', str(field['uuid_id'])),
             ('description', field['Description']),
             ('legacy_ams_taxonomy_num_id', int(field['LocaleCode'])),
             ('parent', jobfields[field['LocaleFieldID']])])
        for field in taxonomy_jobgroups
    }
    jobterms = {
        field['OccupationNameID']:
        OrderedDict([('legacy_ams_taxonomy_id', str(field['OccupationNameID'])),
                     ('type', jt.OCCUPATION_NAME),
                     ('label', field['Term']),
                     ('concept_id', str(field['uuid_id'])),
                     ('legacy_ams_taxonomy_num_id', int(field['OccupationNameID'])),
                     ('parent', jobgroups[field['LocaleCode']])])
        for field in taxonomy_jobterms
    }
    return (jobterms, jobgroups, jobfields)


def create_valuestore_geo(file_places, taxonomy_municipalities, taxonomy_regions,
                          taxonomy_countries):
    countries = {
        field['CountryID']:
        OrderedDict([('legacy_ams_taxonomy_id', str(field['CountryID'])),
                     ('type', jt.COUNTRY),
                     ('label', field['Term']),
                     ('concept_id', str(field['uuid_id'])),
                     ('legacy_ams_taxonomy_num_id', int(field['CountryID'])),
                     ('country_code', field['CountryCode'])])
        for field in taxonomy_countries
    }
    regions = {
        field['NationalNUTSLevel3Code']: OrderedDict(
            [('legacy_ams_taxonomy_id', str(field['NationalNUTSLevel3Code'])),
             ('type', jt.REGION),
             ('label', field['Term']),
             ('concept_id', str(field['uuid_id'])),
             ('legacy_ams_taxonomy_num_id', int(field['NationalNUTSLevel3Code']))])
        for field in taxonomy_regions
    }
    municipalities = {
        field['NationalNUTSLAU2Code']: OrderedDict(
            [('legacy_ams_taxonomy_id', str(field['NationalNUTSLAU2Code'])),
             ('type', jt.MUNICIPALITY),
             ('concept_id', str(field['uuid_id'])),
             ('label', field['Term']),
             ('parent', regions[field['NationalNUTSLevel3Code']]),
             ('legacy_ams_taxonomy_num_id', int(field['NationalNUTSLAU2Code']))])
        for field in taxonomy_municipalities
    }

    places = {}
    for place in file_places:
        identifier = "%s-%s" % (place['kommunkod'], _slugify(place['label']))
        #concept_id = str(place["concept_id"])
        municipality = municipalities[place['kommunkod']]
        places[identifier] = dict({'legacy_ams_taxonomy_id': identifier,
                                   'concept_id': place["uuid_id"]}, **place)
        #del places[identifier]['concept_id']
        places[identifier]['parent'] = municipality
    #print(places)
    return (places, municipalities, regions, countries)


def _slugify(string):
    return string.lower().replace('å', 'a') \
            .replace('ä', 'a').replace('ö', 'o').replace(' ', '_') if string else None


def create_valuestore_skills(taxonomy_skills):
    skills = {
        field['SkillID']:
        OrderedDict([('legacy_ams_taxonomy_id', str(field['SkillID'])),
                     ('type', jt.SKILL),
                     ('label', field['Term']),
                     ('concept_id', str(field['uuid_id'])),
                     ('description', field['Term'])])
        for field in taxonomy_skills
    }
    return (skills)


def create_valuestore_work_time_extent(taxonomy_work_time_extent):
    wte = {
        field['WorkTimeExtentID']:
        OrderedDict([('legacy_ams_taxonomy_id', str(field['WorkTimeExtentID'])),
                     ('type', jt.WORKTIME_EXTENT),
                     ('label', field['Term']),
                     ('concept_id', str(field['uuid_id']))])
        for field in taxonomy_work_time_extent
    }
    return (wte)


def create_valuestore_languages(taxonomy_languages):
    languages = {
        field['LanguageID']:
        OrderedDict([('legacy_ams_taxonomy_id', str(field['LanguageID'])),
                     ('type', jt.LANGUAGE),
                     ('label', field['Term']),
                     ('concept_id', str(field['uuid_id'])),
                     ('legacy_ams_taxonomy_num_id', int(field['LanguageID']))])
        for field in taxonomy_languages
    }
    return (languages)


def create_valuestore_employment_types(taxonomy_employmenttypes):
    employment_types = {
        field['EmploymentTypeID']:
        OrderedDict([('legacy_ams_taxonomy_id', str(field['EmploymentTypeID'])),
                     ('type', jt.EMPLOYMENT_TYPE),
                     ('label', field['Term']),
                     ('concept_id', str(field['uuid_id'])),
                     ('legacy_ams_taxonomy_num_id', int(field['EmploymentTypeID']))])
        for field in taxonomy_employmenttypes
    }
    return employment_types


def create_valuestore_driving_licence(taxonomy_drivinglicence):
    driving_licence = {
        field['DrivingLicenceID']:
        OrderedDict([('legacy_ams_taxonomy_id', str(field['DrivingLicenceID'])),
                     ('type', jt.DRIVING_LICENCE),
                     ('label', field['Term']),
                     ('concept_id', str(field['uuid_id'])),
                     ('description', field['Description']),
                     ('legacy_ams_taxonomy_num_id', int(field['DrivingLicenceID']))])
        for field in taxonomy_drivinglicence
    }
    return driving_licence


def create_valuestore_wagetype(taxonomy_wagetype):
    wage_type = {
        field['WageTypeID']:
        OrderedDict([('legacy_ams_taxonomy_id', str(field['WageTypeID'])),
                     ('type', jt.WAGE_TYPE),
                     ('label', field['Term']),
                     ('concept_id', str(field['uuid_id'])),
                     ('legacy_ams_taxonomy_num_id', int(field['WageTypeID']))])
        for field in taxonomy_wagetype
    }
    return wage_type


def create_valuestore_education_fields(taxonomy_education_field_SUN1, taxonomy_education_field_SUN2, taxonomy_education_field_SUN3):
    education_field_SUN1 = {
        field['SUNField1ID']:
        OrderedDict([('legacy_ams_taxonomy_id', str(field['SUNField1Code'])),
                     ('type', jt.SUN_EDUCATION_FIELD_1),
                     ('label', field['Term']),
                     ('concept_id', str(field['uuid_id'])),
                     ('legacy_ams_taxonomy_num_id', int(field['SUNField1Code']))])
        for field in taxonomy_education_field_SUN1
    }

    education_field_SUN2 = {
        field['SUNField2ID']: OrderedDict(
            [('legacy_ams_taxonomy_id', str(field['SUNField2Code'])),
             ('type',  jt.SUN_EDUCATION_FIELD_2),
             ('label', field['Term']),
             ('concept_id', str(field['uuid_id'])),
             ('legacy_ams_taxonomy_num_id', int(field['SUNField2Code'])),
             ('parent', education_field_SUN1[field['SUNField1ID']])])
        for field in taxonomy_education_field_SUN2
    }

    education_field_SUN3 = {
        field['SUNField3ID']: OrderedDict(
            [('legacy_ams_taxonomy_id', str(field['SUNField3Code'])),
             ('type',  jt.SUN_EDUCATION_FIELD_3),
             ('label', field['Term']),
             ('concept_id', str(field['uuid_id'])),
             ('legacy_ams_taxonomy_num_id', int(field['SUNField3ID'])),
             ('parent', education_field_SUN2[field['SUNField2ID']])])
        for field in taxonomy_education_field_SUN3
    }

    return (education_field_SUN1, education_field_SUN2, education_field_SUN3)


def create_valuestore_education_levels(taxonomy_education_levels_SUN1, taxonomy_education_levels_SUN2,
                                       taxonomy_education_levels_SUN3):
    education_levels_SUN1 = {
        field['SUNLevel1ID']:
            OrderedDict([('legacy_ams_taxonomy_id', str(field['SUNLevel1Code'])),
                         ('type', jt.SUN_EDUCATION_LEVEL_1),
                         ('label', field['Term']),
                         ('concept_id', str(field['uuid_id'])),
                         ('legacy_ams_taxonomy_num_id', int(field['SUNLevel1Code']))])
        for field in taxonomy_education_levels_SUN1
    }

    education_levels_SUN2 = {
        field['SUNLevel2ID']: OrderedDict(
            [('legacy_ams_taxonomy_id', str(field['SUNLevel2Code'])),
             ('type',  jt.SUN_EDUCATION_LEVEL_2),
             ('label', field['Term']),
             ('concept_id', str(field['uuid_id'])),
             ('legacy_ams_taxonomy_num_id', int(field['SUNLevel2Code'])),
             ('parent', education_levels_SUN1[field['SUNLevel1ID']])])
        for field in taxonomy_education_levels_SUN2
    }

    education_levels_SUN3 = {
        field['SUNLevel3ID']: OrderedDict(
            [('legacy_ams_taxonomy_id', str(field['SUNLevel3Code'])),
             ('type',  jt.SUN_EDUCATION_LEVEL_3),
             ('label', field['Term']),
             ('concept_id', str(field['uuid_id'])),
             ('legacy_ams_taxonomy_num_id', int(field['SUNLevel3Code'])),
             ('parent', education_levels_SUN2[field['SUNLevel2ID']])])
        for field in taxonomy_education_levels_SUN3
    }

    return (education_levels_SUN1, education_levels_SUN2, education_levels_SUN3)


def create_valuestore_deprecated_education_level(taxonomy_education_level):
    education_level = {
        field['EducationLevelID']:
            OrderedDict([('legacy_ams_taxonomy_id', str(field['EducationLevelID'])),
                         ('type', jt.DEPRECATED_EDUCATION_LEVEL),
                         ('label', field['Term']),
                         ('concept_id', str(field['uuid_id'])),
                         ('legacy_ams_taxonomy_num_id', int(field['EducationLevelID']))])
        for field in taxonomy_education_level
    }
    return education_level


def create_valuestore_deprecated_education_field(taxonomy_education_field):
    education_field = {
        field['EducationFieldID']:
            OrderedDict([('legacy_ams_taxonomy_id', str(field['EducationFieldID'])),
                         ('type', jt.DEPRECATED_EDUCATION_FIELD),
                         ('label', field['Term']),
                         ('concept_id', str(field['uuid_id'])),
                         ('description', field['Description']),
                         ('legacy_ams_taxonomy_num_id', int(field['EducationFieldID']))])
        for field in taxonomy_education_field
    }
    return education_field


def create_valuestore_duration(taxonomy_duration):
    duration = {
        field['EmploymentDurationID']:
        OrderedDict([('legacy_ams_taxonomy_id', str(field['EmploymentDurationID'])),
                     ('type', jt.EMPLOYMENT_DURATION),
                     ('label', field['Term']),
                     ('concept_id', str(field['uuid_id'])),
                     ('EURESCode', field['EURESCode']),
                     ('legacy_ams_taxonomy_num_id', int(field['EmploymentDurationID']))])
        for field in taxonomy_duration
    }
    return duration


def create_valuestore_occupation_experience(taxonomy_occupation_experience):
    occupation_experience = {
        field['OccupationExperienceYearID']:
        OrderedDict([('legacy_ams_taxonomy_id', str(field['OccupationExperienceYearID'])),
                     ('type', jt.OCCUPATION_EXPERIENCE_YEARS),
                     ('label', field['ExperienceYearCandidate']),
                     ('concept_id', str(field['uuid_id'])),
                     ('legacy_ams_taxonomy_num_id', int(field['OccupationExperienceYearID']))])
        for field in taxonomy_occupation_experience
    }
    return occupation_experience
