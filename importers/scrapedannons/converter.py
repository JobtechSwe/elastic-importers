from importers.repository.taxonomy import get_info_by_label_name_and_type, get_info_by_name
import logging

log = logging.getLogger(__name__)


def use_location_info_get_all_workplace_address_info_from_taxonomy(location):
    value = get_info_by_name(location)
    municipality_concept_id = None
    municipality = None
    region = None
    region_concept_id = None
    country = None
    country_concept_id = None

    if value and value.get('type') == 'place':
        value = value.get('parent', {})

    if value and value.get('type') == 'municipality':
        municipality = value.get('label')
        municipality_concept_id = value.get('concept_id')
        value_parent = value.get('parent', {})
        if value_parent:
            region = value_parent.get('label')
            region_concept_id = value_parent.get('concept_id')
            value_grandmom = value_parent.get('parent', {})
            if value_grandmom:
                country = value_grandmom.get('label')
                country_concept_id = value_grandmom.get('concept_id')
    elif value and value.get('type') == 'region':
        region = value.get('label')
        region_concept_id = value.get('concept_id')
        value_parent = value.get('parent', {})
        if value_parent:
            country = value_parent.get('label')
            country_concept_id = value_parent.get('concept_id')
    elif value and value.get('type') == 'country':
        country = value.get('label')
        country_concept_id = value.get('concept_id')

    return {
            'municipality_concept_id': municipality_concept_id,
            'municipality': municipality,
            'region_concept_id': region_concept_id,
            'region':  region,
            'country_concept_id': country_concept_id or "i46j_HmG_v64",
            'country': country or "Sverige",
        }


def convert_ad(ad_meta):
    annons = dict()
    annons['id'] = ad_meta.get('id', '')
    location = ad_meta.get('ort', '')
    annons['workplace_address'] = use_location_info_get_all_workplace_address_info_from_taxonomy(location)
    occupation_info = get_info_by_label_name_and_type(ad_meta.get('yrke', ''), 'occupation-name')
    occupation = None
    occupation_concept_id = None
    occupation_group = None
    occupation_group_concept_id = None
    occupation_field = None
    occupation_field_concept_id = None

    if occupation_info:
        occupation = occupation_info.get('label', '')
        occupation_concept_id = occupation_info.get('concept_id', '')
        occupation_group = occupation_info.get('parent', '').get('label', '')
        occupation_group_concept_id = occupation_info.get('parent', '').get('concept_id', '')
        occupation_field = occupation_info.get('parent', '').get('parent', '').get('label', '')
        occupation_field_concept_id = occupation_info.get('parent', '').get('parent', '').get('concept_id', '')

    annons['occupation'] = {
        'label': occupation,
        'concept_id': occupation_concept_id
    }
    annons['occupation_group'] = {
        'label': occupation_group,
        'concept_id': occupation_group_concept_id
    }
    annons['occupation_field'] = {
        'label': occupation_field,
        'concept_id': occupation_field_concept_id
    }
    annons['hashsum'] = ad_meta.get('hashsum', '')
    annons['originalJobPosting'] = {}
    original_job_post = ad_meta.get('originalJobPosting', '')
    if original_job_post:
        description_text = original_job_post.get('description', '')
        if type(description_text) == list:
            description_text = description_text[0]
        log.info(f"id: {annons['id']} Description: {description_text}")
        description_text_formatted = "<p>" + description_text.replace("\n", "<br>") + "</p>"
        annons['originalJobPosting'] = {
            'identifier': original_job_post.get('identifier', ''),
            'title': original_job_post.get('title', ''),
            'description': {
                'text': description_text,
                'text_formatted': description_text_formatted
            },
            'url': original_job_post.get('url', ''),
            'sameAs': original_job_post.get('sameAs', '')
        }
    return annons




