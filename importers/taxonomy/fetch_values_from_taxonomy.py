from importers.taxonomy import settings
import requests

from importers.taxonomy.converter import convert_occupation_value, convert_general_value, convert_value_with_replaced, \
    convert_region_value
from importers.taxonomy.queries import OCCUPATIONS_QUERY, GENERAL_QUERY, QUERY_WITH_REPLACED, REGION_QUERY


def _fetch_taxonomy_values(params):
    headers = {"api-key": settings.TAXONOMY_VERSION_2_API_KEY, }
    taxonomy_2_response = requests.get(settings.TAXONOMY_VERSION_2_URL, headers=headers, params=params)
    taxonomy_2_response.raise_for_status()
    return taxonomy_2_response.json()


def _fetch_value(query):
    params = {'query': query}
    response = _fetch_taxonomy_values(params)
    values = response.get('data', {}).get('concepts', [])
    return values


def fetch_and_convert_values():
    converted_values = []
    occupations = _fetch_value(OCCUPATIONS_QUERY)
    converted_values += [item for value in occupations for item in convert_occupation_value(value)]
    regions = _fetch_value(REGION_QUERY)
    converted_values += [convert_region_value(region) for region in regions.get('narrower')]
    general_types = settings.GENERAL_VALUES
    types_with_replaced = settings.REPLECED_VALUES

    for type in general_types:
        field = '"' + type + '"'
        values = _fetch_value(GENERAL_QUERY % field)
        converted_values += [convert_general_value(value, type) for value in values]

    for type in types_with_replaced:
        field = '"' + type + '"'
        values = _fetch_value(QUERY_WITH_REPLACED % field)
        converted_values += [convert_value_with_replaced(value, type) for value in values]
    return converted_values
