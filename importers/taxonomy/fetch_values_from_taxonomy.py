import importers.settings
from importers.taxonomy import taxonomy_settings
from importers import settings
import requests

from importers.taxonomy.converter import convert_occupation_value, convert_general_value, convert_value_with_replaced
from importers.taxonomy.queries import OCCUPATIONS_QUERY, GENERAL_QUERY, QUERY_WITH_REPLACED


def _fetch_taxonomy_values(params):
    headers = {"api-key": importers.settings.TAXONOMY_API_KEY, }
    url = f"{settings.TAXONOMY_URL}/graphql?"
    taxonomy_2_response = requests.get(url, headers=headers, params=params)
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
    general_types = taxonomy_settings.GENERAL_VALUES
    types_with_replaced = taxonomy_settings.REPLACED_VALUES

    for type in general_types:
        field = '"' + type + '"'
        values = _fetch_value(GENERAL_QUERY % field)
        converted_values += [convert_general_value(value, type) for value in values]

    for type in types_with_replaced:
        field = '"' + type + '"'
        values = _fetch_value(QUERY_WITH_REPLACED % field)
        converted_values += [convert_value_with_replaced(value, type) for value in values]
    return converted_values
