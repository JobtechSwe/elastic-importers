import importers.settings
from importers.taxonomy import taxonomy_settings, log
from importers import settings
import requests

from importers.taxonomy.converter import convert_occupation_value, convert_general_value, convert_value_with_replaced, \
    convert_region_value
from importers.taxonomy.queries import OCCUPATIONS_QUERY, GENERAL_QUERY, QUERY_WITH_REPLACED, REGION_QUERY


def fetch_taxonomy_version():
    try:
        headers = {"api-key": importers.settings.TAXONOMY_API_KEY}
        taxonomy_response = requests.get(url=settings.TAXONOMY_VERSION_URL, headers=headers)
        taxonomy_response.raise_for_status()
        versions = taxonomy_response.json()
        latest_version = 0
        timestamp = None
        for version in versions:
            if version.get("taxonomy/version") > latest_version:
                latest_version = version.get("taxonomy/version")
                timestamp = version.get("taxonomy/timestamp")
                log.info(f"Taxonomy version: {latest_version}, timestamp: {timestamp}")
        return latest_version, timestamp
    except Exception as e:
        log.error('Failed to fetch taxonomy version', e)


def _fetch_taxonomy_values(params):
    headers = {"api-key": importers.settings.TAXONOMY_API_KEY, }
    taxonomy_response = requests.get(url=settings.TAXONOMY_GRAPHQL_URL, headers=headers, params=params)
    taxonomy_response.raise_for_status()
    return taxonomy_response.json()


def _fetch_value(query):
    params = {'query': query}
    response = _fetch_taxonomy_values(params)
    values = response.get('data', {}).get('concepts', [])
    return values


def fetch_and_convert_values():
    converted_values = []
    occupations = _fetch_value(OCCUPATIONS_QUERY)
    log.info(f"Occupations: {occupations}")
    converted_values += [item for value in occupations for item in convert_occupation_value(value)]
    regions = _fetch_value(REGION_QUERY)
    log.info(f"Regions: {regions}")
    if regions:
        converted_values += [convert_region_value(region) for region in regions[0].get('narrower', [])]
    else:
        log.warning("Could not fetch regions")
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
