import logging

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)
log = logging.getLogger(__name__)


def _standard_format(legacy_id, type, label, concept_id):
    if legacy_id:
        legacy_num_id = int(legacy_id)
    else:
        legacy_num_id = None
    return {
        'legacy_ams_taxonomy_id': legacy_id,
        'type': type,
        'label': label,
        'concept_id': concept_id,
        'legacy_ams_taxonomy_num_id': legacy_num_id}


def _add_replaced_format(legacy_id, type, label, concept_id, replaced_by):
    result = _standard_format(legacy_id, type, label, concept_id)
    result['replaced_by'] = []
    if replaced_by:
        for replaced in replaced_by:
            result['replaced_by'].append(_standard_format(replaced.get('deprecated_legacy_id', None), type,
                                                          replaced.get('preferred_label', None), replaced.get('id')))
    return result


def convert_occupation_value(value):
    convert_values = []
    occupation_name = _standard_format(value.get('deprecated_legacy_id', None), 'occupation-name',
                                       value.get('preferred_label', None), value.get('id'))
    occupation_name['collection'] = {}
    occupation_name['replaced_by'] = []
    occupation_name['parent'] = {}

    collection_value = value.get('related', [])
    replaced_values = value.get("replaced_by", [])
    parent_value = value.get('broader', {})

    if not parent_value and replaced_values:   # If Occupation name is deprecated, replaced value is used
        parent_value = replaced_values[0].get('broader', {})

    if parent_value:
        parent_value = parent_value[0]
        occupation_group = _standard_format(parent_value.get('ssyk_code_2012', None), 'occupation-group',
                                            parent_value.get("preferred_label", None),  parent_value.get('id'))
        occupation_group['parent'] = {}
        grandparent_value = parent_value.get('broader', {})
        if grandparent_value:
            grandparent_value = grandparent_value[0]
            occupation_field = _standard_format(grandparent_value.get('deprecated_legacy_id', None), 'occupation-field',
                                                grandparent_value.get("preferred_label", None),  grandparent_value.get('id'))
            convert_values.append(occupation_field)
            occupation_group['parent'] = occupation_field
        else:
            log.warning(
                f"There is no occupation field for {parent_value.get('id', None)} - {parent_value.get('preferred_label', None)}")
        convert_values.append(occupation_group)
        occupation_name['parent'] = occupation_group
    else:
        log.warning(f"There is no occupation group for {value.get('id', None)} - {value.get('preferred_label', None)}")

    if collection_value:  # Add collection field
        collection_value = collection_value[0]
        collection = _standard_format(collection_value.get('deprecated_legacy_id', None), 'collection_value',
                                      collection_value.get("preferred_label", None), collection_value.get('id'))
        occupation_name['collection'] = collection

    if replaced_values:  # Add replaced value
        replaced = []
        for replaced_value in replaced_values:
            replaced.append(_standard_format(replaced_value.get('deprecated_legacy_id', None), 'collection_value',
                                             replaced_value.get("preferred_label", None), replaced_value.get('id')))
        occupation_name['replaced_by'] = replaced
    convert_values.append(occupation_name)
    return convert_values


def convert_region_value(value):
    return _standard_format(value.get('national_nuts_level_3_code_2019', None), 'region',
                            value.get("preferred_label", None), value.get('id'))


def convert_municipality_value(value):
    return _standard_format(value.get('lau_2_code_2015', None), 'municipality',
                            value.get("preferred_label", None), value.get('id'))


def convert_general_value(value, type):
    return _standard_format(value.get('deprecated_legacy_id', None), type,
                            value.get("preferred_label", None), value.get('id'))


def convert_value_with_replaced(value, type):
    return _add_replaced_format(value.get('deprecated_legacy_id', None), type,
                                value.get("preferred_label", None), value.get('id'),
                                value.get("replaced_by", []))
