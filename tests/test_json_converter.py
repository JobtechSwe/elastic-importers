from collections import OrderedDict
import importers.new_taxonomy.json_converter
import pytest


def create_test_data_element(legacy_id, type, label, concept_id, legacy_num_id):
    od = OrderedDict()
    od['legacy_ams_taxonomy_id'] = legacy_id
    od['type'] = type
    od['label'] = label
    od['concept_id'] = concept_id
    od['legacy_ams_taxonomy_num_id'] = legacy_num_id
    return od


test_data = []
test_data.append(create_test_data_element('5370',
                                          'jobterm',
                                          '1:e Fartygsingenjör/1:e Maskinist',
                                          '5bcde2bb-ff9b-4a8b-9a09-cf3c67b9a05e',
                                          3151))
test_data.append(create_test_data_element('5372',
                                          'jobterm',
                                          '2:e Fartygsingenjör/2:e Maskinist',
                                          '5bcde2bb-9c6b-4ad0-8e1f-aa086598b663',
                                          3151))
test_data.append(create_test_data_element('5369',
                                          'jobterm',
                                          '2:e Styrman',
                                          '5bcde2bb-5854-4ced-bf3e-7fccef284357',
                                          3151))


@pytest.mark.unit
def test_taxonomy_to_concept():
    result = importers.new_taxonomy.json_converter.taxonomy_to_concept(test_data)
