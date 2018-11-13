from collections import OrderedDict
import importers.new_taxonomy.json_converter
import pytest


def create_test_data_element(legacy_id, job_type, label, concept_id, legacy_num_id):
    od = OrderedDict()
    od['legacy_ams_taxonomy_id'] = legacy_id
    od['type'] = job_type
    od['label'] = label
    od['concept_id'] = concept_id
    od['legacy_ams_taxonomy_num_id'] = legacy_num_id
    return od


json_dict = {'jobterm':
                 {'3151':
                      {'conceptId': '5bcde2bb-ff9b-4a8b-9a09-cf3c67b9a05e',
                       'legacyAmsTaxonomyId': '3151',
                       'preferredTerm': '1:e Fartygsingenjör/1:e Maskinist',
                       'type': 'jobterm'},
                  '5370':
                      {'conceptId': '5bcde2bb-ff9b-4a8b-9a09-cf3c67b9a05e',
                       'legacyAmsTaxonomyId': '5370',
                       'preferredTerm': '1:e Fartygsingenjör/1:e Maskinist',
                       'type': 'jobterm'},
                  '5372':
                      {'conceptId': '5bcde2bb-9c6b-4ad0-8e1f-aa086598b663',
                       'legacyAmsTaxonomyId': '5372',
                       'preferredTerm': '2:e Fartygsingenjör/2:e Maskinist',
                       'type': 'jobterm'},
                  }
             }


test_data = [create_test_data_element('5370',
                                      'jobterm',
                                      '1:e Fartygsingenjör/1:e Maskinist',
                                      '5bcde2bb-ff9b-4a8b-9a09-cf3c67b9a05e',
                                      '3151'),
             create_test_data_element('5372',
                                      'jobterm',
                                      '2:e Fartygsingenjör/2:e Maskinist',
                                      '5bcde2bb-9c6b-4ad0-8e1f-aa086598b663',
                                      '3151'),
             create_test_data_element('5369',
                                      'jobterm',
                                      '2:e Styrman',
                                      '5bcde2bb-5854-4ced-bf3e-7fccef284357',
                                      '3151'),
             ]


@pytest.mark.unit
def test_taxonomy_to_concept():
    result = importers.new_taxonomy.json_converter.taxonomy_to_concept(test_data)
    assert {result[k] == v for k, v in json_dict.items()}
    assert len(result) == len(json_dict)
