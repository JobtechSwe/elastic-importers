import importers.new_taxonomy.json_converter as json_converter
import pytest


test_data_for_map_functions = [{'legacy_ams_taxonomy_id':'5370',
              'type': 'jobterm',
              'label':'1:e Fartygsingenjör/1:e Maskinist',
              'concept_id': '5bcde2bb-ff9b-4a8b-9a09-cf3c67b9a05e',
              'legacy_ams_taxonomy_num_id':'5370'},
             {'legacy_ams_taxonomy_id': '5372',
              'type': 'jobterm',
              'label': '2:e Fartygsingenjör/2:e Maskinist',
              'concept_id': '5bcde2bb-9c6b-4ad0-8e1f-aa086598b663',
              'legacy_ams_taxonomy_num_id': '5372'},
             {'legacy_ams_taxonomy_id':'5369',
              'type': 'jobterm',
              'label':'2:e Styrman',
              'concept_id': '5bcde2bb-5854-4ced-bf3e-7fccef284357',
              'legacy_ams_taxonomy_num_id':'5369'},
             ]


@pytest.mark.unit
def test_taxonomy_to_concept():
    expected_dict = {'jobterm':
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
    result = json_converter.map_taxonomy_to_concept(test_data_for_map_functions)
    assert {result[k] == v for k, v in expected_dict.items()}
    assert len(result) == len(expected_dict)

def test_concept_to_taxonomy():
    expected_dict = {'5bcde2bb-ff9b-4a8b-9a09-cf3c67b9a05e':
                         {'label': '1:e Fartygsingenjör/1:e Maskinist',
                          'legacyAmsTaxonomyId': '5370',
                          'type': 'jobterm'},
                     '5bcde2bb-9c6b-4ad0-8e1f-aa086598b663':
                         {'label': '2:e Fartygsingenjör/2:e Maskinist',
                          'legacyAmsTaxonomyId': '5372',
                          'type': 'jobterm'},
                     '5bcde2bb-5854-4ced-bf3e-7fccef284357':
                         {'label': '2:e Styrman',
                          'legacyAmsTaxonomyId': '5369',
                          'type': 'jobterm'},
                     }
    result = json_converter.map_concept_to_taxonomy(test_data_for_map_functions)
    assert {result[k] == v for k, v in expected_dict.items()}
    assert len(result) == len(expected_dict)
