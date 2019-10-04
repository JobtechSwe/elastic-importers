import os
import sys
from pprint import pprint
import json

import pytest


# Sammanfattning av ändringar, aktivitet https://trello.com/c/Jim7sieL
# Exempel på ny källa: https://www.arbetsformedlingen.se/rest/ledigtarbete/rest/af/v1/ledigtarbete/publikt/annonser/23189102
# =====================================================================
# Borttagna properties:
# 'status' +  'publicerad', 'skapad', 'uppdaterad', 'uppdaterad_av', 'skapad_av', 'anvandarId'
# 'publiceringskanaler' + 'platsbanken', 'ais', 'platsjournalen'

# Flyttade properties:
# 'sista_publiceringsdatum' flyttad från status/sista_publiceringsdatum till 'sista_publiceringsdatum' (högsta nivån)

# Nya properties:
# 'avpublicerad' (boolean)
# 'avpubliceringsdatum' - finns endast om avpublicerad är True
# 'external_id'

currentdir = os.path.dirname(os.path.realpath(__file__)) + '/'


def get_source_ads_from_file():
    with open(currentdir + 'test_resources/platsannonser_source_test_import.json') as f:
        result = json.load(f)
        # pprint(result)
        return result['testannonser']

def get_target_ads_from_file():
    with open(currentdir + 'test_resources/platsannonser_target_test_import.json') as f:
        result = json.load(f)
        # pprint(result)
        return result['hits']['hits']

# @pytest.mark.skip(reason="Temporarily disabled")
@pytest.mark.integration
def test_platsannons_conversion():
    print('============================', sys._getframe().f_code.co_name, '============================ ')

    source_ads = get_source_ads_from_file()
    target_ads = get_target_ads_from_file()


    annons_id = '22884504'
    source_ad = get_source_ad(annons_id, source_ads)

    annons_id = source_ad['annonsId']
    assert_ad_properties(annons_id, source_ads, target_ads)



def assert_ad_properties(annons_id, source_ads, target_ads):
    from importers.platsannons import converter
    print('Testing convert_message for id: %s' % annons_id)
    source_ad = get_source_ad(annons_id, source_ads)
    # pprint(source_ad)
    target_ad = get_target_ad(annons_id, target_ads)
    # pprint(target_ad)
    message_envelope = {}
    # message_envelope['annons'] = source_ad
    message_envelope = source_ad


    message_envelope['version'] = 1
    message_envelope['timestamp'] = target_ad['timestamp']
    # pprint(message_envelope)
    conv_ad = converter.convert_ad(message_envelope)
    # pprint(conv_ad)
    # pprint(target_ad)
    for key, val in target_ad.items():
        # print(key)
        # if key not in ['publiceringskanaler', 'status', 'timestamp', 'keywords_enriched_binary', 'keywords']:
        # if key not in ['avpublicerad']:
        if key not in ['keywords', 'timestamp', 'employer']:
            # print(key)
            assert key in conv_ad.keys()
            assert type(val) == type(conv_ad[key])
            assert val == conv_ad[key]
        elif key == 'keywords':
            # print('keywords', conv_ad['keywords'])
            company_node = conv_ad['keywords']['extracted']['employer']
            # print('company_node', company_node)
            assert 'fazer food services' in company_node
            assert 'gateau' in company_node
            assert 'fazer food services ab' not in company_node
            assert 'gateau ab' not in company_node
        elif key == 'employer':
            employer_node = conv_ad['employer']

            employer_node_keys = employer_node.keys()
            assert 'phone_number' in employer_node_keys
            assert 'email' in employer_node_keys
            assert 'url' in employer_node_keys
            assert 'organization_number' in employer_node_keys
            assert 'name' in employer_node_keys
            assert 'workplace' in employer_node_keys
            assert 'workplace_id' in employer_node_keys

    #########################
    # Borttagna attribut
    #########################
    assert 'status' not in conv_ad
    assert 'publiceringskanaler' not in conv_ad

    #########################
    # Nya/strukturellt ändrade attribut
    #########################
    assert 'removed' in conv_ad
    assert conv_ad['removed'] == source_ad['avpublicerad']
    assert 'last_publication_date' in conv_ad
    assert converter._isodate(source_ad['sistaPubliceringsdatum']) == conv_ad['last_publication_date']

    removed_date_key = 'removed_date'
    assert removed_date_key in conv_ad
    if 'avpubliceringsdatum' in source_ad:
        assert converter._isodate(source_ad['avpubliceringsdatum']) == conv_ad[removed_date_key]
    else:
        assert conv_ad[removed_date_key] is None


# @pytest.mark.skip(reason="Temporarily disabled")
@pytest.mark.integration
def test_corrupt_platsannons():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    from importers.platsannons import converter
    source_ads = get_source_ads_from_file()

    annons_id = '20159374'
    source_ad = get_source_ad(annons_id, source_ads)
    # target_ad = get_target_ad(annons_id, target_ads)
    message_envelope = source_ad


    message_envelope['version'] = 1
    message_envelope['timestamp'] = 1539958052330
    # pprint(message_envelope)
    conv_ad = converter.convert_ad(message_envelope)
    # pprint(conv_ad)

    assert len(conv_ad['must_have']['languages']) == 0
    assert len(conv_ad['must_have']['skills']) == 0
    assert len(conv_ad['must_have']['work_experiences']) == 0

    assert len(conv_ad['nice_to_have']['languages']) == 2
    assert len(conv_ad['nice_to_have']['skills']) == 2
    assert len(conv_ad['nice_to_have']['work_experiences']) == 2




def get_source_ad(annons_id, ads):
    ads_with_id = [ad for ad in ads if str(ad['annonsId']) == str(annons_id)]
    ad = None if len(ads_with_id) == 0 else ads_with_id[0]
    ad['annonsId'] = str(ad['annonsId'])
    return ad

def get_target_ad(annons_id, ads):
    ads_with_id = [ad['_source'] for ad in ads if str(ad['_source']['id']) == str(annons_id)]
    return None if len(ads_with_id) == 0 else ads_with_id[0]


if __name__ == '__main__':
    pytest.main([os.path.realpath(__file__), '-svv', '-ra', '-m integration'])