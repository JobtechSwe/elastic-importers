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
    from importers.platsannons import converter
    source_ads = get_source_ads_from_file()
    target_ads = get_target_ads_from_file()

    for source_ad in source_ads:
        annons_id = source_ad['annonsId']
        # annons_id = '23189097'
        assert_ad_properties(annons_id, source_ads, target_ads, converter)


def assert_ad_properties(annons_id, source_ads, target_ads, converter):
    print('Testing convert_message for id: %s' % annons_id)
    source_ad = get_source_ad(annons_id, source_ads)
    # pprint(source_ad)
    target_ad = get_target_ad(annons_id, target_ads)
    message_envelope = {}
    message_envelope['annons'] = source_ad
    message_envelope['version'] = 1
    message_envelope['annons']['timestamp'] = target_ad['timestamp']
    # pprint(message_envelope)
    conv_ad = converter.convert_message(message_envelope)
    # pprint(conv_ad)
    # pprint(target_ad)
    for key, val in target_ad.items():
        # print(key2)
        if key not in ['publiceringskanaler', 'status', 'timestamp', 'keywords_enriched_binary', 'keywords']:
            assert key in conv_ad.keys()
            assert val == conv_ad[key]

    #########################
    # Borttagna attribut
    #########################
    assert 'status' not in conv_ad
    assert 'publiceringskanaler' not in conv_ad



    key1 = 'avpublicerad'
    assert key1 in conv_ad
    assert conv_ad[key1] == source_ad[key1]
    key2 = 'sista_publiceringsdatum'
    assert key2 in conv_ad
    assert converter._isodate(source_ad['sistaPubliceringsdatum']) == conv_ad[key2]
    key3 = 'avpubliceringsdatum'
    assert key3 in conv_ad
    if key3 in source_ad:
        assert converter._isodate(source_ad[key3]) == conv_ad[key3]
    else:
        assert conv_ad[key3] is None
    # TODO: Attributet 'lonebeskrivning' finns i ny källa, vill vi ha med denna?


def get_source_ad(annons_id, ads):
    ads_with_id = [ad for ad in ads if str(ad['annonsId']) == str(annons_id)]
    return None if len(ads_with_id) == 0 else ads_with_id[0]

def get_target_ad(annons_id, ads):
    ads_with_id = [ad['_source'] for ad in ads if str(ad['_source']['id']) == str(annons_id)]
    return None if len(ads_with_id) == 0 else ads_with_id[0]


if __name__ == '__main__':
    pytest.main([os.path.realpath(__file__), '-svv', '-ra', '-m integration'])