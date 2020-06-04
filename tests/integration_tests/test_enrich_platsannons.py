import os
import sys
import pytest
from importers.platsannons.loader import load_details_from_la
from importers.platsannons import converter, enricher_mt_rest_multiple as enricher


@pytest.mark.skip(reason="Test works only with specific ad id.")
@pytest.mark.integration
def test_get_and_enrich_ad_details():
    print('============================', sys._getframe().f_code.co_name, '============================ ')


    annons_id = 23888740
    ad_meta = {'annonsId': annons_id, 'avpublicerad': False, 'uppdateradTid': 1582272745821}

    ad_detail = load_details_from_la(ad_meta=ad_meta)
    ad_details = {}
    ad_details[annons_id] = ad_detail
    converted_ads = [converter.convert_ad(ad) for ad in ad_details.values()]
    enriched_ads = enricher.enrich(converted_ads)

    assert len(enriched_ads) > 0

    enriched_ad = enriched_ads[0]
    geos = enriched_ad['keywords']['enriched']['location']
    occupations = enriched_ad['keywords']['enriched']['occupation']

    assert 'mattläggare' in occupations
    assert 'golvläggare' in occupations
    assert 'luleå' in geos
    assert 'enriched_typeahead_terms' in enriched_ad['keywords']
    assert 'occupation' in enriched_ad['keywords']['enriched_typeahead_terms']
    occupation_synonyms = enriched_ad['keywords']['enriched_typeahead_terms']['occupation']
    assert len(occupation_synonyms) > 0


if __name__ == '__main__':
    pytest.main([os.path.realpath(__file__), '-svv', '-ra', '-m integration'])
