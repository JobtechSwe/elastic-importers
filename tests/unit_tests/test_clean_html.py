import pytest
import sys
import json
import os
import logging
from importers.common import clean_html

log = logging.getLogger(__name__)

currentdir = os.path.dirname(os.path.realpath(__file__)) + '/'


@pytest.mark.unit
def test_clean_html_non_html():
    print('============================', sys._getframe().f_code.co_name, '============================ ')

    input = 'En mening utan html fast med radbrytning\nAndra raden kommer här'
    output = clean_html(input)
    assert (input == output)


@pytest.mark.unit
def test_clean_html_br():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    output = clean_html('<b>Rubrik</b>Rad 1<br />Rad 2')
    assert 'RubrikRad 1\nRad 2' == output


@pytest.mark.unit
def test_clean_html_p_tags():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    output = clean_html('<p>Paragraf 1</p><p>Paragraf 2</p>')
    assert 'Paragraf 1\n\nParagraf 2' == output


@pytest.mark.unit
def test_clean_html_nested_p_tags():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    output = clean_html('<p>Paragraf 1<p>Nästlad paragraf</p>Och fortsättning paragraf 1</p><p>Paragraf 2</p>')
    assert 'Paragraf 1\nNästlad paragraf\nOch fortsättning paragraf 1\nParagraf 2' == output


@pytest.mark.unit
def test_clean_html_ul_and_li_tags():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    output = clean_html(
        '<p><strong>DIN ROLL:</strong></p><ul><li>Helhetsansvar för implementationsprocessen.</li><li>Planläggning och projektledning.</li><li>Analys och migrering av kundens data.</li></ul>Efter lista')
    expected_output = 'DIN ROLL:\nHelhetsansvar för implementationsprocessen.\nPlanläggning och projektledning.\nAnalys och migrering av kundens data.\nEfter lista'
    assert expected_output == output


@pytest.mark.unit
def test_clean_html_headlines():
    print('============================', sys._getframe().f_code.co_name, '============================ ')

    for i in range(1, 7):
        input = '<h%s>Rubrik</h%s>Brödtext här' % (i, i)
        output = clean_html(input)
        assert ('Rubrik\nBrödtext här' == output)


@pytest.mark.unit
def test_clean_non_valid_html():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    assert 'Paragraf 1 utan sluttag.' == clean_html('<p>Paragraf 1 utan sluttag.')
    assert 'Paragraf 1 utan starttag.' == clean_html('Paragraf 1 utan starttag.</p>')
    assert 'brtag som inte är\nxhtml' == clean_html('brtag som inte är<br>xhtml')
    assert 'Helt fel rubrik' == clean_html('<h1>Helt fel rubrik</h2>')
    assert 'Eget attribut' == clean_html('<h1 hittepå="test">Eget attribut</h1>')


@pytest.mark.unit
def test_clean_script_tags():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    assert '' == clean_html('<script>alert("test");</script>')
    assert 'Lite text. Lite till. Och lite till.' == clean_html(
        '<script>alert("test");</script>Lite text. Lite till. <script>alert("test");</script>Och lite till.')


@pytest.mark.unit
def test_clean_html_from_description():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    source_ads = get_source_ads_from_file()
    annons_id = '23483261'
    source_ad = get_source_ad(annons_id, source_ads)
    ad_html_text = source_ad['annonstextFormaterad']
    cleaned_ad_text = clean_html(ad_html_text)
    assert '</p>' not in cleaned_ad_text
    assert '\n' in cleaned_ad_text


@pytest.mark.unit
def test_clean_html_from_description2():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    source_ads = get_source_ads_from_file()

    annons_id = '8428019'
    source_ad = get_source_ad(annons_id, source_ads)
    ad_html_text = source_ad['annonstextFormaterad']
    cleaned_ad_text = clean_html(ad_html_text)

    assert '</p>' not in cleaned_ad_text
    assert '\n' in cleaned_ad_text


@pytest.mark.unit
def test_clean_html_type_none_input():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    cleaned_ad_text = clean_html(None)
    assert cleaned_ad_text == ''


@pytest.mark.unit
def test_clean_html_p_tags():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    input = '<b>Dina arbetsuppgifter</b><p>Du kommer att undervisa inom ämnet trädgård.'
    expected_output = 'Dina arbetsuppgifter\nDu kommer att undervisa inom ämnet trädgård.'
    assert clean_html(input) == expected_output


@pytest.mark.unit
def test_clean_html_p_tags_no_previous_sibling():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    input = 'Dina arbetsuppgifter<p>Du kommer att undervisa inom ämnet trädgård.'
    expected_output = 'Dina arbetsuppgifter\nDu kommer att undervisa inom ämnet trädgård.'
    assert clean_html(input) == expected_output


@pytest.mark.unit
def test_clean_html_double_p_tags():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    input = '<p><b>Dina arbetsuppgifter</b></p><p>Du kommer att undervisa inom ämnet trädgård.'
    expected_output = 'Dina arbetsuppgifter\nDu kommer att undervisa inom ämnet trädgård.'
    assert clean_html(input) == expected_output


def get_source_ads_from_file():
    with open(currentdir + 'test_resources/platsannonser_source_test_import.json', encoding='utf-8') as f:
        result = json.load(f)
        return result['testannonser']


def get_source_ad(annons_id, ads):
    ads_with_id = [ad for ad in ads if str(ad['annonsId']) == str(annons_id)]
    ad = None if len(ads_with_id) == 0 else ads_with_id[0]
    ad['annonsId'] = str(ad['annonsId'])
    return ad


if __name__ == '__main__':
    pytest.main([os.path.realpath(__file__), '-svv', '-ra', '-m unit'])
