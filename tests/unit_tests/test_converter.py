import pytest
import sys
import logging
from dateutil.parser import ParserError
from datetime import datetime
from importers.platsannons import converter

log = logging.getLogger(__name__)
now = datetime.now()


@pytest.mark.unit
@pytest.mark.parametrize("input_date, expected_result, expected_error",
                         [('180924-00:00', '2024-09-18T00:00:00', None),
                          ('20180924T', '2018-09-24T00:00:00', None),
                          ('mon sep 24', f'{now.year}-09-24T00:00:00', None),
                          ('sep 24', f'{now.year}-09-24T00:00:00', None),
                          ('24', f'{now.year}-{now.month}-24T00:00:00', None),
                          ('00:00:00', f'{now.year}-{now.month}-{now.day}T00:00:00', None),
                          ('20180101f', None, ParserError),
                          ('2099-13-32', None, ParserError),
                          ('18-09-24:01:01', None, ParserError),
                          (' ', None, ParserError),
                          (None, None, TypeError),
                          ([' '], None, TypeError),
                          ([], None, TypeError),
                          ({}, None, TypeError),
                          ('XYZ', None, ParserError)
                          ])
def test_date_conversion(input_date, expected_result, expected_error):
    print('==================', sys._getframe().f_code.co_name, '================== ')
    import dateutil
    try:
        converted_date = converter._date_parser(input_date)
    except dateutil.parser.ParserError as e:
        if expected_error == type(e):
            return
        else:
            raise e
    except TypeError as e:
        if expected_error == type(e):
            return
        else:
            raise e
    else:
        assert converted_date == expected_result


@pytest.mark.unit
@pytest.mark.parametrize("location, expected", [("Solna", "solna"),
                                                ("Upplands Väsby", "upplands väsby"),
                                                ("Upplands-Bro", "upplands-bro"),
                                                ("Malmö (huvudkontor)", "malmö"),
                                                ("12345 Danderyd", "danderyd"),
                                                ("34-5 Sundsvall", "sundsvall"),
                                                ("34-5 Sundsvall", "sundsvall"),
                                                ("Stockholm 8", "stockholm"),
                                                ("Gävle (avd.8)", "gävle"),
                                                ("Gävle (avd. 8)", "gävle"),
                                                ("Liljeholmen ()", "liljeholmen"),
                                                ("dk-5000 odense", "odense"),
                                                ("d02 r299 dublin", "dublin"),
                                                ("02d 299r dublin", "dublin"),
                                                ("campanillas, málaga", "málaga"),
                                                ("box 215, se-251 helsingborg",
                                                 "helsingborg"),
                                                ("bromma stockholm", "bromma stockholm"),
                                                ])
def test_trim_location(location, expected):
    print('==================', sys._getframe().f_code.co_name, '================== ')
    assert converter._trim_location(location) == expected


@pytest.mark.unit
@pytest.mark.parametrize("employer, expected", [(["Logopedbyrån Dynamica AB",
                                                  "Logopedbyrån Dynamica Stockholm AB"],
                                                 ["logopedbyrån dynamica"]),
                                                (["Logopedbyrån Dynamica Stockholm AB",
                                                  "AB Logopedbyrån Dynamica"],
                                                 ["logopedbyrån dynamica"]),
                                                (["AB Foo", "Foo"], ["foo"]),
                                                (["  AB Foo", "AB Foo"], ["foo"]),
                                                (["Foo Bar AB", "Foo Bar"], ["foo bar"]),
                                                ([" Foo Bar AB"], ["foo bar"]),
                                                ([" Foo Bar AB "], ["foo bar"]),
                                                (["Fazer AB", "Gateau"],
                                                 ["fazer", "gateau"]),
                                                (["Fazer Stockholm AB", "Gateau"],
                                                 ["gateau", "fazer stockholm"]),
                                                (["Fazer Stockholm AB", "Gateau", "Foo"],
                                                 ["foo", "gateau", "fazer stockholm"]),
                                                (["Fazer Stockholm AB", "Gateau",
                                                  "Gateau"],
                                                 ["gateau", "fazer stockholm"]),
                                                (None, [])
                                                ])
def test_create_employer_name(employer, expected):
    print('==================', sys._getframe().f_code.co_name, '================== ')
    assert converter._create_employer_name_keywords(employer) == expected


@pytest.mark.unit
@pytest.mark.parametrize("fake_ad, expected_location_count, expected_locations, \
                         expected_employer_count, expected_employers",
                         [
                             ({
                                  "workplace_address": {
                                      "city": "Stockholm",
                                      "municipality": "Stockholm",
                                      "region": "Stockholms län",
                                      "country": "Sverige"
                                  },
                                  "employer": {
                                      "name": "TestByrån Obfuscatica Stockholm AB",
                                      "workplace": "TestByrån Obfuscatica AB"
                                  }
                              }, 3, ['stockholm', 'stockholms län', 'sverige'],
                              1, ['testbyrån obfuscatica']),
                             ({
                                  "workplace_address": {
                                      "city": "",
                                      "municipality": "Stockholm",
                                      "region": "Stockholms län",
                                      "country": "Sverige"
                                  },
                                  "employer": {
                                      "name": "TestByrån Obfuscatica Stockholm AB",
                                      "workplace": "AB TestByrån Obfuscatica"
                                  }
                              }, 3, ['stockholm', 'stockholms län', 'sverige'],
                              1, ['testbyrån obfuscatica']),
                         ])
def test_extract_keywords(fake_ad, expected_location_count, expected_locations,
                          expected_employer_count, expected_employers):
    print('==================', sys._getframe().f_code.co_name, '================== ')
    enriched_ad = converter._add_keywords(fake_ad)

    for location in expected_locations:
        assert location in enriched_ad['keywords']['extracted']['location']
    assert len(enriched_ad['keywords']['extracted']['location']) == expected_location_count

    for employer in expected_employers:
        assert employer in enriched_ad['keywords']['extracted']['employer']
    assert len(enriched_ad['keywords']['extracted']['employer']) == expected_employer_count
