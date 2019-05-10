import pytest
import sys
import logging
from importers.platsannons import converter

log = logging.getLogger(__name__)


@pytest.mark.unit
@pytest.mark.parametrize("parsing_date", ['180924-00:00', '20180924T',
                                          'mon sep 24', '00:00:00'])
@pytest.mark.parametrize("not_parsing_date", ['20180101f', '2099-13-32',
                                              '18-09-24:01:01', '', None, []])
def test_isodate(parsing_date, not_parsing_date):
    print('==================', sys._getframe().f_code.co_name, '================== ')
    if not not_parsing_date:
        assert converter._isodate(not_parsing_date) is None
        return
    assert converter._isodate(not_parsing_date) is None
    assert converter._isodate(parsing_date) is not None


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
