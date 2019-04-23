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
                                                ("Malmö (huvudkontor)", "malmö"),
                                                ("12345 Danderyd", "danderyd"),
                                                ("34-5 Sundsvall", "sundsvall"),
                                                ("Stockholm 8", "stockholm"),
                                                ("Gävle (avd. 8)", "gävle"),
                                                ("Liljeholmen ()", "liljeholmen"),
                                                ])
def test_trim_location(location, expected):
    print('==================', sys._getframe().f_code.co_name, '================== ')
    assert converter._trim_location(location) == expected
