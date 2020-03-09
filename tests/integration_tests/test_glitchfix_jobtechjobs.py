import os
import sys
from pprint import pprint
import json

import pytest


@pytest.mark.skip(reason="Disabled")
@pytest.mark.integration
def test_glitchfix():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    from importers.auranest.main import glitchfix
    glitchfix()

@pytest.mark.skip(reason="Disabled")
@pytest.mark.integration
def test_get_glitch_jobtechjobs_ids():
    print('============================', sys._getframe().f_code.co_name, '============================ ')
    # Hämta annons-id:n från Elastic som saknar removedAt och där deadline passerat
    from importers.repository.elastic import get_glitch_jobtechjobs_ids
    gen_ids = get_glitch_jobtechjobs_ids(max_items=100)

    ids = [id for id in gen_ids]
    # pprint(ids)
    # for id in gen_ids:
    #     print(id)


@pytest.mark.skip(reason="Disabled")
@pytest.mark.integration
def test_glitchfix_read_ads_from_postgres():
    print('============================', sys._getframe().f_code.co_name, '============================ ')

    # from importers.repository.elastic import get_glitch_jobtechjobs_ids
    from importers import settings
    from importers.repository.postgresql import read_docs_with_ids
    # gen_ids = get_glitch_jobtechjobs_ids(max_items=100)
    # ids = [id for id in gen_ids]
    ids = ['3a7cb98a-4b7f-40ff-96e4-659e91716320', '3aeedba9-0db7-467e-8c4a-a18cde1394e5']
    # pprint(ids)
    documents = read_docs_with_ids(settings.PG_AURANEST_TABLE, ids)
    pprint(documents)


if __name__ == '__main__':
    pytest.main([os.path.realpath(__file__), '-svv', '-ra', '-m integration'])