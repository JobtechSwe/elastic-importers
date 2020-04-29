import os
import sys
from pprint import pprint
import json

import pytest




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





if __name__ == '__main__':
    pytest.main([os.path.realpath(__file__), '-svv', '-ra', '-m integration'])