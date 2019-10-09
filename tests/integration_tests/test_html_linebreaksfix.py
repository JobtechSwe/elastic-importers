import logging
import os
import sys
from ssl import create_default_context

import certifi
import pytest
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

log = logging.getLogger(__name__)

indexname = 'platsannons-read'

context = create_default_context(cafile=certifi.where())

def create_dev_client():
    es_dev = Elasticsearch([os.getenv('ES_HOST')], port=int(os.getenv('ES_PORT')),
                            use_ssl=True, scheme='https', ssl_context=context,
                            http_auth=(os.getenv('ES_USER'), os.getenv('ES_PWD')))
    return es_dev


@pytest.mark.skip(reason="Temporarily disabled. Needs pytest_secrets.env for env-variables.")
@pytest.mark.integration
def test_missing_description_text():
    print('============================', sys._getframe().f_code.co_name, '============================ ')

    es_dev = create_dev_client()

    query = {
        "query": {
            "bool": {
                "filter": [
                    {"range": {"publication_date": {"lte": "now/m"}}},
                    {"range": {"last_publication_date": {"gte": "now/m"}}},
                    {"term": {"removed": False}}
                ]
            }
        }
    }

    max_items = 50000
    doc_counter = 0
    dev_missing_description_ids = []

    for doc in scan(es_dev, query=query, index=indexname, size=100):
        doc_counter += 1
        if max_items and doc_counter > max_items:
            break
        dev_doc = doc['_source']
        if dev_doc['description']['text'] == '' or dev_doc['description']['text'] is None:
            dev_missing_description_ids.append(dev_doc['id'])
    # pprint(dev_missing_description_ids)

    assert len(dev_missing_description_ids) == 0



if __name__ == '__main__':
    pytest.main([os.path.realpath(__file__), '-svv', '-ra', '-m integration'])