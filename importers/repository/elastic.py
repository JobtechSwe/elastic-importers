import logging
import certifi
import time
from ssl import create_default_context
from elasticsearch.helpers import bulk, scan
from elasticsearch import Elasticsearch
from importers import settings


log = logging.getLogger(__name__)

if settings.ES_USER and settings.ES_PWD:
    context = create_default_context(cafile=certifi.where())
    es = Elasticsearch([settings.ES_HOST], port=settings.ES_PORT,
                       use_ssl=True, scheme='https', ssl_context=context,
                       http_auth=(settings.ES_USER, settings.ES_PWD))
else:
    es = Elasticsearch([{'host': settings.ES_HOST, 'port': settings.ES_PORT}])


def _bulk_generator(documents, indexname, idkey):
    for document in documents:
        if "concept_id" in document:
            doc_id = document["concept_id"]
        else:
            doc_id = '-'.join([document[key]
                               for key in idkey]) \
                if isinstance(idkey, list) else document[idkey]

        yield {
            '_index': indexname,
            '_id': doc_id,
            '_source': document
        }


def bulk_index(documents, indexname, idkey='id'):
    bulk(es, _bulk_generator(documents, indexname, idkey), request_timeout=30)


def get_last_timestamp(indexname):
    response = es.search(index=indexname,
                         body={
                             "from": 0, "size": 1,
                             "sort": {"timestamp": "desc"},
                             "_source": "timestamp",
                             "query": {
                                 "match_all": {}
                             }
                         })
    hits = response['hits']['hits']
    return hits[0]['_source']['timestamp'] if hits else 0


def get_ids_with_timestamp(ts, indexname):
    # Possible failure if there are more than "size" documents with the same timestamp
    max_size = 1000
    response = es.search(index=indexname,
                         body={
                             "from": 0, "size": max_size,
                             "sort": {"timestamp": "desc"},
                             "_source": "id",
                             "query": {
                                 "term": {"timestamp": ts}
                             }
                         })
    hits = response['hits']['hits']
    return [hit['_source']['id'] for hit in hits]


def get_glitch_jobtechjobs_ids(max_items=None):
    '''
    Gets ids for ads that lacks "removedAt" but has
    a deadline in the past.
    '''
    query = {
        "query": {
            "bool": {
                "must_not": [
                    {
                        "exists": {
                            "field": "source.removedAt"
                        }
                    }
                ],
                "must": [
                    {
                        "exists": {
                            "field": "application.deadline"
                        }
                    },
                    {
                        "range": {
                            "application.deadline": {
                                "lt": "now/m"
                            }
                        }
                    }
                ]
            }
        }}
    doc_counter = 0

    ids = []
    for doc in scan(es, query=query, index=settings.ES_AURANEST_INDEX, size=1000):
        doc_counter += 1
        if max_items and doc_counter > max_items:
            break
        ids.append(doc['_source']['id'])
    return ids


def index_exists(indexname):
    es_available = False
    fail_count = 0
    while not es_available:
        try:
            result = es.indices.exists(index=[indexname])
            es_available = True
            return result
        except Exception as e:
            if fail_count > 1:
                # Elastic has its own failure management, so > 1 is enough.
                log.error("Elastic not available. Stop trying.")
                raise e
            log.warning("Elasticsearch currently not available. Waiting ...")
            log.debug("Connection failed: %s" % str(e))
            fail_count += 1
            time.sleep(1)


def alias_exists(aliasname):
    return es.indices.exists_alias(name=[aliasname])


def get_alias(aliasname):
    return es.indices.get_alias(name=[aliasname])


def put_alias(indexlist, aliasname):
    return es.indices.put_alias(index=indexlist, name=aliasname)


def setup_indices(es_index, default_index, mappings):
    write_alias = None
    read_alias = None
    if not es_index:
        es_index = default_index
        write_alias = "%s%s" % (es_index, settings.WRITE_INDEX_SUFFIX)
        read_alias = "%s%s" % (es_index, settings.READ_INDEX_SUFFIX)
    if not index_exists(es_index):
        log.info("Creating index %s" % es_index)
        create_index(es_index, mappings)
    if write_alias and not alias_exists(write_alias):
        log.info("Setting up alias %s for index %s" % (write_alias, es_index))
        put_alias([es_index], write_alias)
    if read_alias and not alias_exists(read_alias):
        log.info("Setting up alias %s for index %s" % (read_alias, es_index))
        put_alias([es_index], read_alias)

    return write_alias or es_index


def create_index(indexname, extra_mappings=None):
    basic_body = {
        "mappings": {
            "properties": {
                "timestamp": {
                    "type": "long"
                },
            }
        }
    }

    if extra_mappings:
        body = extra_mappings
        if 'mappings' in body:
            body.get('mappings', {}) \
                .get('properties', {})['timestamp'] = {'type': 'long'}
        else:
            body.update(basic_body)
    else:
        body = basic_body

    # Creates an index with mappings, ignoring if it already exists
    result = es.indices.create(index=indexname, body=body, ignore=400)
    if 'error' in result:
        log.error("Error on create index: %s" % result)


def add_indices_to_alias(indexlist, aliasname):
    response = es.indices.update_aliases(body={
        "actions": [
            {"add": {"indices": indexlist, "alias": aliasname}}
        ]
    })
    return response


def update_alias(indexname, old_indexlist, aliasname):
    actions = {
        "actions": [
        ]
    }
    for index in old_indexlist:
        actions["actions"].append({"remove": {"index": index,
                                              "alias": aliasname}})
        actions["actions"].append({"add": {"index": indexname, "alias": aliasname}})
    es.indices.update_aliases(body=actions)
