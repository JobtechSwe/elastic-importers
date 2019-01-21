import logging
import certifi
import time
from ssl import create_default_context
from elasticsearch.helpers import bulk, scan
from elasticsearch import Elasticsearch, ElasticsearchException
from importers import settings


log = logging.getLogger(__name__)

if settings.ES_USER and settings.ES_PWD:
    context = create_default_context(cafile=certifi.where())
    es = Elasticsearch([settings.ES_HOST], port=settings.ES_PORT,
                       use_ssl=True, scheme='https', ssl_context=context,
                       http_auth=(settings.ES_USER, settings.ES_PWD))
else:
    es = Elasticsearch([{'host': settings.ES_HOST, 'port': settings.ES_PORT}])


def _bulk_generator(documents, indexname, idkey, doctype='document'):
    for document in documents:
        if "concept_id" in document:
            doc_id = document["concept_id"]
        else:
            doc_id = '-'.join([document[key]
                               for key in idkey]) if isinstance(idkey, list) else document[idkey]

        yield {
            '_index': indexname,
            '_type': doctype,
            '_id': doc_id,
            '_source': document
        }


def load_terms(termtype):
    dsl = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type.keyword": termtype.upper()}}
                ]
            }
        }
    }
    results = scan(es, query=dsl, index=settings.ES_ONTOLOGY_INDEX, doc_type='default')
    terms = [result['_source'] for result in results]
    return terms


def bulk_index(documents, indexname, idkey='id'):
    try:
        bulk(es, _bulk_generator(documents, indexname, idkey))
    except ElasticsearchException as e:
        log.error("Indexing failed: %s" % str(e))


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


def index_exists(indexname):
    es_available = False
    while not es_available:
        try:
            result = es.indices.exists(index=[indexname])
            es_available = True
            return result
        except Exception as e:
            log.warning("Elasticsearch currently not available. Waiting ...")
            log.debug("Connection failed: %s" % str(e))
            time.sleep(1)


def alias_exists(aliasname):
    return es.indices.exists_alias(name=[aliasname])


def get_alias(aliasname):
    return es.indices.get_alias(name=[aliasname])


def put_alias(indexlist, aliasname):
    return es.indices.put_alias(index=indexlist, name=aliasname)


def setup_indices(args, default_index, mappings):
    es_alias = None
    if len(args) > 1:
        es_index = args[1]
    else:
        es_index = default_index
        es_alias = "%s%s" % (es_index, settings.WRITE_INDEX_SUFFIX)
    if not index_exists(es_index):
        log.info("Creating index %s" % es_index)
        create_index(es_index, mappings)
    if es_alias and not alias_exists(es_alias):
        log.info("Setting up alias %s for index %s" % (es_alias, es_index))
        put_alias([es_index], es_alias)

        return es_alias

    return es_index


def create_index(indexname, extra_mappings=None):
    basic_body = {
        "mappings": {
            "document": {
                "properties": {
                    "timestamp": {
                        "type": "long"
                    },
                }
            }
        }
    }

    if extra_mappings:
        body = extra_mappings
        if 'mappings' in body:
            body.get('mappings', {}) \
                .get('document', {}).get('properties', {})['timestamp'] = {'type': 'long'}
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
