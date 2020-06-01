import logging
import certifi
import time
from ssl import create_default_context
from elasticsearch.helpers import bulk, scan, BulkIndexError
from elasticsearch import Elasticsearch
from importers import settings

log = logging.getLogger(__name__)

if settings.ES_USER and settings.ES_PWD:
    context = create_default_context(cafile=certifi.where())
    es = Elasticsearch([settings.ES_HOST], port=settings.ES_PORT,
                       use_ssl=True, scheme='https', ssl_context=context,
                       http_auth=(settings.ES_USER, settings.ES_PWD), timeout=60)
else:
    es = Elasticsearch([{'host': settings.ES_HOST, 'port': settings.ES_PORT}], timeout=60)


def _bulk_generator(documents, indexname, idkey, deleted_index):

    log.debug("(_bulk_generator) index: %s, idkey: %s, deleted_index: %s" % (indexname, idkey, deleted_index))
    for document in documents:
        if "concept_id" in document:
            doc_id = document["concept_id"]
        else:
            doc_id = '-'.join([document[key] for key in idkey]) \
                if isinstance(idkey, list) else document[idkey]

        if document.get('removed', False):
            remove_statement = {
                '_op_type': 'delete',
                '_index': indexname,
                '_id': doc_id,
                '_source': False
            }
            if deleted_index:
                tombstone = {
                    'id': doc_id,
                    'removed': True,
                    'removed_date': document.get('removed_date'),
                    'timestamp': document.get('timestamp'),
                    'occupation': document.get('occupation'),
                    'occupation_field': document.get('occupation_field'),
                    'occupation_group': document.get('occupation_group'),
                    'workplace_address': document.get('workplace_address'),
                    'publication_date': None,
                    'last_publication_date': None,
                }
                #yield remove_statement
                yield {
                    '_index': indexname,
                    '_id': doc_id,
                    '_source': tombstone,
                }
                yield {
                    '_index': deleted_index,
                    '_id': doc_id,
                    '_source': tombstone,
                }
            else:
                # yield remove_statement
                yield {
                    '_index': indexname,
                    '_id': doc_id,
                    '_source': tombstone,
                }
        else:
            yield {
                '_index': indexname,
                '_id': doc_id,
                '_source': document
            }


def bulk_index(documents, indexname, deleted_index=None, idkey='id'):
    action_iterator = _bulk_generator(documents, indexname, idkey, deleted_index)
    log.info("(bulk_index) action iterator: %s" % action_iterator)
    result = bulk(es, action_iterator, request_timeout=30, raise_on_error=True, yield_ok=False)
    return result[0]


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


def find_missing_ad_ids(ad_ids, es_index):
    # Check if the index has the ad ids. If refresh fails, still scan and log but return 0.
    try:
        es.indices.refresh(es_index)
        refresh_success = True
    except Exception as e:
        refresh_success = False
        log.warning("Refresh operation failed when trying to find missing ads: %s" % str(e))
    missing_ads_dsl = {
        "query": {
            "ids": {
                "values": ad_ids
            }
        }
    }
    ads = scan(es, query=missing_ads_dsl, index=es_index)
    indexed_ids = []
    for ad in ads:
        indexed_ids.append(ad['_id'])
    missing_ad_ids = set(ad_ids)-set(indexed_ids)
    if not refresh_success:
        log.warning(f"Found: {len(missing_ad_ids)} missing ads from index: {es_index}")
        return 0
    else:
        return missing_ad_ids


def document_count(es_index):
    # Returns the number of documents in the index or None if operation fails.
    try:
        es.indices.refresh(es_index)
        num_doc_elastic = es.cat.count(es_index, params={"format": "json"})[0]['count']
    except Exception as e:
        log.warning("Operation failed when trying to count ads: %s" % str(e))
        num_doc_elastic = None
    return num_doc_elastic


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
                log.error("Elastic not available after try: %d .Stop trying. %s"
                          % (fail_count, str(e)))
                raise e
            log.warning("Elasticsearch currently not available. Waiting ...")
            log.warning("Connection failed: %s" % str(e))
            fail_count += 1
            time.sleep(1)


def alias_exists(aliasname):
    return es.indices.exists_alias(name=[aliasname])


def get_alias(aliasname):
    return es.indices.get_alias(name=[aliasname])


def put_alias(indexlist, aliasname):
    return es.indices.put_alias(index=indexlist, name=aliasname)


def setup_indices(es_index, default_index, mappings, mappings_deleted=None):
    write_alias = None
    read_alias = None
    stream_alias = None
    deleted_index = "%s%s" % (settings.ES_ANNONS_PREFIX,
                              settings.DELETED_INDEX_SUFFIX)
    if not es_index:
        es_index = default_index
        write_alias = "%s%s" % (es_index, settings.WRITE_INDEX_SUFFIX)
        read_alias = "%s%s" % (es_index, settings.READ_INDEX_SUFFIX)
        stream_alias = "%s%s" % (es_index, settings.STREAM_INDEX_SUFFIX)
    if not index_exists(deleted_index):
        log.info("Creating index %s" % deleted_index)
        create_index(deleted_index, mappings_deleted)
    if not index_exists(es_index):
        log.info("Creating index %s" % es_index)
        create_index(es_index, mappings)
    if write_alias and not alias_exists(write_alias):
        log.info("Setting up alias %s for index %s" % (write_alias, es_index))
        put_alias([es_index], write_alias)
    if read_alias and not alias_exists(read_alias):
        log.info("Setting up alias %s for index %s" % (read_alias, es_index))
        put_alias([es_index], read_alias)
    if stream_alias and not alias_exists(stream_alias):
        log.info("Setting up alias %s for indices %s" % (
                 stream_alias, (es_index, deleted_index)))
        put_alias([es_index, deleted_index], stream_alias)

    current_index = write_alias or es_index
    return current_index, deleted_index


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

    # Creates an index with mappings, ignoring if it already exists.
    # TODO error already exist is not ignored on ignore=400
    result = es.indices.create(index=indexname, body=body, ignore=400)
    if 'error' in result:
        log.error("Error on create index %s: %s" % (indexname, result))
    else:
        log.info("New index created without errors: %s" % indexname)


def add_indices_to_alias(indexlist, aliasname):
    response = es.indices.update_aliases(body={
        "actions": [
            {"add": {"indices": indexlist, "alias": aliasname}}
        ]
    })
    log.info("add_indices_to_alias. Index names: %s. Alias name: %s" % (indexlist, aliasname))
    return response


def update_alias(indexnames, old_indexlist, aliasname):
    actions = {
        "actions": [
        ]
    }
    for index in old_indexlist:
        actions["actions"].append({"remove": {"index": index,
                                              "alias": aliasname}})

    actions["actions"].append(
        {"add": {"indices": indexnames, "alias": aliasname}})
    log.info("update_alias. Index names: %s. Old removed indices: %s. Alias name: %s" % (indexnames, old_indexlist, aliasname))
    log.debug("update_alias. Actions: %s" % actions)
    es.indices.update_aliases(body=actions)

