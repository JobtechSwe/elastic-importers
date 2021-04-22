GENERAL_VALUES = ['region', 'municipality', 'country', 'worktime-extent', 'language', 'driving-licence', 'wage-type',
                  'sun-education-field-1', 'sun-education-field-2', 'sun-education-field-3', 'sun-education-level-1',
                  'sun-education-level-2', 'sun-education-level-3', 'employment-duration']

REPLACED_VALUES = ['skill', 'employment-type']

# Elasticsearch settings

TAXONOMY_INDEX_CONFIGURATION = {
    "mappings": {
        "properties": {
            "description": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "concept_id": {
                "type": "keyword"
            },
            "label": {
                "type": "text",
                "fields": {
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "ngram",
                        "search_analyzer": "simple"
                    }
                }
            },
            "type": {
                "type": "keyword"
            },
            "legacy_ams_taxonomy_id": {
                "type": "keyword"
            }
        }
    },
    "settings": {
        "index": {
            "number_of_shards": "1",
            "number_of_replicas": "1"
        },
        "analysis": {
            "analyzer": {
                "ngram": {
                    "filter": [
                        "lowercase"
                    ],
                    "tokenizer": "ngram_tokenizer"
                }
            },
            "tokenizer": {
                "ngram_tokenizer": {
                    "token_chars": [
                        "letter",
                        "digit"
                    ],
                    "min_gram": "1",
                    "type": "edgeNGram",
                    "max_gram": "10"
                }
            }
        }
    }
}
