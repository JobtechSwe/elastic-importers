import os

# Taxonomyservice settings
TAXONOMY_SERVICE_URL = os.getenv("TAXONOMY_SERVICE_URL",
                                 "http://api.arbetsformedlingen.se/taxonomi/v0/TaxonomiService.asmx")
LANGUAGE_CODE_SE = 502
COUNTRY_CODE_SE = 199

# Elasticsearch settings
ES_TAX_INDEX_BASE = os.getenv("ES_TAX_INDEX_BASE", "taxonomy-")
ES_TAX_INDEX_ALIAS = os.getenv("ES_TAX_INDEX_ALIAS", "taxonomy")
ES_TAX_ARCHIVE_ALIAS = os.getenv("ES_TAX_ARCHIVE_ALIAS", "taxonomy-archive")

TAXONOMY_INDEX_CONFIGURATION = {
    "mappings": {
        "document": {
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
                "id": {
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
                "parent_id": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "type": {
                    "type": "keyword"
                },
                "num_id": {
                    "type": "long"
                }
            }
        }
    },
    "settings": {
        "index": {
            "number_of_shards": "5",
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
