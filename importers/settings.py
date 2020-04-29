import os

ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = os.getenv("ES_PORT", 9200)
ES_USER = os.getenv("ES_USER")
ES_PWD = os.getenv("ES_PWD")

# For platsannonser
WRITE_INDEX_SUFFIX = '-write'
READ_INDEX_SUFFIX = '-read'
DELETED_INDEX_SUFFIX = '-deleted'
STREAM_INDEX_SUFFIX = '-stream'
ES_ANNONS_PREFIX = os.getenv('ES_ANNONS_INDEX',
                             os.getenv('ES_ANNONS', 'platsannons'))
ES_ANNONS_INDEX = "%s%s" % (ES_ANNONS_PREFIX, WRITE_INDEX_SUFFIX)

# Parameter names corresponding to
# narvaltextdocenrichments.textdocenrichments.NarvalEnricher
ENRICHER_PARAM_DOC_ID = 'doc_id'
ENRICHER_PARAM_DOC_HEADLINE = 'doc_headline'
ENRICHER_PARAM_DOC_TEXT = 'doc_text'

ENRICHER_PROCESSES = int(os.getenv("ENRICHER_PROCESSES", 8))

# From loaders
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT', 5432)
PG_DBNAME = os.getenv('PG_DBNAME')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_BATCH_SIZE = os.getenv('PG_BATCH_SIZE', 2000)
PG_PLATSANNONS_TABLE = os.getenv('PG_PLATSANNONS_TABLE', 'platsannons_la')
PG_SSLMODE = os.getenv('PG_SSLMODE', 'require')


LOADER_START_DATE = os.getenv('LOADER_START_DATE', '2018-01-01')

LA_FEED_URL = os.getenv('LA_FEED_URL')
LA_BOOTSTRAP_FEED_URL = os.getenv('LA_BOOTSTRAP_FEED_URL')
LA_DETAILS_URL = os.getenv('LA_DETAILS_URL')
LA_DETAILS_PARALLELISM = os.getenv('LA_DETAILS_PARALLELISM', 8)
# End from loaders

platsannons_deleted_mappings = {
    "mappings": {
        "properties": {
            "publication_date": {
                "type": "date",
                "null_value": "1970-01-01"
            },
            "last_publication_date": {
                "type": "date",
                "null_value": "2100-12-31"
            },
        }
    }
}

platsannons_mappings = {
    "settings": {
        "analysis": {
            "analyzer": {
                "simple_word_splitter": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filter": ["lowercase"],
                    "char_filter": ["punctuation_filter"]
                },
                "wildcard_prefix": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filter": ["lowercase", "edgengram_filter"],
                    "char_filter": ["punctuation_filter"]
                },
                "wildcard_suffix": {
                    "type": "custom",
                    "tokenizer": "whitespace",
                    "filter": ["lowercase", "reverse", "edgengram_filter", "reverse"],
                    "char_filter": ["punctuation_filter"]
                },
                "bigram_combiner": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "custom_shingle",
                        "my_char_filter"
                    ]
                },
                "trigram": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "shingle", "swedish_stop", "swedish_keywords"]
                },
                "reverse": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "reverse", "swedish_stop", "swedish_keywords"]
                }
            },
            "filter": {
                "edgengram_filter": {
                    "type": "edge_ngram",
                    "min_gram": 3,
                    "max_gram": 30
                },
                "custom_shingle": {
                     "type": "shingle",
                     "min_shingle_size": 2,
                     "max_shingle_size": 3,
                     "output_unigrams": True
                },
                "my_char_filter": {
                     "type": "pattern_replace",
                     "pattern": " ",
                     "replacement": ""
                },
                "shingle": {
                    "type": "shingle",
                    "min_shingle_size": 2,
                    "max_shingle_size": 3
                },
                "swedish_stop": {
                    "type": "stop",
                    "stopwords": "_swedish_"
                },
                "swedish_keywords": {
                    "type": "keyword_marker",
                    "keywords": ["exempel"]
                }
            },
            "char_filter": {
                "punctuation_filter": {
                    "type": "mapping",
                    "mappings": [
                        ". => ",
                        ", => \\u0020",
                        "\\u00A0 => \\u0020",
                        ": => ",
                        "! => "
                    ]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "id": {
                "type": "keyword"
            },
            "external_id": {
                "type": "keyword"
            },
            "headline": {
                "type": "text",
                "fields": {
                    "words": {
                        "type": "text",
                        "analyzer": "simple_word_splitter"
                    },
                    "prefix": {
                        "type": "text",
                        "analyzer": "wildcard_prefix",
                        "search_analyzer": "simple_word_splitter"
                    },
                    "suffix": {
                        "type": "text",
                        "analyzer": "wildcard_suffix",
                        "search_analyzer": "simple_word_splitter"
                    },
                }
            },
            "description": {
                "type": "object",
                "properties": {
                    "text": {
                        "type": "text",
                        "analyzer": "simple_word_splitter",
                        "fields": {
                            "prefix": {
                                "type": "text",
                                "analyzer": "wildcard_prefix",
                                "search_analyzer": "simple_word_splitter"
                            },
                            "suffix": {
                                "type": "text",
                                "analyzer": "wildcard_suffix",
                                "search_analyzer": "simple_word_splitter"
                            },
                        }
                    }
                }
            },
            "keywords": {
                "type": "object",
                "properties": {
                    "enriched": {
                        "type": "object",
                        "properties": {
                            "occupation": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "suggest": {
                                        "type": "completion"
                                    }
                                }
                            },
                            "skill": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "suggest": {
                                        "type": "completion"
                                    }
                                }
                            },
                            "trait": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "suggest": {
                                        "type": "completion"
                                    }
                                }
                            },
                            "location": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "suggest": {
                                        "type": "completion"
                                    }
                                }
                            },
                            "compound": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "suggest": {
                                        "type": "completion"
                                    },
                                    "trigram": {
                                        "type": "text",
                                        "analyzer": "trigram"
                                    },
                                    "reverse": {
                                        "type": "text",
                                        "analyzer": "reverse"
                                    },
                                }
                            }
                        }
                    },
                    "enriched_typeahead_terms": {
                        "type": "object",
                        "properties": {
                            "occupation": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "suggest": {
                                        "type": "completion"
                                    }
                                }
                            },
                            "skill": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "suggest": {
                                        "type": "completion"
                                    }
                                }
                            },
                            "trait": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "suggest": {
                                        "type": "completion"
                                    }
                                }
                            },
                            "location": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "suggest": {
                                        "type": "completion"
                                    }
                                }
                            },
                            "compound": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "suggest": {
                                        "type": "completion"
                                    },
                                    "trigram": {
                                        "type": "text",
                                        "analyzer": "trigram"
                                    },
                                    "reverse": {
                                        "type": "text",
                                        "analyzer": "reverse"
                                    },
                                }
                            },
                        }
                    },
                    "extracted": {
                        "type": "object",
                        "properties": {
                            "occupation": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "skill": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "location": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "employer": {
                                "type": "text",
                                "fields": {
                                    "raw": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    },
                                    "suggest": {
                                        "type": "completion"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "publication_date": {
                "type": "date"
            },
            "last_publication_date": {
                "type": "date"
            },
            "application_deadline": {
                "type": "date"
            },
            "access": {
                "type": "text"
            },
            "workplace_address": {
                "properties": {
                    "municipality_code": {
                        "type": "keyword",
                    },
                    "municipality_concept_id": {
                        "type": "keyword",
                    },
                    "region_code": {
                        "type": "keyword",
                    },
                    "region_concept_id": {
                        "type": "keyword",
                    },
                    "country_code": {
                        "type": "keyword",
                        "null_value": "199"  # Assume Sweden when not specified
                    },
                    "country_concept_id": {
                        "type": "keyword",
                        "null_value": "i46j_HmG_v64"  # Assume Sweden when not specified
                    },
                    "coordinates": {
                        "type": "geo_point",
                        "ignore_malformed": True
                    }
                }
            },
            "scope_of_work": {
                "properties": {
                    "min": {
                        "type": "float"
                    },
                    "max": {
                        "type": "float"
                    }
                }
            }
        }
    }
}


# For berikning (platsannonser)
URL_ENRICH_TEXTDOCS_SERVICE = \
    os.getenv('URL_ENRICH_TEXTDOCS_SERVICE',
              'https://textdoc-enrichments.dev.services.jtech.se'
              '/enrichtextdocuments')
API_KEY_ENRICH_TEXTDOCS = os.getenv("API_KEY_ENRICH_TEXTDOCS", '')
#    os.getenv('URL_ENRICH_TEXTDOCS_BINARY_SERVICE',
#              'http://localhost:6357/enrichtextdocumentsbinary')
ENRICH_THRESHOLD_OCCUPATION = os.getenv('ENRICH_THRESHOLD_OCCUPATION', 0.8)
ENRICH_THRESHOLD_SKILL = os.getenv('ENRICH_THRESHOLD_SKILL', 0.5)
ENRICH_THRESHOLD_GEO = os.getenv('ENRICH_THRESHOLD_GEO', 0.7)
ENRICH_THRESHOLD_TRAIT = os.getenv('ENRICH_THRESHOLD_TRAIT', 0.5)

COMPANY_LOGO_BASE_URL = os.getenv('COMPANY_LOGO_BASE_URL',
                                  'https://www.arbetsformedlingen.se/rest/arbetsgivare/rest/af/v3/')
