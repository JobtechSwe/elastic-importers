import os

ES_HOST = os.getenv("ES_HOST", "127.0.0.1")
ES_PORT = os.getenv("ES_PORT", 9200)
ES_USER = os.getenv("ES_USER")
ES_PWD = os.getenv("ES_PWD")

# For platsannonser
WRITE_INDEX_SUFFIX = '-write'
READ_INDEX_SUFFIX = '-read'
DELETED_INDEX_SUFFIX = '-deleted'
STREAM_INDEX_SUFFIX = '-stream'
ES_ANNONS_PREFIX = os.getenv('ES_ANNONS_INDEX', os.getenv('ES_ANNONS', 'platsannons'))
ES_ANNONS_INDEX = "%s%s" % (ES_ANNONS_PREFIX, WRITE_INDEX_SUFFIX)

# Parameter names corresponding to
# narvaltextdocenrichments.textdocenrichments.NarvalEnricher
ENRICHER_PARAM_DOC_ID = 'doc_id'
ENRICHER_PARAM_DOC_HEADLINE = 'doc_headline'
ENRICHER_PARAM_DOC_TEXT = 'doc_text'
ENRICHER_PROCESSES = int(os.getenv("ENRICHER_PROCESSES", 8))

# legacy name starts with PG
PG_BATCH_SIZE = int(os.getenv('PG_BATCH_SIZE', 2000))

LA_FEED_URL = os.getenv('LA_FEED_URL')
LA_BOOTSTRAP_FEED_URL = os.getenv('LA_BOOTSTRAP_FEED_URL')
LA_DETAILS_URL = os.getenv('LA_DETAILS_URL')
LA_DETAILS_PARALLELISM = int(os.getenv('LA_DETAILS_PARALLELISM', 8))
LA_ANNONS_MAX_TRY = int(os.getenv('LA_ANNONS_MAX_TRY', 10))
LA_ANNONS_TIMEOUT = int(os.getenv('LA_ANNONS_TIMEOUT', 10))
LA_LAST_TIMESTAMP_MANUAL = os.getenv('LA_LAST_TIMESTAMP_MANUAL', None)
LA_LAST_TIMESTAMP = int(os.getenv('LA_LAST_TIMESTAMP', 0))
# trigger to use ad format with v2 (concept_id)
LA_ANNONS_V2 = os.getenv('LA_ANNONS_V2', 'false').lower() == 'true'

# For berikning (platsannonser)
URL_ENRICH = 'https://textdoc-enrichments.dev.services.jtech.se/enrichtextdocuments'
URL_ENRICH_TEXTDOCS_SERVICE = os.getenv('URL_ENRICH_TEXTDOCS_SERVICE', URL_ENRICH)
API_KEY_ENRICH_TEXTDOCS = os.getenv("API_KEY_ENRICH_TEXTDOCS", '')
ENRICH_THRESHOLD_OCCUPATION = os.getenv('ENRICH_THRESHOLD_OCCUPATION', 0.8)
ENRICH_THRESHOLD_SKILL = os.getenv('ENRICH_THRESHOLD_SKILL', 0.5)
ENRICH_THRESHOLD_GEO = os.getenv('ENRICH_THRESHOLD_GEO', 0.7)
ENRICH_THRESHOLD_TRAIT = os.getenv('ENRICH_THRESHOLD_TRAIT', 0.5)

COMPANY_LOGO_BASE_URL = os.getenv('COMPANY_LOGO_BASE_URL',
                                  'https://www.arbetsformedlingen.se/rest/arbetsgivare/rest/af/v3/')
COMPANY_LOGO_TIMEOUT = int(os.getenv('COMPANY_LOGO_TIMEOUT', 10))

platsannons_deleted_mappings = {
    "mappings": {
        "properties": {
            "id": {
                "type": "keyword"
            },
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
            "application_details": {
                "properties": {
                    "reference": {
                        "type": "text"
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
