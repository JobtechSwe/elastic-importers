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
ENRICHER_PARAM_INC_SYNONYMS = 'include_synonyms'

ENRICHER_PROCESSES = int(os.getenv("ENRICHER_PROCESSES", 8))

# From loaders
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT', 5432)
PG_DBNAME = os.getenv('PG_DBNAME')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_BATCH_SIZE = os.getenv('PG_BATCH_SIZE', 2000)
PG_AURANEST_TABLE = os.getenv('PG_AURANEST_TABLE', 'auranest')
PG_PLATSANNONS_TABLE = os.getenv('PG_PLATSANNONS_TABLE', 'platsannons_la')
PG_SSLMODE = os.getenv('PG_SSLMODE', 'require')

AURANEST_FEED_URL = os.getenv('AURANEST_FEED_URL')
AURANEST_DETAILS_URL = os.getenv('AURANEST_DETAILS_URL')
AURANEST_USER = os.getenv('AURANEST_USER')
AURANEST_PASSWORD = os.getenv('AURANEST_PASSWORD')
AURANEST_EXPIRE_PATH = 'source.removedAt'

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
                }
            },
            "char_filter": {
                "punctuation_filter": {
                    "type": "mapping",
                    "mappings": [
                        ". => ",
                        ", => \\u0020",
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
                    }
                }
            },
            "description": {
                "type": "object",
                "properties": {
                    "text": {
                        "type": "text",
                        "analyzer": "simple_word_splitter"
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
                            "trait": {
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
                            }
                        }
                    },
                    "enriched_synonyms": {
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
                            "trait": {
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
                            }
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
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
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

# For postgres (platsannonser and auranest)
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", 5432)
PG_DBNAME = os.getenv("PG_DBNAME")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_BATCH_SIZE = os.getenv("PG_BATCH_SIZE", 1000)
PG_PLATSANNONS_TABLE = os.getenv("PG_PLATSANNONS_TABLE", "platsannons_la")
PG_AURANEST_TABLE = os.getenv("PG_AURANEST_TABLE", "auranest")
PG_SSLMODE = os.getenv("PG_SSLMODE", 'require')

# For berikning (platsannonser och auranest)
URL_ENRICH_TEXTDOCS_BINARY_SERVICE = \
    os.getenv('URL_ENRICH_TEXTDOCS_BINARY_SERVICE',
              'https://textdoc-enrichments.dev.services.jtech.se'
              '/enrichtextdocumentsbinary')
API_KEY_ENRICH_TEXTDOCS = os.getenv("API_KEY_ENRICH_TEXTDOCS", '')
#    os.getenv('URL_ENRICH_TEXTDOCS_BINARY_SERVICE',
#              'http://localhost:6357/enrichtextdocumentsbinary')

# For kandidat import
ES_KANDIDAT_INDEX = os.getenv('ES_KANDIDAT_INDEX',
                              os.getenv('ES_KANDIDAT', 'kandidater'))
ORACLE_USER = os.getenv('ORACLE_USER')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')
ORACLE_PORT = os.getenv('ORACLE_PORT', '1521')
ORACLE_HOST = os.getenv('ORACLE_HOST')
ORACLE_SERVICE = os.getenv('ORACLE_SERVICE')

# For auranest import
ES_AURANEST_PREFIX = os.getenv('ES_AURANEST_INDEX',
                               os.getenv('ES_AURANEST', 'auranest'))
ES_AURANEST_INDEX = "%s%s" % (ES_AURANEST_PREFIX, WRITE_INDEX_SUFFIX)

auranest_mappings = {
    "settings": {
        "analysis": {
            "normalizer": {
                "lc_normalizer": {
                    "type": "custom",
                    "filter": ["lowercase"]
                }
            }
        }
    },
    "mappings": {
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "group": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        }
                    }
                },
                "occupations": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    },
                    "copy_to": "keywords"
                },
                "skills": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    },
                    "copy_to": "keywords"
                },
                "traits": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    },
                    "copy_to": "keywords"
                },
                "location": {
                    "type": "object",
                    "properties": {
                        "translations": {
                            "type": "object",
                            "properties": {
                                "sv-SE": {
                                    "type": "text",
                                    "copy_to": "keywords",
                                    "fields": {
                                        "keyword": {
                                            "type": "keyword",
                                            "ignore_above": 256
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "keywords": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lc_normalizer"
                        }
                    }
                },
            }
    }
}
