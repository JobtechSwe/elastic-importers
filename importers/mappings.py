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
            "workplace_address": {
                "properties": {
                    "municipality_concept_id": {
                        "type": "keyword",
                    },
                    "region_concept_id": {
                        "type": "keyword",
                    },
                    "country_concept_id": {
                        "type": "keyword",
                        "null_value": "i46j_HmG_v64"
                    }
                }
            },
        }
    }
}
