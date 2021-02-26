import os

VERSION = '1.4.0'

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

NEW_ADS_COEFFICIENT = 0.8  # don't switch alias to new index if number of ads in new index is too low

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
LA_LAST_TIMESTAMP_MANUAL = os.getenv('LA_LAST_TIMESTAMP_MANUAL', 'false').lower() == 'true'
LA_LAST_TIMESTAMP = int(os.getenv('LA_LAST_TIMESTAMP', 0))


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

REMOTE_MATCH_PHRASES = [y.lower() for y in
                        ["Arbeta på distans", "Arbete på distans", "Jobba på distans", "Arbeta hemifrån",
                         "Arbetar hemifrån", "Jobba hemifrån", "Jobb hemifrån", "remote work", "jobba tryggt hemifrån"]]
