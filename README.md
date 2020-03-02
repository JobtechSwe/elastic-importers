# elastic-importer
Imports job ads and taxonomy values into elasticsearch. 
Creates console script entry points to ne run with different intervals, usually via cron to load data into elastic.

## Installation

    $ python setup.py 

### For development 

    $ python setup.py develop
    
This creates symlinked versions of the scripts to enable development without having to run setup for every change.

## Configuration
The application is entirely configured using environment variables. 

|Environment variable   | Default value  | Comment |Used by system|
|---|---|---|---|
| ES_HOST  | localhost  | Elasticsearch host | all |
| ES_PORT  | 9200  | Elasticsearch port | all |
| ES_USER  |   | Elasticsearch username | all |
| ES_PWD  |   | Elasticsearch password | all |
| ES_TAX_INDEX_BASE  | taxonomy-  | Base string from which index for different taxonomyversions will be created |import-taxonomy|
| ES_TAX_INDEX_ALIAS  |  taxonomy | Alias for index that is the current version of the taxonomy |import-taxonomy|
| ES_TAX_ARCHIVE_ALIAS  |  taxonomy-archive | Alias collecting all older versions of the taxonomy |import-taxonomy|
| ES_ANNONS_INDEX | platsannons | Base index name for job ads |import-platsannonser, import-platsannonser-daily|
| LA_FEED_URL | | REST feed API for changes in job ads | import-platsannonser, import-platsannonser-daily |
| LA_BOOTSTRAP_FEED_URL | | REST feed API for all currently available job ads | import-platsannonser-daily |
| LA_DETAILS_URL | | REST feed API job ad details (i.e. the entire job ad) | import-platsannonser, import-platsannonser-daily |
| LA_DETAILS_PARALLELISM | 8 | Limits how many simultaneous threads are run for loading ad details | import-platsannonser, import-platsannonser-daily |
| URL_ENRICH_TEXTDOCS_SERVICE | https://textdoc-enrichments.dev.services.jtech.se/enrichtextdocuments | Endpoint for ML enrichment of job ads |import-platsannonser, import-platsannonser-daily|
| API_KEY_ENRICH_TEXTDOCS | | API key to use for enrichment | import-platsannonser, import-platsannonser-daily |
| COMPANY_LOGO_BASE_URL | https://www.arbetsformedlingen.se/rest/arbetsgivare/rest/af/v3/ | Endpoint to check for available company logo associated with job ad | import-platsannonser, import-platsannonser-daily|

## Console scripts
### import-taxonomy
Dumps taxonomy data into elasticsearch.

#### Usage

    $ import-taxonomy
    
### import-platsannonser
Imports updated,new and removed job ads from a REST endpoint at Arbetsförmedlingen.

If the configured index does not exists it is created and two aliases are created, with the suffixes "-read", and "-write" (e.g. platsannons-write) once import is completed.
The "-read" alias is used by default by the JobSearch API.

#### Usage

    $ import-platsannonser
    
Starts an import into the current "-write" alias if it exists. Otherwise, a new is created as stated above.
This script is typically run every few minutes by cron.

### import-platsannoser-daily

    $ import-platsannonser-daily
    
Creates a new index for platsannonser according to the configured ```ES_ANNONS_INDEX``` environment variable, with todays date and hour as a suffix, and starts a full load into that index.
After successfull import, new "-read" and "-write" aliases are pointed to the new index.
This is run every night to create a fresh index and make sure no ads are missed by ```import-platsannonser```.

## Test

## Köra unittester
    $ python3 -m pytest -svv -ra -m unit tests/
    
### Köra integrationstester    
When running integration tests, the system needs access to other services so you need to specify environment variables in order for it to run properly.

    $ python3 -m pytest -svv -ra -m integration tests/
