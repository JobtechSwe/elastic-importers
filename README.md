# elastic-importer
Elasticsearch importscript för kandidat- och annons-söktjänster.

## Användning
Script som är tänkta att köras med olika intervall, i regel via cron, för att ladda in diverse olika typer av data till Elasticsearch.

## Installation
### Exempel virtualenv:

    $ workon <virtuell miljö för huvudapplikationen>
    $ python setup.py develop

### Exempel anaconda:

    $ source activate <virtuell miljö för huvudapplikationen>
    $ python setup.py develop
    

### import-taxonomy
Importerar värdeförråd från Arbetsförmedlingens taxonomitjänst via SOAP. Förutsätter att det finns ett Elasticsearch-cluster att ladda
in datan till. Datat ändras relativt sällan och scriptet bör inte köras mer ofta än dagligen för närvarande.
Följande environmental variabler används:

|Environment variable   | Default value  | Comment |
|---|---|---|
| ES_HOST  | localhost  | Elasticsearch host |
| ES_PORT  | 9200  | Elasticsearch port |
| ES_USER  |   | Elasticsearch username |
| ES_PWD  |   | Elasticsearch password |
| ES_TAX_INDEX_BASE  | taxonomy-  | Base string from which index for different taxonomyversions will be created |
| ES_TAX_INDEX_ALIAS  |  taxonomy | Alias for index that is the current version of the taxonomy |
| ES_TAX_ARCHIVE_ALIAS  |  taxonomy-archive | Alias collecting all older versions of the taxonomy |
| TAXONOMY_SERVICE_URL  | http://api.arbetsformedlingen.se/taxonomi/v0/TaxonomiService.asmx  | URL for the taxonomy SOAP service |


#### Användning

    $ import-taxonomy
    
### import-kandidater
TBD
#### Användning
TBD

### import-platsannonser
TBD
#### Användning
TBD

### import-auranest
TBD
#### Användning
TBD
