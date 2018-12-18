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
Importerar platsannonser från databas till Elasticsearch. 

Om det konfigurerade indexet inte existerar skapas det, och ett "skriv-alias" sätts upp mot det indexet med suffix "-write" (e.g. platsannons-write). 
På samma sätt är ```sokannonser-api``` konfigurerat för att läsa från ett "läs-alias" med suffix "-read".

#### Användning
Kommandot ```import-platsannonser``` startar en import av platsannonser till förkonfigurerat skriv-alias.

#### Omindexering
Vid förändringar i mappningar eller datastruktur behöver man läsa om hela indexet. Med fördel gör man då ett nytt index som man läser in allt data till utan att ställa som skriv-aliaset. 
Om man anger ett indexnamn som argument till ```import-platsannonser``` så skapas det indexet (om det inte redan finns), men inget nytt skriv-alias skapas.
På så sätt kan man skriva datat till ett nytt index och när det är klart kan man peka om skriv- och läs-aliasen till det nya indexet utan nedtid.

    $ import-platsannonser nytt-platsannons-index
    ... vänta tills datat lästs in ...
    $ set-write-alias-platsannons nytt-platsannons-index
    $ set-read-alias-platsannons nytt-platsannons-index

### import-auranest
TBD
#### Användning
TBD
