Utility to save enriched ads to a file so that they can be imported into any Elastic in a reproducable way. Intended for
test index with known data that the tests depend on

Usage:

Make sure the desired ads are in the Mock Ads repo (anonymized)
in main: _convert_and_save_to_elastic, add the following code after enrichment
(after enriched_ads = enricher.enrich(converted_ads))

    from index_from_file.file_handling import save_enriched_ads_to_file
    save_enriched_ads_to_file(enriched_ads)

This is intentional to avoid test code in the normal code. Set env vars and run import as ususal

a Pickle file (check name in settings.py) is created.

To import into Elastic:
set env vars check index_from_file/settings.py run index_from_file/main.py
