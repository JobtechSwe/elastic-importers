Utility to save enriched ads to a file so that they can be imported into any Elastic in a reproducible way. Intended for
test index with known data that the tests depend on

Usage:

set variable SAVE_ENRICHED_ADS = True in importers.settings

Make sure the desired ads are in the Mock Ads repo (anonymized)

import

a Pickle file is created, save it

To import into Elastic:
set filename in settings (FILE_TO_UNPICKLE)

run index_from_file/main.py
