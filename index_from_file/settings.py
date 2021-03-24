import pathlib
from datetime import datetime
PATH = pathlib.Path.cwd()
FILE_NAME = f"enriched_ads_{datetime.now().strftime('%Y%m%d-%H%M')}.pickle"
INDEX_NAME = "platsannons-import-from-file-0850"
deleted_index = f"{INDEX_NAME}-deleted"


FILE_TO_UNPICKLE = ''

