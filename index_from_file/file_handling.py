import pickle
import logging
from index_from_file import settings


def save_enriched_ads_to_file(list_of_enriched_ads):
    with open(settings.PATH / settings.FILE_NAME, 'wb') as f:
        pickle.dump(list_of_enriched_ads, f, protocol=pickle.HIGHEST_PROTOCOL)
        logging.info(f"saved {len(list_of_enriched_ads)} to file {settings.PATH / settings.FILE_NAME}")


def unpickle_ads():
    with open(settings.FILE_TO_UNPICKLE, 'rb') as f:
        return pickle.load(f)
