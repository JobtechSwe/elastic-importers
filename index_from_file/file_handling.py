import pickle
from index_from_file import settings


def save_enriched_ads_to_file(list_of_enriched_ads):
    with open(settings.PATH / settings.FILE_NAME, 'wb') as f:
        pickle.dump(list_of_enriched_ads, f, protocol=pickle.HIGHEST_PROTOCOL)


def unpickle_ads():
    with open(settings.PATH / settings.FILE_NAME, 'rb') as f:
        return pickle.load(f)
