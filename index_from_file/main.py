from importers.repository import elastic
from index_from_file import settings
from index_from_file.file_handling import unpickle_ads


def create_index_from_saved_ads():
    num_indexed = elastic.bulk_index(unpickle_ads(), settings.INDEX_NAME, settings.deleted_index)
    print(f"Added {num_indexed} ads to index {settings.INDEX_NAME}")


if __name__ == '__main__':
    create_index_from_saved_ads()
