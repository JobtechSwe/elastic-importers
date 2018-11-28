import sys
from elasticsearch.exceptions import NotFoundError
from importers.repository import elastic
from importers import settings

WRITE_SUFFIX = '-write'
READ_SUFFIX = '-read'


def set_platsannons_read_alias():
    if len(sys.argv) < 1:
        print("Must provide name of index to alias against.")
        sys.exit(1)

    idxname = sys.argv[1]
    aliasname = "%s%s" % (settings.ES_ANNONS_PREFIX, READ_SUFFIX)
    change_alias(idxname, aliasname)


def set_platsannons_write_alias():
    if len(sys.argv) < 1:
        print("Must provide name of index to alias against.")
        sys.exit(1)

    idxname = sys.argv[1]
    change_alias(idxname, settings.ES_ANNONS_INDEX)


def set_auranest_read_alias():
    if len(sys.argv) < 1:
        print("Must provide name of index to alias against.")
        sys.exit(1)

    idxname = sys.argv[1]
    aliasname = "%s%s" % (settings.ES_AURANEST_PREFIX, READ_SUFFIX)
    change_alias(idxname, aliasname)


def set_auranest_write_alias():
    if len(sys.argv) < 1:
        print("Must provide name of index to alias against.")
        sys.exit(1)

    idxname = sys.argv[1]
    change_alias(idxname, settings.ES_AURANEST_INDEX)


def change_alias(idxname, aliasname):
    try:
        if elastic.alias_exists(aliasname):
            oldindices = list(elastic.get_alias(aliasname).keys())
            elastic.update_alias(idxname, oldindices, aliasname)
        else:
            elastic.add_indices_to_alias(idxname, aliasname)
    except NotFoundError:
        print("Error: Can't create alias \"%s\". Index \"%s\" not found" % (aliasname,
                                                                            idxname))
        sys.exit(1)


if __name__ == '__main__':
    print("Use methods 'set_read_alias' or 'set_write_alias' for wanted dataset.")
