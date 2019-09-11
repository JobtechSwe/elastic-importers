import logging
import sys
import time
import psycopg2
from importers import settings

log = logging.getLogger(__name__)

if not settings.PG_DBNAME or not settings.PG_USER:
    print("You must set environment variables PG_DBNAME and PG_USER.")
    sys.exit(1)

try:
    pg_conn = psycopg2.connect(host=settings.PG_HOST,
                               port=settings.PG_PORT,
                               dbname=settings.PG_DBNAME,
                               user=settings.PG_USER,
                               password=settings.PG_PASSWORD,
                               sslmode=settings.PG_SSLMODE)
    log.debug("Postgresql DSN: %s" % pg_conn.get_dsn_parameters())
except psycopg2.OperationalError as e:
    log.error("Failed to connect to PostgreSQL on %s:%s" % (settings.PG_HOST,
                                                            settings.PG_PORT))
    log.error("Reason for PostgreSQL failure: %s" % str(e))
    sys.exit(1)


def query(sql, args):
    cur = pg_conn.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall()
    cur.close()
    return rows

def read_docs_with_ids(tablename, ids, converter=None):
    cur = pg_conn.cursor()

    sql_str = "SELECT id, timestamp, doc FROM " + tablename + \
              " WHERE " \
              " id in %(incl_id)s"
    cur.execute(sql_str,
                {'incl_id': tuple(ids if len(ids) > 0 else ['this string needed for sql syntax'])})
    rows = cur.fetchall()

    documents = [dict(converter.convert_message(row[2]) if converter else dict(row[2]),
                      **{'id': row[0].strip(), 'timestamp': row[1]})
                 for row in rows]

    return documents


def read_from_pg_since(last_ids, timestamp, tablename, converter=None):
    cur = pg_conn.cursor()
    ts_today = int(round(time.time() * 1000))  # Get current timestamp

    sql_last_ids = [str(id) for id in last_ids]

    sql_str = "SELECT id, timestamp, doc FROM " + tablename + \
              " WHERE timestamp >= %(ts)s AND (expires IS NULL OR expires > %(expires)s)" \
              " AND expired = %(expired)s" \
              " AND id not in %(excl_id)s" \
              " ORDER BY timestamp ASC LIMIT %(limit)s"
    cur.execute(sql_str,
                {'ts': timestamp,
                 'expires': ts_today,
                 'expired': False,
                 'excl_id': tuple(sql_last_ids if len(sql_last_ids) > 0 else ['this string needed for sql syntax']),
                 'limit': settings.PG_BATCH_SIZE})
    rows = cur.fetchall()

    # Create a list of dictionaries from row[2] adding to it the id and
    # timestamp from row[0] and row[1] unless the id is the one of the
    # same as last time method was called
    documents = [dict(converter.convert_message(row[2]) if converter else dict(row[2]),
                      **{'id': row[0], 'timestamp': row[1]})
                 for row in rows if row[0] not in last_ids]

    # Return a tuple containing a list of last ids, last timestamp and
    # list of dictionaries (annonser to save)
    return [row[0] for row in rows], rows[-1][1] if rows else 0, documents
