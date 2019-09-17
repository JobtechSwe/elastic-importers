import logging
import sys
import time
import psycopg2
import datetime
import json
from importers import settings

log = logging.getLogger(__name__)

pg_conn = None

if settings.PG_DBNAME and settings.PG_USER:
    try:
        if not settings.PG_HOST:
            log.info("PG_HOST not set, assuming local socket")
            pg_conn = psycopg2.connect(dbname=settings.PG_DBNAME,
                                       user=settings.PG_USER)
        else:
            pg_conn = psycopg2.connect(host=settings.PG_HOST,
                                       port=settings.PG_PORT,
                                       dbname=settings.PG_DBNAME,
                                       user=settings.PG_USER,
                                       password=settings.PG_PASSWORD,
                                       sslmode=settings.PG_SSLMODE)
    except psycopg2.OperationalError as e:
        log.error("Failed to connect to PostgreSQL on %s:%s" % (settings.PG_HOST,
                                                                settings.PG_PORT))
        log.error("Reason for PostgreSQL failure: %s" % str(e))
        sys.exit(1)


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


# Below is methods used by loaders
def get_new_pg_conn():
    if not settings.PG_HOST:
        log.info("PG_HOST not set, assuming local socket")
        new_conn = psycopg2.connect(dbname=settings.PG_DBNAME,
                                    user=settings.PG_USER)
    else:
        new_conn = psycopg2.connect(host=settings.PG_HOST,
                                    port=settings.PG_PORT,
                                    dbname=settings.PG_DBNAME,
                                    user=settings.PG_USER,
                                    password=settings.PG_PASSWORD,
                                    sslmode=settings.PG_SSLMODE)
    return new_conn


def query(sql, args):
    cur = pg_conn.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall()
    cur.close()
    return rows


def table_exists(table):
    cur = pg_conn.cursor()
    cur.execute("select exists(select * from information_schema.tables "
                "where table_name=%s)", (table,))
    return cur.fetchone()[0]


def create_default_table(table):
    if not pg_conn:
        return
    statements = (
        "CREATE TABLE {table} (id VARCHAR(64) PRIMARY KEY, doc JSONB, "
        "timestamp BIGINT, expires BIGINT, "
        "expired boolean NOT NULL DEFAULT false)".format(table=table),
        "CREATE INDEX {table}_timestamp_idx ON {table} (timestamp)".format(table=table),
        "CREATE INDEX {table}_expires_idx ON {table} (expires)".format(table=table),
    )
    try:
        cur = pg_conn.cursor()
        for statement in statements:
            cur.execute(statement)
        cur.close()
        pg_conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        log.error("Failed to create database table: %s" % str(e))


def system_status(table):
    if not pg_conn:
        return None
    if not table_exists(table):
        create_default_table(table)
    cur = pg_conn.cursor()
    # Fetch last timestamp from table
    cur.execute("SELECT timestamp FROM " + table + " ORDER BY timestamp DESC LIMIT 1")
    ts_row = cur.fetchone()
    ts = _convert_to_timestring(ts_row[0]) \
        if ts_row else settings.LOADER_START_DATE
    cur.execute("SELECT id FROM " + table + " WHERE timestamp = %s",
                [convert_to_timestamp(ts)])
    id_rows = cur.fetchall()
    ids = [id_row[0] for id_row in id_rows]
    cur.close()
    return {'last_timestamp': ts, 'last_ids': ids}


def fetch_ad(ad_id, table):
    if not pg_conn:
        return None
    cur = pg_conn.cursor()
    cur.execute("SELECT * FROM " + table + " WHERE TRIM(id) = %s", [str(ad_id)])
    result = cur.fetchone()
    cur.close()
    return result


def fetch_expired_ads(ad_id_list, table, last_ts):
    try:
        ad_ids = tuple(ad_id_list)
        pg_conn = get_new_pg_conn()
        cur = pg_conn.cursor()
        sql_missing = (f"SELECT id FROM {table}" +
                       f" WHERE expires > {last_ts}" +
                       f" AND doc @> '{{\"avpublicerad\": false}}'" +
                       f" AND TRIM(id) not in %s")
        cur.execute(sql_missing, (ad_ids,))
        result = cur.fetchall()
        return [id[0] for id in result]
    except psycopg2.DatabaseError as e:
        log.error(e)
    finally:
        cur.close()
        pg_conn.close()


def check_missing_ads(ad_id_list, table):
    try:
        pg_conn = get_new_pg_conn()
        cur = pg_conn.cursor()
        ad_ids = tuple(ad_id_list)
        sql = f"SELECT id FROM {table} WHERE TRIM(id) in %s"
        cur.execute(sql, (ad_ids,))
        result = cur.fetchall()
        found_ids = [id[0] for id in result]
        return list(set(ad_id_list)-set(found_ids))
    except psycopg2.DatabaseError as e:
        log.error(e)
    finally:
        cur.close()
        pg_conn.close()


def update_ad(ad_id, doc, timestamp, table):
    cur = pg_conn.cursor()
    cur.execute("UPDATE " +
                table +
                " SET doc = %s, timestamp = %s WHERE TRIM(id) = %s",
                (json.dumps(doc),
                 convert_to_timestamp(timestamp),
                 str(ad_id)))
    pg_conn.commit()
    cur.close()


def set_all_expired(table):
    if not table_exists(table):
        create_default_table(table)
        return
    cur = pg_conn.cursor()
    cur.execute("UPDATE " + table + " SET expired = %s", [True])
    pg_conn.commit()
    cur.close()


def set_expired_for_ids(table, ad_ids, expired=True):
    if not pg_conn:
        return
    cur = pg_conn.cursor()
    for ad_id in ad_ids:
        cur.execute("UPDATE " + table + " SET expired = %s WHERE id = %s", [expired, str(ad_id)])
    pg_conn.commit()
    cur.close()


def system_status_platsannonser(table):
    if not table_exists(table):
        create_default_table(table)
    cur = pg_conn.cursor()
    # Fetch last timestamp from table
    cur.execute("SELECT timestamp FROM " + table + " ORDER BY timestamp DESC LIMIT 1")
    ts_row = cur.fetchone()
    ts = ts_row[0] \
        if ts_row else convert_to_timestamp(settings.LOADER_START_DATE)
    cur.execute("SELECT TRIM(id) FROM " + table + " WHERE timestamp = %s",
                [ts])
    id_rows = cur.fetchall()
    ids = [id_row[0] for id_row in id_rows]
    cur.close()
    return {'last_timestamp': ts, 'last_ids': ids}


def bulk(items, table):
    if not pg_conn:
        log.warning('No database configured for this session.')
        return
    start_time = time.time()
    if not table_exists(table):
        create_default_table(table)
    adapted_items = [(item['id'].strip(),
                      convert_to_timestamp(item['updatedAt']),
                      convert_to_timestamp(item.get('expiresAt')),
                      json.dumps(item),
                      convert_to_timestamp(item['updatedAt']),
                      convert_to_timestamp(item.get('expiresAt')),
                      json.dumps(item), False) for item in items if item]
    try:
        bulk_conn = get_new_pg_conn()
        cur = bulk_conn.cursor()
        cur.executemany("INSERT INTO " +
                        table + " "
                        "(id, timestamp, expires, doc) VALUES (%s, %s, %s, %s) "
                        "ON CONFLICT (id) DO UPDATE "
                        "SET timestamp = %s, expires = %s, doc = %s, expired = %s",
                        adapted_items)
        bulk_conn.commit()

    except psycopg2.DatabaseError as e:
        log.error('Could not bulk insert in database', e)
        sys.exit(1)
    finally:
        cur.close()
        bulk_conn.close()

    elapsed_time = time.time() - start_time

    log.info("Bulk inserted %d docs in: %s seconds." % (len(adapted_items), elapsed_time))


def convert_to_timestamp(date):
    if not date:
        return None
    if type(date) == int and date > 0:
        # Already a valid timestamp
        return date

    ts = 0
    for dateformat in [
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S%Z',
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d'
    ]:

        try:
            ts = time.mktime(time.strptime(date, dateformat)) * 1000
            log.debug("Converted date %s to %d" % (date, ts))
            break
        except ValueError as e:
            log.debug("Failed to convert date %s" % date, e)

    return int(ts)


def _convert_to_timestring(ts):
    return datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%dT%H:%M:%SZ')
