import pprint
from dataclasses import asdict

import deepdiff
from psycopg2.extensions import connection


def exec_req(engine: connection, req: str, *args):
    with engine.cursor() as curr:
        curr.execute(req, args)
    return engine.commit()


def clean_tables(engine: connection, tables: list[str]):
    _tables = ', '.join(tables)
    exec_req(engine, f'TRUNCATE {_tables}')


TABLES: list[str] | None = None


def fetch_all(engine: connection, query: str, *data, as_dict=True):
    with engine.cursor() as cur:
        cur.execute(query) if not data else cur.execute(query, data)
        col_names = [desc[0] for desc in cur.description]
        resp = cur.fetchall()
    return [
        dict(zip(col_names, d)) if as_dict else d[0]
        for d in resp
    ]


def get_tables(engine: connection, ignore=None):
    global TABLES
    if TABLES:
        return TABLES

    table_query = """
    SELECT concat_ws('.', schemaname, tablename) as table
    from pg_catalog.pg_tables
    where schemaname IN ('public')
    ORDER BY schemaname, tablename
    """
    resp = fetch_all(engine, table_query)
    # Foreign keys
    ignore = set(ignore) if ignore else []
    TABLES = [x['table'] for x in resp if x['table'] not in ignore]
    return TABLES


def pprint_dataclass_diff(d1, d2):
    _d1 = [asdict(x) for x in d1] if isinstance(d1, list) else asdict(d1)
    _d2 = [asdict(x) for x in d2] if isinstance(d2, list) else asdict(d2)
    diff = deepdiff.DeepDiff(_d1, _d2)
    pprint.pprint(diff)


def insert_many(table, data, conn, **kwargs):
    keys = tuple(data[0].keys())
    fields = ', '.join(f'{k}' for k in keys)
    values = ', '.join(f'%s' for _ in keys)
    query = f'INSERT INTO {table} ({fields}) VALUES ({values})'
    _data = [tuple(x.values()) for x in data]
    with conn.cursor() as cur:
        _ = cur.executemany(query, _data)
    return conn.commit()
