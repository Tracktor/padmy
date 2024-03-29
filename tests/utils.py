import pprint
from dataclasses import asdict
from pathlib import Path
from psycopg.errors import DuplicateDatabase
import deepdiff
import psycopg
from typing_extensions import LiteralString
from typing import cast


def pprint_dataclass_diff(d1, d2):
    _d1 = [asdict(x) for x in d1] if isinstance(d1, list) else asdict(d1)
    _d2 = [asdict(x) for x in d2] if isinstance(d2, list) else asdict(d2)
    diff = deepdiff.DeepDiff(_d1, _d2)
    pprint.pprint(diff)


_TABLE_EXISTS_QUERY = """
SELECT EXISTS (
   SELECT FROM information_schema.tables 
   WHERE  table_schema = %s
   AND    table_name   = %s
   )
"""


def check_table_exists(engine: psycopg.Connection, schema: str, table: str):
    with engine.cursor() as c:
        c.execute(_TABLE_EXISTS_QUERY, (schema, table))
        res = c.fetchone()
    return res[0] if res else None


_COLUMN_EXISTS_QUERY = """
SELECT EXISTS (
    SELECT
    FROM information_schema.columns 
    WHERE table_schema  = %s 
    AND table_name = %s 
    AND column_name = %s
)
"""


def check_column_exists(engine: psycopg.Connection, schema: str, table: str, column: str):
    with engine.cursor() as c:
        c.execute(_COLUMN_EXISTS_QUERY, (schema, table, column))
        res = c.fetchone()
    return res[0] if res else None


def compare_files(f1: Path, f2: Path, ignore_order: bool = False):
    def get_lines(f: Path):
        return [x.strip() for x in f.read_text().split("\n") if x.strip()]

    assert not deepdiff.DeepDiff(get_lines(f1), get_lines(f2), ignore_order=ignore_order)


def create_db(url: str, db: str, *, drop_first: bool = True):
    pg_conn = psycopg.connect(f"{url}/postgres")
    pg_conn.autocommit = True
    try:
        if drop_first:
            try:
                pg_conn.execute(cast(LiteralString, f"DROP DATABASE {db}"))
            except psycopg.errors.InvalidCatalogName:
                pass
        try:
            with pg_conn.cursor() as cursor:
                cursor.execute(cast(LiteralString, f"CREATE DATABASE {db}"))
        except DuplicateDatabase as e:
            if drop_first:
                raise e
    finally:
        pg_conn.close()
