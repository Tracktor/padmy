import asyncio
import logging
import os
import typing
from pathlib import Path

import asyncpg
import psycopg
import pytest
from tracktolib.pg_sync import get_tables, clean_tables, insert_many
from typing_extensions import LiteralString
import tempfile
from .utils import create_db

__all__ = (
    "engine",
    "aengine",
    "apool",
    "loop",
    "setup_tables",
    "clean_pg",
    "populate_data",
    "sample_engine",
    "clean_sample_pg",
    "setup_sample_tables",
    "PG_URL",
    "PG_DATABASE",
    "PG_SAMPLE_DATABASE",
    "STATIC_DIR",
    "setup_test_db",
    "setup_sample_db",
    "PG_HOST",
    "PG_PORT",
    "PG_PASSWORD",
    "set_envs",
)

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = "postgres"  # os.getenv('PG_USER', 'postgres')
PG_PASSWORD = "postgres"  # os.getenv('PG_PASSWORD', 'postgres')
PG_DATABASE = "test"  # os.getenv('PG_DATABASE', 'test')

PG_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}"

STATIC_DIR = Path(os.path.dirname(__file__)) / "static"


@pytest.fixture(scope="session", autouse=True)
def set_envs():
    from padmy.logs import logs

    os.environ["PG_PASSWORD"] = PG_PASSWORD
    os.environ["PG_USER"] = PG_USER
    logs.setLevel(logging.ERROR)
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ["PADMY_FOLDER"] = str(Path(tmpdirname) / ".padmy")
        yield


@pytest.fixture()
def padmy_folder():
    return Path(os.environ["PADMY_FOLDER"])


@pytest.fixture(scope="session")
def engine() -> typing.Generator[psycopg.Connection, None, None]:
    conn = psycopg.connect(f"{PG_URL}/{PG_DATABASE}")

    yield conn

    conn.close()


@pytest.fixture(scope="session")
def aengine(loop):
    from padmy.utils import init_connection

    conn = loop.run_until_complete(asyncpg.connect(f"{PG_URL}/{PG_DATABASE}"))
    loop.run_until_complete(init_connection(conn))
    yield conn
    loop.run_until_complete(conn.close())


@pytest.fixture(scope="session")
def apool(loop):
    from padmy.utils import init_connection

    pool = loop.run_until_complete(asyncpg.create_pool(f"{PG_URL}/{PG_DATABASE}", loop=loop, init=init_connection))
    yield pool
    loop.run_until_complete(asyncio.wait_for(pool.close(), timeout=3))


@pytest.fixture(scope="session")
def loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture()
def setup_tables(engine):
    sql_file = STATIC_DIR / "setup.sql"
    engine.execute(sql_file.read_text())
    engine.commit()


@pytest.fixture(autouse=False, scope="session")
def setup_test_db():
    create_db(PG_URL, PG_DATABASE, drop_first=False)


PG_SAMPLE_DATABASE = "sample"


@pytest.fixture(autouse=True, scope="session")
def setup_sample_db():
    create_db(PG_URL, PG_SAMPLE_DATABASE, drop_first=True)


@pytest.fixture()
def sample_engine() -> typing.Generator[psycopg.Connection, None, None]:
    conn = psycopg.connect(f"{PG_URL}/{PG_SAMPLE_DATABASE}")

    yield conn

    conn.close()


_TABLES: dict[str, list[LiteralString]] = {}


@pytest.fixture(scope="function", autouse=True)
def clean_pg(engine):
    global _TABLES

    _TABLES[PG_DATABASE] = get_tables(engine, schemas=["public", "test", "tmptest"])
    clean_tables(engine, _TABLES[PG_DATABASE])

    yield


@pytest.fixture()
def populate_data(engine):
    table_1 = [
        {
            "id": i,
            "foo": f"foo-{i}",
        }
        for i in range(10)
    ]
    table_2 = [{"id": 0, "table_1_id": table_1[0]["id"]}]

    insert_many(engine, "public.table_1", table_1)
    insert_many(engine, "public.table_2", table_2)


@pytest.fixture(
    scope="function",
)
def clean_sample_pg(sample_engine):
    global _TABLES

    _TABLES[PG_SAMPLE_DATABASE] = get_tables(sample_engine, schemas=["public", "test"])
    clean_tables(sample_engine, _TABLES[PG_SAMPLE_DATABASE])

    yield


@pytest.fixture()
def setup_sample_tables(sample_engine, clean_sample_pg):
    sql_file = STATIC_DIR / "setup.sql"
    sample_engine.execute(sql_file.read_text())
    sample_engine.commit()
