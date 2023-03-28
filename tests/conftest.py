import asyncio
import os
from pathlib import Path

import asyncpg
import psycopg2
import psycopg2.errors
import pytest
from psycopg2.extensions import connection

PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = int(os.getenv('PG_PORT', '5432'))
PG_USER = 'postgres'  # os.getenv('PG_USER', 'postgres')
PG_PASSWORD = 'postgres'  # os.getenv('PG_PASSWORD', 'postgres')
PG_DATABASE = 'test'  # os.getenv('PG_DATABASE', 'test')

PG_URL = f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}'

STATIC_DIR = Path(os.path.dirname(__file__)) / 'static'


@pytest.fixture(scope='session', autouse=True)
def set_envs():
    os.environ['PG_PASSWORD'] = PG_PASSWORD
    os.environ['PG_USER'] = PG_USER


@pytest.fixture(scope='session')
def engine() -> connection: # type: ignore
    conn = psycopg2.connect(f'{PG_URL}/{PG_DATABASE}')

    yield conn

    conn.close()


@pytest.fixture(scope='session')
def aengine(loop):
    conn = loop.run_until_complete(asyncpg.connect(f'{PG_URL}/{PG_DATABASE}'))
    yield conn
    loop.run_until_complete(conn.close())


@pytest.fixture(scope='session')
def apool(loop):
    pool = loop.run_until_complete(
        asyncpg.create_pool(f'{PG_URL}/{PG_DATABASE}', loop=loop)
    )
    yield pool
    loop.run_until_complete(asyncio.wait_for(pool.close(), timeout=3))


@pytest.fixture(scope='session')
def loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=False, scope="session")
def setup_test_db():
    pg_conn = psycopg2.connect(f'{PG_URL}/postgres')
    pg_conn.autocommit = True
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {PG_DATABASE}")
    except psycopg2.errors.DuplicateDatabase:
        pass

    yield

    pg_conn.close()
    # with pg_conn.cursor() as cursor:
    #     cursor.execute(f"DROP DATABASE {PG_DATABASE}")
