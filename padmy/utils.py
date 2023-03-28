import subprocess
from importlib import reload
from pathlib import Path
from typing import Sequence, AsyncIterator, Callable, TypeVar, cast

import asyncpg
from piou import Option, Derived, Password

from padmy import env
from .env import PG_HOST, PG_PORT, PG_USER, PG_DATABASE, PG_PASSWORD
from .logs import logs

PgHost = Option(PG_HOST, '--host', help='PG Host')
PgPort = Option(PG_PORT, '--port', help='PG Port')
PgUser = Option(PG_USER, '--user', help='PG User')
PgPassword = Option(PG_PASSWORD, '-p', help='PG Password')
PgDatabase = Option(PG_DATABASE, '--db', help='PG Database')


def get_pg_root(
        pg_host: str = PgHost,
        pg_port: int = PgPort,
        pg_user: str = PgUser,
        pg_password: Password = PgPassword,
):
    return f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}'


def get_pg_url(
        pg_url: str = Derived(get_pg_root),
        pg_database: str = PgDatabase
):
    return f'{pg_url}/{pg_database}'


async def get_pg(
        pg_url: str = Derived(get_pg_url)
):
    return await asyncpg.connect(pg_url)


def exec_cmd(cmd: Sequence[str | int],
             env: dict | None = None,
             *,
             on_stderr: Callable | None = None) -> str:
    _cmd = cmd if isinstance(cmd, str) else ' '.join(str(x) for x in cmd)
    logs.debug(f'Executing cmd: {_cmd}')
    stdout, stderr = subprocess.Popen(_cmd,
                                      shell=True,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE,
                                      env=env).communicate()
    if stderr:
        _msg = stderr.decode('utf-8')
        logs.warning(_msg)
        if on_stderr:
            on_stderr(_msg)
    return stdout.decode('utf-8')


def has_cmd(cmd: str) -> bool:
    return exec_cmd(f'command -v "{cmd}"') != ''


class CommandNotFound(Exception):
    def __init__(self, cmd: str):
        super().__init__()
        self.cmd = cmd


def check_cmd(cmd: str):
    if not has_cmd(cmd):
        raise CommandNotFound(cmd=cmd)


def get_pg_envs():
    reload(env)
    return {'PGPASSWORD': env.PG_PASSWORD,
            'PGUSER': env.PG_USER,
            'PGHOST': env.PG_HOST,
            'PGPORT': str(env.PG_PORT)}


def _on_pg_error(msg: str):
    if 'error' in msg:
        raise ValueError(msg)


def pg_dump(
        database: str,
        schemas: list[str],
        dump_path: str,
        options: list[str] | None = None
):
    _schemas = '|'.join(x for x in schemas)
    cmd = [
        'pg_dump',
        '-n', f"'({_schemas})'",
        '-d', database
    ]

    if options:
        cmd += options

    cmd += ['>', dump_path]

    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


def pg_restore(
        database: str,
        dump_path: str,
):
    cmd = [
        'pg_restore', '-d', database,
        dump_path
    ]
    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


def create_db(database: str):
    cmd = ['createdb', database]
    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


def drop_db(database: str, if_exists: bool = True):
    cmd = ['dropdb']
    if if_exists:
        cmd.append('--if-exists')
    cmd.append(database)
    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


def exec_psql_file(database: str, sql_file: str):
    cmd = ['psql', '-f', sql_file, '-d', database]
    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


def exec_psql(database: str, query: str):
    cmd = ['psql', '-c', f"'{query}'", '-d', database]
    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


async def exec_file(conn: asyncpg.Connection, file: Path):
    await conn.execute(file.read_text())


async def iterate_pg(conn: asyncpg.Connection,
                     query: str,
                     *args,
                     from_offset: int = 0,
                     chunk_size: int = 500,
                     timeout: int | None = None) -> AsyncIterator[list[asyncpg.Record]]:
    async with conn.transaction():
        cur: asyncpg.connection.cursor.Cursor = await conn.cursor(query, *args)
        if from_offset:
            await cur.forward(from_offset, timeout=timeout)
        while data := await cur.fetch(chunk_size, timeout=timeout):
            yield cast(list[asyncpg.Record], data)


_EXT_EXISTS_QUERY = """
SELECT EXISTS(
    SELECT FROM pg_extension WHERE extname = $1
)
"""


async def check_extension_exists(conn: asyncpg.Connection,
                                 extension: str) -> bool:
    _exists = await conn.fetchval(_EXT_EXISTS_QUERY, extension)
    if _exists is None:
        raise NotImplementedError()
    return _exists


_TMP_TABLE_EXISTS_QUERY = """
    SELECT EXISTS (
        SELECT 
        FROM   information_schema.tables 
        WHERE  table_schema LIKE 'pg_temp_%'
        AND table_name = $1
    )
"""


async def check_tmp_table_exists(conn: asyncpg.Connection, table: str) -> bool:
    exists = await conn.fetchval(_TMP_TABLE_EXISTS_QUERY, table)
    if exists is None:
        raise NotImplementedError()
    return exists


async def insert_many(conn: asyncpg.Connection,
                      table: str,
                      data: list[dict]):
    """
    Keys in dict must have the same order
    """
    keys = tuple(data[0].keys())
    fields = ', '.join(f'"{k}"' for k in keys)
    values = ', '.join(f'${i + 1}' for i, _ in enumerate(keys))
    query = f'INSERT INTO {table} ({fields}) VALUES ({values})'
    _data = [tuple(x.values()) for x in data]
    await conn.executemany(query, _data)


X = TypeVar('X')


def get_first(*it: X, fn: Callable) -> X | None:
    for v in it:
        if fn(v):
            return v
    return None


async def get_conn(pool: asyncpg.Pool, fn: Callable):
    async with pool.acquire() as conn:
        await fn(conn=conn)
