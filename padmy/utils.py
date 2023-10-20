from importlib import reload

import asyncpg
import json
import os
import re
import subprocess
import typing
from contextlib import contextmanager
from pathlib import Path
from piou import Option, Derived, Password
from typing import Sequence, AsyncIterator, Callable, TypeVar, cast, Literal

from padmy import env
from .env import PG_HOST, PG_PORT, PG_USER, PG_DATABASE, PG_PASSWORD
from .logs import logs

PgHost = Option(PG_HOST, "--host", help="PG Host")
PgPort = Option(PG_PORT, "--port", help="PG Port")
PgUser = Option(PG_USER, "--user", help="PG User")
PgPassword = Option(PG_PASSWORD, "-p", help="PG Password")
PgDatabase = Option(PG_DATABASE, "--db", help="PG Database")


def get_pg_root(
    pg_host: str = Option(PG_HOST, "--host", help="PG Host"),
    pg_port: int = Option(PG_PORT, "--port", help="PG Port"),
    pg_user: str = Option(PG_USER, "--user", help="PG User"),
    pg_password: Password = Option(PG_PASSWORD, "-p", help="PG Password"),
):
    return f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}"


def get_pg_url(pg_url: str = Derived(get_pg_root), pg_database: str = PgDatabase):
    return f"{pg_url}/{pg_database}"


async def get_pg(pg_url: str = Derived(get_pg_url)):
    return await asyncpg.connect(pg_url)


def exec_cmd(
    cmd: Sequence[str | int],
    env: dict | None = None,
    *,
    on_stderr: Callable | None = None,
) -> str:
    _cmd = cmd if isinstance(cmd, str) else " ".join(str(x) for x in cmd)
    logs.debug(f"Executing cmd: {_cmd}")
    stdout, stderr = subprocess.Popen(
        _cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    ).communicate()
    if stderr:
        _msg = stderr.decode("utf-8")
        logs.warning(_msg)
        if on_stderr:
            on_stderr(_msg)
    return stdout.decode("utf-8")


def has_cmd(cmd: str) -> bool:
    return exec_cmd(f'command -v "{cmd}"') != ""


class CommandNotFound(Exception):
    def __init__(self, cmd: str):
        super().__init__()
        self.cmd = cmd


_COMMANDS = {}


def check_cmd(cmd: str):
    global _COMMANDS
    path = exec_cmd(f'command -v "{cmd}"')
    if path == "":
        raise CommandNotFound(cmd=cmd)
    _COMMANDS[cmd] = path.strip()


def get_pg_envs():
    reload(env)
    return {
        "PGPASSWORD": env.PG_PASSWORD,
        "PGUSER": env.PG_USER,
        "PGHOST": env.PG_HOST,
        "PGPORT": str(env.PG_PORT),
    }


def _on_pg_error(msg: str):
    if "error" in msg or "fatal" in msg:
        raise ValueError(msg)


def _get_conn_infos(
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
):
    infos = {"-U": user, "-W": password, "-h": host, "-p": port}
    cmd = []
    for k, v in infos.items():
        if v is not None:
            cmd += [k, v]
    return cmd


def pg_dump(
    database: str,
    schemas: list[str],
    dump_path: str,
    options: list[str] | None = None,
    *,
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
):
    _schemas = "|".join(x for x in schemas)
    cmd = [
        _COMMANDS["pg_dump"],
        "-n",
        f"'({_schemas})'",
        "-d",
        database,
    ] + _get_conn_infos(user, password, host, port)
    if options:
        cmd += options

    cmd += [">", dump_path]
    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


def pg_restore(
    database: str,
    dump_path: str,
    options: list[str] | None = None,
    *,
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
):
    cmd = [_COMMANDS["pg_restore"], "-d", database, dump_path] + _get_conn_infos(
        user, password, host, port
    )
    if options is not None:
        cmd += options
    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


def create_db(
    database: str,
    *,
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
):
    cmd = [_COMMANDS["createdb"], database] + _get_conn_infos(
        user, password, host, port
    )
    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


def drop_db(
    database: str,
    *,
    if_exists: bool = True,
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
):
    cmd = [_COMMANDS["dropdb"]] + _get_conn_infos(user, password, host, port)

    if if_exists:
        cmd.append("--if-exists")
    cmd.append(database)
    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


def exec_psql_file(
    database: str,
    sql_file: str,
    *,
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
):
    cmd = [_COMMANDS["psql"], "-f", sql_file, "-d", database] + _get_conn_infos(
        user, password, host, port
    )
    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


def exec_psql(
    database: str,
    query: str,
    *,
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
):
    cmd = [_COMMANDS["psql"], "-c", f"'{query}'", "-d", database] + _get_conn_infos(
        user, password, host, port
    )
    exec_cmd(cmd, env=get_pg_envs(), on_stderr=_on_pg_error)


async def exec_file(conn: asyncpg.Connection, file: Path):
    await conn.execute(file.read_text())


async def iterate_pg(
    conn: asyncpg.Connection,
    query: str,
    *args,
    from_offset: int = 0,
    chunk_size: int = 500,
    timeout: int | None = None,
) -> AsyncIterator[list[asyncpg.Record]]:
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


async def check_extension_exists(conn: asyncpg.Connection, extension: str) -> bool:
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


X = TypeVar("X")


def get_first(*it: X, fn: Callable) -> X | None:
    for v in it:
        if fn(v):
            return v
    return None


async def get_conn(pool: asyncpg.Pool, fn: Callable):
    async with pool.acquire() as conn:
        await fn(conn=conn)


def get_pg_root_from(source: Literal["from", "to"]):
    _source, _source_lower = source.upper(), source.lower()

    def _get_pg_root(
        pg_host: str = Option(
            os.getenv(f"PG_HOST_{_source}", "localhost"),
            f"--host-{_source_lower}",
            help=f"PG Host {_source}",
            arg_name=f"pg_host_{_source}",
        ),
        pg_port: int = Option(
            int(os.getenv(f"PG_PORT_{_source}", 5432)),
            f"--port-{_source_lower}",
            help=f"PG Port {_source}",
            arg_name=f"pg_port_{_source}",
        ),
        pg_user: str = Option(
            os.getenv(f"PG_USER_{_source}", "postgres"),
            f"--user-{_source_lower}",
            help=f"PG User {_source}",
            arg_name=f"pg_user_{_source}",
        ),
        pg_password: Password = Option(
            os.getenv(f"PG_PASSWORD_{_source}", "postgres"),
            f"--password-{_source_lower}",
            help=f"PG Password {_source}",
            arg_name=f"pg_password_{_source}",
        ),
    ):
        url = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}"
        pg_clean = url.replace(f":{pg_password}", ":********")
        logs.debug(f"PG url {source}: {pg_clean}")
        return url

    return _get_pg_root


class PGUriInfos(typing.TypedDict):
    user: str
    password: str
    host: str
    port: int
    database: str | None


_PG_URI_REG = re.compile(
    r"postgresql:\/\/(?P<user>.*):(?P<password>.*)@(?P<host>.*):(?P<port>\d+)(\/(?P<dbname>.*))?"
)


def parse_pg_uri(uri: str) -> PGUriInfos:
    if match := _PG_URI_REG.match(uri):
        group = match.groupdict()
        return {
            "user": group["user"],
            "password": group["password"],
            "host": group["host"],
            "port": int(group["port"]),
            "database": group["dbname"],
        }
    raise ValueError(f"Invalid PG uri: {uri}")


@contextmanager
def temp_env(new_env: dict):
    """
    Overrides the environment variables with the given ones
    """
    _env = os.environ.copy()
    os.environ.update(new_env)
    try:
        yield
    finally:
        os.environ.update(_env)


async def init_connection(conn: asyncpg.Connection):
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    await conn.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
