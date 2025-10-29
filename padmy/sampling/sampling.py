import contextlib
import tempfile
from math import floor
from typing import cast, Literal

import asyncpg
from rich.progress import Progress
from tracktolib.pg import insert_many, iterate_pg

from ..db import Database, Table, FKConstraint
from ..logs import logs
from ..utils import (
    check_cmd,
    pg_restore,
    pg_dump,
    create_db,
    drop_db,
    exec_psql,
    check_extension_exists,
    parse_pg_uri,
    temp_env,
)


def copy_database(
    from_pg_uri: str,
    from_db: str,
    to_pg_uri: str,
    to_db: str,
    schemas: list[str],
    drop_public: bool = False,
):
    """
    Dumps and recreate the schemas from `from_db` into `to_db`.
    You can optionally specify `drop_public` to drop the public schema
    before restoring and avoid ' ERROR:  schema "public" already exists'
    (useful if you have *public* specified in your schemas list)
    """
    pg_from_infos, pg_target_infos = parse_pg_uri(from_pg_uri), parse_pg_uri(to_pg_uri)
    for cmd in ["pg_dump", "createdb", "dropdb", "pg_restore", "psql"]:
        check_cmd(cmd)

    with tempfile.NamedTemporaryFile(suffix=".dump") as tmp_file:
        # tmp_file = '/tmp/schema.dump'
        with temp_env({"PG_PASSWORD": pg_from_infos["password"]}):
            pg_dump(
                schemas=schemas,
                dump_path=tmp_file.name,
                options=[
                    "-Fc",
                    "--schema-only",
                    "--no-owner",
                    "--no-privileges",
                    "--extension=*",
                ],
                user=pg_from_infos["user"],
                host=pg_from_infos["host"],
                port=pg_from_infos["port"],
                database=from_db,
            )

        _partial_target_infos: dict = dict(pg_target_infos)
        _partial_target_infos.pop("database")
        with temp_env({"PG_PASSWORD": _partial_target_infos.pop("password")}):
            drop_db(
                to_db,
                **_partial_target_infos,
                if_exists=True,
            )
            create_db(database=to_db, **_partial_target_infos)

            if "public" in schemas or drop_public:
                exec_psql(to_db, "DROP SCHEMA public;", **_partial_target_infos)
            pg_restore(
                dump_path=tmp_file.name,
                database=to_db,
                options=["--no-owner", "--no-privileges"],
                **_partial_target_infos,
            )


def get_insert_child_fk_data_query(table: Table, child_table: Table) -> str:
    """
    Gets the query to insert data from a table to the temporary table.
    """
    joins = []

    def _fk_join(tmp_name: str, fk: FKConstraint, fk_index: int) -> str:
        return f"inner join {tmp_name} _s{fk_index} on " + " and ".join(
            f"_s{fk_index}.{column_name} = t.{foreign_column_name}"
            for column_name, foreign_column_name in zip(fk.column_names, fk.foreign_column_names)
        )

    for i, _fk in enumerate(child_table.foreign_keys):
        if _fk.foreign_full_name != table.full_name:
            continue
        joins.append(_fk_join(child_table.tmp_name, _fk, i))

    query = (
        f"""
    INSERT INTO {table.tmp_name} ({table.values})
    SELECT {table.get_values("t")} from {table.full_name} t
    """
        + "\n".join(joins)
        + "\n ON CONFLICT DO NOTHING"
    )

    return query


def get_insert_data_query(table: Table):
    where_str = "and ".join(f"t2.{_pk.column_name} = t1.{_pk.column_name}" for _pk in table.primary_keys)
    query = f"""
    INSERT into {table.tmp_name} ({table.values})
    SELECT {table.values} from {table.full_name} t1
    """
    if where_str:
        query = f"{query}\nwhere not exists(select null from {table.tmp_name} t2 where {where_str})"
    query = f"{query}\nLIMIT $1"
    return query


async def _insert_leaf_table(conn: asyncpg.Connection, table: Table, table_size: int):
    query = f"CREATE TEMP TABLE {table.tmp_name} ON COMMIT DROP AS SELECT * from {table.full_name}"
    args = []
    # if table_size > 0:
    query = f"{query} TABLESAMPLE SYSTEM_ROWS($1)"
    args.append(table_size)
    logs.debug(f"{query} {args}")
    await conn.execute(query, *args)


async def _insert_node_table(conn: asyncpg.Connection, table: Table, table_size: int):
    # Creating the table
    query = f"CREATE TEMP TABLE {table.tmp_name} (LIKE {table.full_name} INCLUDING ALL) ON COMMIT DROP"
    logs.debug(f"Create node table query: {query}")
    await conn.execute(query)

    # Inserting data from child table
    for _child_table in table.child_tables_safe:
        query = get_insert_child_fk_data_query(table, _child_table)
        logs.debug(f"Insert child data query: {query}")
        await conn.execute(query)

    count = cast(int | None, await conn.fetchval(f"SELECT count(*) from {table.tmp_name}"))
    if count is None:
        raise NotImplementedError("Got empty table")

    logs.debug(f"Table {table.tmp_name!r} has {count} rows, expected {table_size}")
    match count:
        case count if count == table_size:
            pass
        case count if count < table_size:
            query = get_insert_data_query(table)
            _limit = table_size - count
            logs.debug(f"Got {count} < {table_size}, inserting data: {query.replace('$1', str(_limit))}")
            await conn.execute(query, _limit)
        case count if count > table_size:
            logs.warning(f"Sample size cannot be reached (got {count}, expected {table_size})")
        case _:
            raise NotImplementedError(f"Got invalid count: {count} (table_size: {table_size})")


async def process_table(table: Table, conn: asyncpg.Connection) -> set[Table]:
    """ """
    if table.has_been_processed:
        raise ValueError(f"Table {table.full_name!r} has already been processed")

    if table.sample_size is None:
        raise ValueError(f"Got empty sample_size for {table.full_name!r}")

    table_size = floor(table.count * table.sample_size / 100)
    logs.debug(
        f"Processing table: {table.full_name} (is_leaf: {table.is_leaf}, "
        f"sample_size: {table.sample_size}, "
        f"table.count: {table.count}, "
        f"table_size: {table_size}"
        ")"
    )
    if table.is_leaf:
        logs.debug(f"\tInserting leaf table {table.full_name}")
        await _insert_leaf_table(conn, table, table_size)
    else:
        if table.children_has_been_processed:
            logs.debug(f"\tInserting node table {table.full_name}")
            await _insert_node_table(conn, table, table_size)
        else:
            logs.debug("\tWaiting for children to be processed")
            return {_child_table for _child_table in table.child_tables_safe if not _child_table.has_been_processed}

    table.has_been_processed = True

    return {_parent_table for _parent_table in table.parent_tables_safe if not _parent_table.has_been_processed}


@contextlib.asynccontextmanager
async def disable_trigger(conn: asyncpg.Connection, *, active: bool = True):
    if not active:
        yield
        return

    await conn.execute("SET session_replication_role = 'replica'")
    try:
        yield
    finally:
        await conn.execute("SET session_replication_role = 'origin'")


async def create_temp_tables(
    conn: asyncpg.Connection,
    tables: list[Table],
    *,
    start_from: Literal["node", "leaf"] = "node",
):
    """
    Creates temporary tables with a sample of the original table.
    The default strategy for the sampling is to start from root tables
    and insert a sample of the data from the child tables.
    """

    # We start from the nodes
    _tables = set(
        table for table in tables if (table.is_root if start_from == "node" else table.is_leaf) and not table.ignore
    )

    if not _tables:
        raise NotImplementedError("No node table found")

    while len(_tables) > 0:
        _parent_tables = set()

        for table in _tables:
            if table.has_been_processed:
                continue

            _parent_tables = _parent_tables | await process_table(table, conn)

        # To avoid infinite loop
        if _tables == _parent_tables:
            _not_processed = [_table for _table in tables if not _table.children_has_been_processed]

            for _table in _not_processed:
                logs.error(_table.full_name)
                for c in _table.child_tables:
                    logs.error(f"\t {c.full_name} ({c.has_been_processed})")
            raise ValueError(
                "Cyclic foreign keys detected. "
                f"Possible tables are: {', '.join(x.full_name for x in _not_processed)}. "
                "Run `analyze` with `--show-graphs` to debug."
            )

        _tables = _parent_tables


async def sample_database(
    conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    db: Database,
    *,
    show_progress: bool = False,
    chunk_size: int = 5_000,
    # Deactivate triggers on reinserting to new database
    no_trigger: bool = True,
):
    """
    From top table to bottom
    """

    # getting the leaves
    has_ext = await check_extension_exists(conn, "tsm_system_rows")
    if not has_ext:
        await conn.execute("CREATE EXTENSION tsm_system_rows")

    async with conn.transaction():
        logs.info("Creating temporary tables")
        await create_temp_tables(conn, db.tables)

        # Safety check
        _not_processed_tables = [x.full_name for x in db.tables if not x.has_been_processed]
        if _not_processed_tables:
            raise NotImplementedError(
                f"Found {len(_not_processed_tables)} tables that has not been "
                f"processed: {', '.join(_not_processed_tables)}"
            )
        #
        _table_count: dict[str, int] = {}
        if show_progress:
            logs.info("Loading tables count")
            for table in db.tables:
                _count = await conn.fetchval(f"SELECT count(*) from {table.tmp_name}")
                if _count is None:
                    raise ValueError("Got empty count")
                _table_count[table.tmp_name] = _count

        logs.info("Done creating temporary tables, inserting to new database.")
        async with disable_trigger(target_conn, active=no_trigger):
            with Progress(disable=not show_progress) as progress:
                task1 = progress.add_task("[green]Inserting tables....", total=len(db.tables))
                task2 = progress.add_task("[purple]Inserting chunks....") if show_progress else None
                for table in db.tables:
                    logs.debug(f"Inserting to {table.full_name}")
                    if task2 is not None:
                        progress.reset(task2, total=_table_count[table.tmp_name])
                    query = f"SELECT {table.values} from {table.tmp_name}"

                    async for chunk in iterate_pg(conn, query, chunk_size=chunk_size):
                        logs.debug(f"{table.full_name} ({len(chunk)})")
                        await insert_many(
                            target_conn,
                            table.full_name,
                            [dict(x) for x in chunk],
                            on_conflict="ON CONFLICT DO NOTHING",
                            quote_columns=True,
                        )
                        if task2 is not None:
                            progress.update(task2, advance=chunk_size)
                    progress.update(task1, advance=1)
