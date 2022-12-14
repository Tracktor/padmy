import contextlib
import logging
import tempfile
from math import floor

import asyncpg

from ..config import Config
from ..db import Database, Table, FKConstraint
from ..logs import logs
from ..utils import (
    check_cmd, pg_restore, pg_dump, create_db, drop_db, exec_psql, iterate_pg,
    check_extension_exists, check_tmp_table_exists,
    insert_many
)


def copy_database(database: str,
                  target_db: str,
                  schemas: list[str],
                  drop_public: bool = False):
    """
    Dumps and recreate the schemas from __repr__`database` into `target_db`.
    You can optionally specify `drop_public` to drop the public schema
    before restoring and avoid ' ERROR:  schema "public" already exists'
    (useful if you have *public* specified in your schemas list)
    """
    for cmd in ['pg_dump', 'createdb', 'dropdb', 'pg_restore', 'psql']:
        check_cmd(cmd)

    with tempfile.NamedTemporaryFile(suffix='.dump') as tmp_file:
        pg_dump(schemas=schemas,
                dump_path=tmp_file.name,
                database=database,
                options=['-Fc', '--schema-only'])
        drop_db(target_db, if_exists=True)
        create_db(database=target_db)

        if 'public' in schemas or drop_public:
            exec_psql(target_db, 'DROP SCHEMA public;')

        pg_restore(dump_path=tmp_file.name,
                   database=target_db)


def get_insert_child_fk_data_query(table: Table, child_table: Table) -> str:
    """
    Gets the query
    """
    joins = []

    def _fk_join(tmp_name: str, fk: FKConstraint, fk_index: int) -> str:
        return f'inner join {tmp_name} _s{fk_index} on _s{fk_index}.{fk.column_name} = t.{fk.foreign_column_name}'

    for i, _fk in enumerate(child_table.foreign_keys):
        if _fk.foreign_full_name != table.full_name:
            continue
        joins.append(_fk_join(child_table.tmp_name, _fk, i))

    query = f"""
    INSERT INTO {table.tmp_name}
    SELECT * from {table.full_name} t
    """ + '\n'.join(joins) + '\n ON CONFLICT DO NOTHING'

    return query


def get_insert_data_query(table: Table):
    where_str = 'and '.join(f't2.{_pk.column_name} = t1.{_pk.column_name}' for _pk in table.primary_keys)
    query = f"""
    INSERT into {table.tmp_name}
    SELECT * from {table.full_name} t1
    where not exists(select null from {table.tmp_name} t2 where {where_str})
    limit $1
    """
    return query


async def _insert_leaf_table(conn: asyncpg.Connection, table: Table, table_size: int):
    query = f"CREATE TEMP TABLE {table.tmp_name} ON COMMIT DROP AS SELECT * from {table.full_name}"
    args = []
    if table_size > 0:
        query = f"{query} TABLESAMPLE SYSTEM_ROWS($1)"
        args.append(table_size)
    logs.debug(f'{query} {args}')
    await conn.execute(query, *args)


async def _insert_node_table(conn: asyncpg.Connection,
                             table: Table,
                             table_size: int):
    # Creating the table
    query = f"CREATE TEMP TABLE {table.tmp_name} (LIKE {table.full_name} INCLUDING ALL)  ON COMMIT DROP"
    logs.debug(query)
    await conn.execute(query)

    # Inserting data from child table
    for _child_table in table.child_tables:
        table_exists = await check_tmp_table_exists(conn, _child_table.full_name)
        if not table_exists:
            continue
        query = get_insert_child_fk_data_query(table, _child_table)
        logs.debug(query)
        await conn.execute(query)

    count: int | None = await conn.fetchval(f'SELECT count(*) from {table.tmp_name}')
    if count is None:
        raise NotImplementedError('Got empty table')

    match count:
        case count if count == table_size:
            pass
        case count if count < table_size:
            query = get_insert_data_query(table)
            _limit = table_size - count
            logs.debug(f'{query} limit: {_limit}')
            await conn.execute(query, _limit)
        case count if count > table_size:
            logs.warning(f'Sample size cannot be reached (got {count}, expected {table_size})')
        case _:
            raise NotImplementedError(f'Got invalid count: {count} (table_size: {table_size})')


async def process_table(table: Table, conn: asyncpg.Connection) -> set[Table]:
    if table.has_been_processed:
        raise ValueError(f'table {table.full_name!r} has already been processed')

    is_leaf = not table.has_children

    if table.sample_size is None:
        raise ValueError(f'Got empty sample_size for {table.full_name!r}')

    table_size = floor(table.count * table.sample_size / 100)
    if is_leaf:
        logs.info(f'\t Inserting {table.full_name}')
        await _insert_leaf_table(conn, table, table_size)
    else:
        if table.children_has_been_processed:
            logs.info(f'\t Inserting {table.full_name}')
            await _insert_node_table(conn, table, table_size)
        else:
            return {_child_table for _child_table in table.child_tables_safe
                    if not _child_table.has_been_processed
                    }

    if logs.level == logging.DEBUG:
        c = await conn.fetchval(f'SELECT count(*) from {table.tmp_name}')
        logs.debug(f'{table.tmp_name}: {c}')

    table.has_been_processed = True

    return {_parent_table for _parent_table
            in table.parent_tables_safe
            if not _parent_table.has_been_processed}


@contextlib.asynccontextmanager
async def disable_trigger(conn: asyncpg.Connection):
    await conn.execute("SET session_replication_role = 'replica'")
    try:
        yield
    finally:
        await conn.execute("SET session_replication_role = 'origin'")


async def create_temp_tables(conn: asyncpg.Connection,
                             tables: list[Table]):
    """
    Creates temporary tables with a sample of the original table
    """

    # We start from the leaves
    _tables = set(table for table in tables if not table.has_children)

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
                    logs.error(f'\t {c.full_name} ({c.has_been_processed})')
            raise ValueError('Cyclic foreign keys detected. '
                             f'Possible tables are: {", ".join(x.full_name for x in _not_processed)}. '
                             'Run `analyze` with `--show-graphs` to debug.')

        _tables = _parent_tables


async def sample_database(conn: asyncpg.Connection,
                          target_conn: asyncpg.Connection,
                          config: Config,
                          db: Database,
                          *,
                          chunk_size: int = 1_000):
    """
    From top table to bottom
    """

    # getting the leaves
    has_ext = await check_extension_exists(conn, 'tsm_system_rows')
    if not has_ext:
        await conn.execute('CREATE EXTENSION tsm_system_rows')

    async with conn.transaction():
        await create_temp_tables(conn, db.tables)

        # Safety check
        _not_processed_tables = [x.full_name for x in db.tables if not x.has_been_processed]
        if _not_processed_tables:
            raise NotImplementedError(f'Found {len(_not_processed_tables)} tables that has not been'
                                      f'processed: {",".join(_not_processed_tables)}')
        #
        if logs.level == logging.DEBUG:
            for table in db.tables:
                c = await conn.fetchval(f'SELECT count(*) from {table.tmp_name}')
                logs.debug(f'{table.tmp_name}: {c}')

        logs.info('Done creating temporary tables, inserting to new database')

        async with disable_trigger(target_conn):
            for table in db.tables:
                logs.info(f'Inserting to {table.full_name}')
                query = f'SELECT * from {table.tmp_name}'
                async for chunk in iterate_pg(conn, query, chunk_size=chunk_size):
                    await insert_many(target_conn, table.full_name, chunk)
