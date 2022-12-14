import asyncio
import functools
from dataclasses import dataclass, field

import asyncpg
from rich.console import Console
from rich.table import Table as RTable
from typing_extensions import Self

from padmy.config import Config, SampleType
from padmy.utils import get_first, get_conn


# if sys.version_info.minor < 11 and sys.version_info.major >= 3:
# else:
#     from typing import Self


def _get_full_name(schema: str | None, table: str | None) -> str:
    if schema is None or table is None:
        raise ValueError('schema and table must not be empty')
    return f'{schema}.{table}'


@dataclass
class FKConstraint:
    column_name: str
    constraint_name: str

    # references
    foreign_schema: str
    foreign_table: str
    foreign_column_name: str

    table: str | None = None
    schema: str | None = None

    @property
    def foreign_full_name(self):
        return _get_full_name(self.foreign_schema, self.foreign_table)

    @property
    def full_name(self):
        return _get_full_name(self.schema, self.table)


@dataclass
class PKConstraint:
    column_name: str
    table: str
    schema: str

    @property
    def full_name(self):
        return _get_full_name(self.schema, self.table)


@dataclass(eq=False)
class Table:
    schema: str
    table: str

    _count: int | None = None  # field(init=False)

    foreign_keys: list[FKConstraint] = field(default_factory=list)
    primary_keys: list[PKConstraint] = field(default_factory=list)

    parent_tables: set[Self] = field(default_factory=set)
    child_tables: set[Self] = field(default_factory=set)

    # Has already been sampled and it's temporary table has been created
    has_been_processed: bool = False

    # Sample size
    sample_size: int | None = None

    @property
    def parent_tables_safe(self):
        # return self.parent_tables - {self}
        return {x for x in self.parent_tables if x.full_name != self.full_name}

    @property
    def child_tables_safe(self):
        # return self.child_tables - {self}
        return {x for x in self.child_tables if x.full_name != self.full_name}

    @property
    def full_name(self):
        return _get_full_name(self.schema, self.table)

    @property
    def tmp_name(self):
        return f'_{self.schema}_{self.table}_tmp'

    @property
    def has_parent(self):
        return len(self.parent_tables_safe) > 0

    @property
    def has_children(self):
        return len(self.child_tables_safe) > 0

    @property
    def children_has_been_processed(self):
        return all(_child.has_been_processed for _child in self.child_tables_safe)

    @property
    def count(self) -> int:
        if self._count is None:
            raise ValueError('Count must be loaded first')
        return self._count

    @count.setter
    def count(self, v: int):
        self._count = v

    async def load_count(self, conn: asyncpg.Connection):
        self._count = await conn.fetchval(f'SELECT count(*) from {self.full_name}')

    def __eq__(self, other: Self):
        for k in ['full_name', 'has_been_processed']:
            if getattr(self, k) != getattr(other, k):
                return False
        return True

    def __hash__(self):
        return hash((getattr(self, x) for x in ['full_name']))

    def __repr__(self):
        return f'Table(full_name={self.full_name!r} ' \
               f'count={self._count} ' \
               f'foreign_keys={len(self.foreign_keys)} ' \
               f'parents={len(self.parent_tables)} ' \
               f'children={len(self.child_tables)}' \
               f')'

    def __post_init__(self):
        for fk in self.foreign_keys:
            fk.table = self.table
            fk.schema = self.schema


async def get_tables(conn: asyncpg.Connection, schemas: list[str]):
    query = """
    select 
    table_schema as schema, table_name as table
    from information_schema.tables
    where table_schema = ANY ($1::text[]) and
    table_type = 'BASE TABLE'
    """
    data = await conn.fetch(query, schemas)
    return [Table(**x) for x in data]


SCHEMA_FK_QUERY = """
SELECT
    tc.table_schema as schema,
    tc.constraint_name,
    tc.table_name as table,
    kcu.column_name,
    ccu.table_schema AS foreign_schema,
    ccu.table_name AS foreign_table,
    ccu.column_name AS foreign_column_name
FROM
    information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = ANY ($1::text[]);
"""

SCHEMA_PK_QUERY = """
SELECT 
    tc.table_schema as schema, 
    tc.table_name as table, 
    c.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
WHERE constraint_type = 'PRIMARY KEY' and tc.table_schema = ANY ($1::text[]);
"""


async def load_foreign_keys(conn: asyncpg.Connection, schemas: list[str]):
    data = await conn.fetch(SCHEMA_FK_QUERY, schemas)
    return [FKConstraint(**x) for x in data]


async def load_primary_keys(conn: asyncpg.Connection, schemas: list[str]):
    data = await conn.fetch(SCHEMA_PK_QUERY, schemas)
    return [PKConstraint(**x) for x in data]


GET_COLUMNS_TYPE_QUERY = """
select 
    column_name, data_type 
from information_schema.columns 
where table_schema = $1 and 
      table_name = $2 and 
      column_name = ANY ($3::text[])
"""


async def load_columns_type(conn: asyncpg.Connection, schema: str,
                            table: str,
                            columns: list[str]):
    data = await conn.fetch(GET_COLUMNS_TYPE_QUERY, schema,
                            table, columns)
    return functools.reduce(lambda p, n: {**p, **{n['column_name']: n['data_type']}},
                            data, {})


@dataclass
class Database:
    name: str
    tables: list[Table] = field(default_factory=list)

    async def explore(self, pool: asyncpg.Pool, schemas: list[str], *, load_count: bool = True):
        async with pool.acquire() as conn:
            self.tables = await get_tables(conn, schemas)
            fks = await load_foreign_keys(conn, schemas)
            pks = await load_primary_keys(conn, schemas)

        _tables: dict[str, Table] = {_table.full_name: _table for _table in self.tables}

        for _pk in pks:
            _tables[_pk.full_name].primary_keys.append(_pk)

        for _fk in fks:
            _tables[_fk.full_name].foreign_keys.append(_fk)
            _tables[_fk.full_name].parent_tables.add(_tables[_fk.foreign_full_name])
            _tables[_fk.foreign_full_name].child_tables.add(_tables[_fk.full_name])

        if load_count:
            await asyncio.gather(*[
                get_conn(pool, table.load_count) for table in self.tables
            ])

    def load_config(self, config: Config):
        """
        Loads the sample sizes for each tables from the config file.
        Tables need to have been loaded first
        """
        _schemas: dict[str, SampleType | None] = {schema.schema: schema.sample for schema in config.schemas}
        _tables: dict[str, SampleType | None] = {f'{_table.schema}.{_table.table}': _table.sample for _table in
                                                 config.tables}
        for _table in self.tables:
            _schema_sample = _schemas.get(_table.schema)
            _table_sample = _tables.get(_table.full_name)
            _sample = get_first(_table_sample,
                                _schema_sample,
                                config.sample,
                                fn=lambda x: x is not None)
            if _sample is None:
                raise ValueError('_sample must not be empty')
            _table.sample_size = int(_sample)


def pretty_print_stats(database: Database):
    table = RTable(title=f"Stats for {database.name}")

    table.add_column("Table", justify="left", style="cyan")
    table.add_column("Count", justify="right", style="green")
    table.add_column("# FKs", justify="right", style="magenta")
    table.add_column("# Parents", justify="right", style="magenta")
    table.add_column("# Children", justify="right", style="magenta")

    for _table in sorted(database.tables, key=lambda x: x.full_name):
        table.add_row(_table.full_name, str(_table.count),
                      str(len(_table.foreign_keys)),
                      str(len(_table.parent_tables)),
                      str(len(_table.child_tables))
                      )

    console = Console()
    console.print(table)


def pprint_compared_dbs(db_1: Database, db_2: Database):
    table = RTable(title=f"Comparing {db_1.name} and {db_2.name}")

    table.add_column("Table", justify="left", style="blue")
    table.add_column(f"Count {db_1.name!r}", justify="right", style="cyan")
    table.add_column(f"Count {db_2.name!r}", justify="right", style="cyan")
    table.add_column("Diff", justify="right", style="green")

    tables_1, tables_2 = sorted(db_1.tables, key=lambda x: x.full_name), sorted(db_2.tables, key=lambda x: x.full_name)

    for _table1, _table2 in zip(tables_1, tables_2):
        perc_diff = 100 if _table2.count == 0 else int(_table2.count * 100 / _table1.count)
        if perc_diff > 0:
            perc_sign = '+'
        elif perc_diff < 0:
            perc_sign = '-'
        else:
            perc_sign = ''
        table.add_row(_table1.full_name,
                      str(_table1.count),
                      str(_table2.count),
                      f'{perc_diff}%')

    console = Console()
    console.print(table)
