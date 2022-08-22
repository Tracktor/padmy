import pytest

from .utils import exec_req, pprint_dataclass_diff


@pytest.fixture()
def setup_tmp_schema(engine):
    exec_req(engine, 'DROP SCHEMA IF EXISTS tmp_test CASCADE')
    exec_req(engine, 'CREATE SCHEMA tmp_test')
    yield
    exec_req(engine, 'DROP SCHEMA tmp_test CASCADE')


NO_CONSTRAINTS_QUERY = """
CREATE TABLE IF NOT EXISTS tmp_test.table_1
(
    id  SERIAL PRIMARY KEY,
    foo TEXT
);

CREATE TABLE IF NOT EXISTS tmp_test.table_2
(
    id           SERIAL PRIMARY KEY,
    bar   TEXT
);
"""

ONE_CONSTRAINT_QUERY = """
CREATE TABLE IF NOT EXISTS tmp_test.table_1
(
    id  SERIAL PRIMARY KEY,
    foo TEXT
);

CREATE TABLE IF NOT EXISTS tmp_test.table_2
(
    id           SERIAL PRIMARY KEY,
    table_1_id   int references tmp_test.table_1
);
"""

MULTIPLE_CONSTRAINTS_QUERY = """
CREATE TABLE IF NOT EXISTS tmp_test.table_1
(
    id  SERIAL PRIMARY KEY,
    foo TEXT
);

CREATE TABLE IF NOT EXISTS tmp_test.table_2
(
    id           SERIAL PRIMARY KEY,
    table_1_id   int references tmp_test.table_1
);

CREATE TABLE IF NOT EXISTS tmp_test.table_3
(
    id           SERIAL PRIMARY KEY,
    table_1_id   int references tmp_test.table_1,
    table_2_id   int references tmp_test.table_2
);
"""

DEFAULT = {
    'schema': 'tmp_test',
    'foreign_schema': 'tmp_test',
    'foreign_column_name': 'id',
    'column_name': 'table_1_id'
}

_FKS_PARAMS = [
    ('No table', None, []),
    ('No constraints', NO_CONSTRAINTS_QUERY, []),
    ('One constraint', ONE_CONSTRAINT_QUERY, [
        {**DEFAULT, 'table': 'table_2', 'foreign_table': 'table_1', 'constraint_name': 'table_2_table_1_id_fkey'}
    ]),
    ('Multiple constraints', MULTIPLE_CONSTRAINTS_QUERY, [
        {**DEFAULT, 'table': 'table_2', 'foreign_table': 'table_1', 'constraint_name': 'table_2_table_1_id_fkey'},
        {**DEFAULT, 'table': 'table_3', 'foreign_table': 'table_1', 'constraint_name': 'table_3_table_1_id_fkey'},
        {**DEFAULT, 'table': 'table_3', 'foreign_table': 'table_2', 'constraint_name': 'table_3_table_2_id_fkey',
         'column_name': 'table_2_id'}
    ])

]


@pytest.mark.usefixtures('setup_tmp_schema')
@pytest.mark.parametrize('name, query, expected', _FKS_PARAMS, ids=[x[0] for x in _FKS_PARAMS])
def test_load_fk_constraints(name, query, expected, aengine, engine, loop):
    from padmy.db import FKConstraint, load_foreign_keys
    if query:
        exec_req(engine, query)

    fks = loop.run_until_complete(load_foreign_keys(aengine, ['tmp_test']))
    expected_fks = [FKConstraint(**x) for x in expected]
    assert fks == expected_fks, pprint_dataclass_diff(fks, expected_fks)


ONE_PK_TABLE = """
CREATE TABLE IF NOT EXISTS tmp_test.table_1
(
    id  SERIAL PRIMARY KEY,
    foo TEXT
);
"""

MULTIPLE_PK_TABLE = """
CREATE TABLE IF NOT EXISTS tmp_test.table_1
(
    id_1  INT,
    id_2  TEXT,
    bar   TEXT,
    PRIMARY KEY (id_1, id_2)
);
"""

DEFAULT_PK_CONSTRAINT = {
    'table': 'table_1',
    'schema': 'tmp_test'
}

_PKS_PARAMS = [
    ('No table', None, []),
    ('One Primary key', ONE_PK_TABLE, [{**DEFAULT_PK_CONSTRAINT, 'column_name': 'id'}]),
    ('Multiple primary keys', MULTIPLE_PK_TABLE, [
        {**DEFAULT_PK_CONSTRAINT, 'column_name': 'id_1'},
        {**DEFAULT_PK_CONSTRAINT, 'column_name': 'id_2'}
    ])
]


@pytest.mark.usefixtures('setup_tmp_schema')
@pytest.mark.parametrize('name, table, expected', _PKS_PARAMS,
                         ids=[x[0] for x in _PKS_PARAMS])
def test_load_pk_constraints(name, table, expected, aengine, engine, loop):
    from padmy.db import PKConstraint, load_primary_keys
    if table:
        exec_req(engine, table)

    pks = loop.run_until_complete(load_primary_keys(aengine, ['tmp_test']))
    expected_pks = [PKConstraint(**x) for x in expected]
    assert pks == expected_pks, pprint_dataclass_diff(pks, expected_pks)


def test_explore(loop, apool):
    from padmy.db import Database

    db = Database(name='test')

    async def test():
        await db.explore(apool, ['public'])

    loop.run_until_complete(test())
    assert len(db.tables) == 7
    table_1 = [x for x in db.tables if x.full_name == 'public.table_1'][0]
    assert len(table_1.child_tables) == 2
    assert len(table_1.child_tables_safe) == 2
    assert table_1.has_children
    assert not table_1.children_has_been_processed
    # Parents
    assert not table_1.has_parent
    assert len(table_1.parent_tables) == 0
    assert len(table_1.parent_tables_safe) == 0

    single_circular = [x for x in db.tables if x.full_name == 'public.single_circular'][0]

    assert len(single_circular.child_tables) == 1
    assert len(single_circular.child_tables_safe) == 0
    assert not single_circular.has_children


def test_table_hash():
    from padmy.db import Table

    t2 = Table('public', 'table_2')
    t3 = Table('public', 'table_3')
    t1 = Table('public', 'table_1')
    t1.child_tables = {t1, t2, t3}

    z = {t1, t2, t3} - {t1}

    assert z == {t2, t3}
    assert t1.child_tables_safe == {t2, t3}
