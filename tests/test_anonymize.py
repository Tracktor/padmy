import textwrap

import pytest

from .utils import fetch_all, exec_req, insert_many


@pytest.mark.parametrize('table,pks,fields,field_types,expected', [
    ('public.test', ['id1'], ['field_1'],
     {'id1': 'text', 'field_1': 'integer'},
     """
    UPDATE public.test as u
    SET
      field_1 = u2.field_1
    from (values
      ($1::text, $2::integer)
    ) as u2(id1, field_1)
    where u2.id1 = u.id1
    """
     ),
    ('public.test', ['id1', 'id2'], ['field_1', 'field_2'],
     {'id1': 'text', 'id2': 'text', 'field_1': 'integer', 'field_2': 'text'},
     """
    UPDATE public.test as u
    SET
      field_1 = u2.field_1, field_2 = u2.field_2
    from (values
      ($1::text, $2::text, $3::integer, $4::text)
    ) as u2(id1, id2, field_1, field_2)
    where u2.id1 = u.id1 and u2.id2 = u.id2
    """
     )
])
def test_get_update_query(table, pks, fields, field_types, expected):
    from padmy.anonymize.anonymize import get_update_query

    query = get_update_query(table, pks, fields, field_types)
    assert textwrap.dedent(query).strip() == textwrap.dedent(expected).strip(), print(query)


@pytest.fixture()
def add_table_1_data(engine):
    data = [
        {'id': 1, 'foo': 'foo_1'},
        {'id': 2, 'foo': 'foo_2'}
    ]
    exec_req(engine, 'TRUNCATE public.table_1 CASCADE')
    insert_many('public.table_1', data, engine)
    yield
    exec_req(engine, 'TRUNCATE public.table_1 CASCADE')


@pytest.mark.usefixtures('add_table_1_data')
def test_anonymize_table(aengine, loop, engine, faker):
    from padmy.anonymize.anonymize import anonymize_table
    from padmy.config import ConfigTable, AnoFields

    table = ConfigTable('public', 'table_1',
                        fields=[AnoFields.load({'foo': 'EMAIL'})])

    async def test():
        await anonymize_table(aengine, table, ['id'], faker)

    loop.run_until_complete(test())

    db = fetch_all(engine, 'SELECT * from public.table_1 order by id')

    assert db == [
        {'id': 1, 'foo': 'achang@example.org'},
        {'id': 2, 'foo': 'tammy76@example.com'}
    ]


@pytest.mark.usefixtures('add_table_1_data')
def test_anonymize_db(apool, engine, loop, faker):
    from padmy.anonymize import anonymize_db
    from padmy.config import Config, ConfigTable, AnoFields

    config = Config(tables=[
        ConfigTable('public', 'table_1',
                    fields=[AnoFields(column='foo',
                                      type='EMAIL',
                                      extra_args={'domain': 'my-domain.fr'})
                            ])
    ])

    async def test():
        await anonymize_db(apool, config, faker)

    loop.run_until_complete(test())

    db = fetch_all(engine, 'SELECT foo, id from public.table_1 order by id')

    assert db == [
        {'foo': 'achang@my-domain.fr', 'id': 1},
        {'foo': 'greenwilliam@my-domain.fr', 'id': 2}
    ]
