def get_table_no_constraints():
    from padmy.db import Table
    table = Table(schema='public', table='table_1')
    table.count = 10
    return table


def get_table_one_constraint():
    from padmy.db import Table, FKConstraint
    fks = [
        FKConstraint(column_name='table_1_id', constraint_name='t2_t1_id',
                     foreign_schema='public', foreign_table='table_1',
                     foreign_column_name='id')
    ]
    table = Table(schema='public', table='table_2',
                  foreign_keys=fks)
    table.count = 10
    return table


def get_table_multiple_constraints():
    from padmy.db import Table, FKConstraint
    table_fks = [
        FKConstraint(column_name='table_1_id', constraint_name='t3_t1_id',
                     foreign_schema='public', foreign_table='table_1',
                     foreign_column_name='id'),
        FKConstraint(column_name='table_2_id', constraint_name='t3_t2_id',
                     foreign_schema='public', foreign_table='table_2',
                     foreign_column_name='id')
    ]
    table = Table(schema='public', table='table_3', foreign_keys=table_fks)

    return table


def test_sample_db_circular_single(loop, aengine):
    from padmy.sampling.sampling import create_temp_tables
    from padmy.db import Table, FKConstraint
    from padmy.utils import check_tmp_table_exists

    fks = [
        FKConstraint(column_name='parent_id',
                     constraint_name='test',
                     foreign_schema='public', foreign_table='single_circular',
                     foreign_column_name='id')
    ]
    table = Table(schema='public', table='single_circular',
                  foreign_keys=fks,
                  sample_size=20)

    async def test():
        await table.load_count(aengine)
        async with aengine.transaction():
            await create_temp_tables(aengine, [table])
            tmp_exists = await check_tmp_table_exists(aengine, table.tmp_name)
            assert tmp_exists

    loop.run_until_complete(test())


def test_sample_db_circular_mutiple(loop, aengine):
    from padmy.sampling.sampling import create_temp_tables
    from padmy.db import Table, FKConstraint
    from padmy.utils import check_tmp_table_exists

    table1 = Table(schema='public', table='single_circular',
                   foreign_keys=[
                       FKConstraint(column_name='parent_id',
                                    constraint_name='test',
                                    foreign_schema='public',
                                    foreign_table='single_circular',
                                    foreign_column_name='id')
                   ],
                   sample_size=20)
    table1.count = 1
    table2 = Table(schema='public', table='single_circular',
                   foreign_keys=[
                       FKConstraint(column_name='multiple_circular_id',
                                    constraint_name='test2',
                                    foreign_schema='public',
                                    foreign_table='multiple_circular',
                                    foreign_column_name='id')
                   ],
                   sample_size=20)

    table2.count = 1

    tables = [table1, table2]

    async def test():
        async with aengine.transaction():
            await create_temp_tables(aengine, tables)
            for table in tables:
                tmp_exists = await check_tmp_table_exists(aengine, table.tmp_name)
                assert tmp_exists

    loop.run_until_complete(test())

# @pytest.mark.parametrize('table, child_table, expected', [
#     (
#             get_table_no_constraints(),
#             get_table_no_constraints(),
#             "SELECT t.* from _public_table_1_tmp t"
#     ),
# ])
# def test_get_insert_child_fk_data_query(table, child_table, expected):
#     from padmy.sampling import get_insert_child_fk_data_query
#
#     query = get_insert_child_fk_data_query(table, child_table)
#     print(query)
#     assert query == textwrap.dedent(expected).strip()
