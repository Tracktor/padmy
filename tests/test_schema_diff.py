import pytest
import psycopg

from .conftest import PG_URL
from .utils import create_db

FROM_DB = "from_db"
TO_DB = "to_db"


@pytest.fixture()
def setup_dbs():
    create_db(PG_URL, FROM_DB, drop_first=True)
    create_db(PG_URL, TO_DB, drop_first=True)

    yield


@pytest.fixture()
def engine_from():
    conn = psycopg.connect(f"{PG_URL}/{FROM_DB}")
    yield conn
    conn.close()


@pytest.fixture()
def engine_to():
    conn = psycopg.connect(f"{PG_URL}/{TO_DB}")
    yield conn
    conn.close()


@pytest.fixture()
def no_diffs(engine_from, engine_to):
    query = """
    DROP SCHEMA IF EXISTS schema1, schema2 CASCADE;
    CREATE SCHEMA schema1;
    CREATE SCHEMA schema2;
    CREATE TABLE IF NOT EXISTS schema1.table_1
    (
        id  SERIAL PRIMARY KEY,
        foo TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS schema2.table_1
    (
        id  SERIAL PRIMARY KEY,
        bar INT NOT NULL
    );
    """
    for _engine in (engine_from, engine_to):
        _engine.execute(query)
        _engine.commit()

    yield

    for _engine in (engine_from, engine_to):
        _engine.execute("DROP SCHEMA schema1, schema2 CASCADE")
        _engine.commit()


@pytest.fixture()
def diffs(no_diffs, engine_to):
    query = """
    ALTER TABLE schema1.table_1 ADD COLUMN baz TEXT;
    """
    engine_to.execute(query)
    engine_to.commit()
    yield


@pytest.mark.parametrize(
    "patch_scenario, expected_diff",
    [
        pytest.param("no_diffs", None, id="no_diffs"),
        pytest.param(
            "diffs",
            [
                "--- from_db-from.sql\n",
                "+++ to_db-to.sql\n",
                "@@ -44,7 +44,8 @@\n",
                " CREATE TABLE schema1.table_1 (",
                "     id integer NOT NULL,",
                "-    foo text NOT NULL",
                "+    foo text NOT NULL,",
                "+    baz text",
                " );",
            ],
            id="diffs",
        ),
    ],
)
@pytest.mark.usefixtures("setup_dbs")
def test_schema_differences(tmp_path, request, patch_scenario, expected_diff):
    request.getfixturevalue(patch_scenario)
    from padmy.compare import compare_databases

    diff = compare_databases(
        PG_URL, database=FROM_DB, schemas=["schema1", "schema2"], dump_dir=tmp_path, db_to=TO_DB, no_privileges=True
    )
    if expected_diff is None:
        assert diff is None
    else:
        _diff = [line for line in (diff or []) if line.strip()]
        assert _diff == expected_diff
