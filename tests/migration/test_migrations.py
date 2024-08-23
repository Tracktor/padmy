import logging
import re
from contextlib import nullcontext

import psycopg
import pytest

from unittest.mock import Mock
from .conftest import VALID_MIGRATIONS_DIR, INVALID_MIGRATIONS_DIR, INVALID_MIGRATIONS_DIR_MULTIPLE
from ..conftest import PG_DATABASE
from tracktolib.pg_sync import fetch_all, insert_one, insert_many
from ..utils import check_table_exists, check_column_exists
from padmy.migration.utils import parse_filename

TEST_EMAIL = "foo@baz.baz"


@pytest.fixture()
def setup(loop, aengine):
    from padmy.migration.migration import migrate_setup

    loop.run_until_complete(migrate_setup(aengine))


@pytest.mark.parametrize("version", [None, "0.0.0"])
def test_create_new_migration(monkeypatch, migration_dir, capsys, version):
    from padmy.migration import create_new_migration

    monkeypatch.setattr("padmy.migration.utils.get_git_email", lambda x: TEST_EMAIL)

    time_call_count = 0

    def mock_time():
        nonlocal time_call_count
        time_call_count += 1
        return time_call_count

    monkeypatch.setattr("uuid.uuid4", lambda: "0000000000")
    monkeypatch.setattr("time.time", mock_time)

    class PromptMock:
        @staticmethod
        def ask(*args, **kwargs):
            return TEST_EMAIL

    monkeypatch.setattr("padmy.migration.create_files.Prompt", PromptMock)
    create_new_migration(folder=migration_dir, version=version)
    new_files = list(migration_dir.glob("*.sql"))
    assert capsys.readouterr().out.strip().startswith("Creating new migration file")
    assert len(new_files) == 2
    down_file, up_file = sorted(new_files)
    up_content = [x.rstrip() for x in up_file.open().readlines() if x.strip()]

    _optional_args = [f"-- Version: {version}"] if version else []

    assert up_content == ["-- Prev-file:", f"-- Author: {TEST_EMAIL}"] + _optional_args
    down_content = [x.rstrip() for x in down_file.open().readlines() if x.strip()]
    assert down_content == ["-- Prev-file:", f"-- Author: {TEST_EMAIL}"] + _optional_args

    # Creating second migration

    create_new_migration(folder=migration_dir, version=version)
    new_files = list(migration_dir.glob("*.sql"))
    assert capsys.readouterr().out.strip().startswith("Creating new migration file")
    up_file2, down_file2 = sorted(new_files, key=lambda x: x.name, reverse=True)[:2]
    up_content = [x.rstrip() for x in up_file2.open().readlines() if x.strip()]
    assert up_content == [f"-- Prev-file: {up_file.name}", f"-- Author: {TEST_EMAIL}"] + _optional_args
    down_content = [x.rstrip() for x in down_file2.open().readlines() if x.strip()]
    assert down_content == [f"-- Prev-file: {down_file.name}", f"-- Author: {TEST_EMAIL}"] + _optional_args


@pytest.mark.usefixtures("clean_migration")
def test_migrate_setup(engine, loop, aengine):
    from padmy.migration import migrate_setup

    loop.run_until_complete(migrate_setup(aengine))
    assert check_table_exists(engine, "public", "migration")


@pytest.mark.usefixtures("setup_test_schema")
@pytest.mark.parametrize("only_last", [True, False])
def test_migrate_verify_valid(monkeypatch, engine, tmp_path, only_last):
    from padmy.migration import migrate_verify
    from padmy.migration.migration import MigrationError

    migrate_verify(
        database=PG_DATABASE,
        schemas=["general"],
        dump_dir=tmp_path,
        migration_folder=VALID_MIGRATIONS_DIR,
        only_last=only_last,
    )
    # Rerunning again for double check

    try:
        migrate_verify(
            database=PG_DATABASE,
            schemas=["general"],
            dump_dir=tmp_path,
            migration_folder=VALID_MIGRATIONS_DIR,
            only_last=only_last,
        )
    except MigrationError as e:
        print(e.diff)
        raise e


@pytest.mark.usefixtures("setup_test_schema")
@pytest.mark.parametrize("only_last", [False, True])
@pytest.mark.parametrize(
    "migration_dir, error_msg",
    [
        pytest.param(INVALID_MIGRATIONS_DIR, "Difference found for migration: 00000000", id="invalid"),
        pytest.param(
            INVALID_MIGRATIONS_DIR_MULTIPLE, "Difference found for migration: 00000001", id="multiple invalid"
        ),
    ],
)
def test_migrate_verify(monkeypatch, engine, tmp_path, only_last, migration_dir, error_msg):
    from padmy.migration import migrate_verify
    from padmy.migration.migration import MigrationError

    with pytest.raises(MigrationError, match=re.escape(error_msg)):
        migrate_verify(
            database=PG_DATABASE,
            schemas=["general"],
            dump_dir=tmp_path,
            migration_folder=migration_dir,
            only_last=only_last,
        )

    # TODO: flaky on CI
    # error_file = tmp_path / "error.diff"
    # with error_file.open("w") as f:
    #     f.write(e.value.diff + "\n")
    #
    # compare_files(
    #     error_file, INVALID_MIGRATIONS_DIR / "1-00000000.diff", ignore_order=True
    # )


SETUP_ERROR_MSG = re.escape(
    'Could not find table table "public.migration", ' 'did you forget to setup the table by running "migration setup" ?'
)


@pytest.mark.parametrize("migration_type", ["up", "down"])
@pytest.mark.usefixtures("clean_migration")
def test_migrate_no_setup(engine, monkeypatch, aengine, loop, migration_type):
    from padmy.migration.migration import migrate_up, NoSetupTableError, migrate_down

    fn = migrate_up if migration_type == "up" else migrate_down

    async def _test():
        with pytest.raises(NoSetupTableError, match=SETUP_ERROR_MSG):
            await fn(aengine, folder=VALID_MIGRATIONS_DIR)

    loop.run_until_complete(_test())


def _insert_migrations(engine: psycopg.Connection, nb_migrations: int):
    up_files = sorted(list(VALID_MIGRATIONS_DIR.glob("*up.sql")))
    if nb_migrations > (nb_max_migrations := len(up_files)):
        raise ValueError(f"Invalid number of migrations {nb_migrations} > {nb_max_migrations}")

    _data = [
        {
            "file_name": up_files[i].name,
            **parse_filename(up_files[i].name),
        }
        for i in range(nb_migrations)
    ]
    insert_many(engine, "migration", _data)


@pytest.mark.parametrize(
    "migration_type, setup_fn, nb_migrations,expected",
    [
        pytest.param("up", None, 1, ["00000000"], id="one migration up"),
        pytest.param("down", None, 1, [], id="one migration down with no applied migrations"),
        pytest.param(
            "down", lambda engine: _insert_migrations(engine, nb_migrations=1), 1, ["00000000"], id="one migration down"
        ),
        pytest.param("up", None, -1, ["00000000", "00000001"], id="multiple migrations up"),
        pytest.param(
            "up",
            lambda engine: _insert_migrations(engine, nb_migrations=1),
            1,
            ["00000001"],
            id="up with applied migrations",
        ),
        pytest.param(
            "up", lambda engine: _insert_migrations(engine, nb_migrations=2), 1, [], id="up with no more migrations"
        ),
        pytest.param(
            "up",
            lambda engine: (
                insert_one(
                    engine,
                    "migration",
                    {
                        "file_name": "2-00000001-up.sql",
                        **parse_filename("2-00000001-up.sql"),
                    },
                )
            ),
            1,
            [],
            id="up with missing intermediate migration",
        ),
        pytest.param(
            "down",
            lambda engine: _insert_migrations(engine, nb_migrations=1),
            2,
            ["00000000"],
            id="two rollback with 1 migration only",
        ),
    ],
)
@pytest.mark.usefixtures("clean_migration", "setup")
def test_migration_files(engine, aengine, loop, migration_type, setup_fn, nb_migrations, expected):
    if setup_fn is not None:
        setup_fn(engine)

    from padmy.migration.migration import get_migration_files, get_rollback_files

    fn = get_migration_files if migration_type == "up" else get_rollback_files

    async def _test():
        _files = await fn(aengine, folder=VALID_MIGRATIONS_DIR, nb_migrations=nb_migrations)
        return _files

    files = loop.run_until_complete(_test())
    assert [x.file_id for x in files] == expected


@pytest.mark.usefixtures("clean_migration", "setup_test_schema")
def test_migrate_up_down(engine, monkeypatch, caplog, aengine, loop):
    def _get_migrations():
        return fetch_all(engine, "SELECT * FROM public.migration order by applied_at desc")

    caplog.set_level(logging.INFO)
    from padmy.migration import migrate_up, migrate_setup, migrate_down

    # Setting up migration

    loop.run_until_complete(migrate_setup(aengine))
    assert not check_table_exists(engine, "general", "test")

    migrations = _get_migrations()
    assert len(migrations) == 0

    # 1rst migration
    loop.run_until_complete(migrate_up(aengine, folder=VALID_MIGRATIONS_DIR, nb_migrations=1, metadata={"foo": "bar"}))

    assert check_table_exists(engine, "general", "test")
    assert check_table_exists(engine, "general", "test2")
    assert not check_column_exists(engine, "general", "test", "baz")
    migrations = _get_migrations()
    assert len(migrations) == 1
    last_migration = migrations[0]
    assert last_migration["meta"] == {"foo": "bar"}
    assert last_migration.pop("applied_at")
    assert last_migration.pop("id") is not None
    assert last_migration["file_id"] == "00000000"
    assert last_migration["migration_type"] == "up"

    # 2nd migration

    loop.run_until_complete(migrate_up(aengine, folder=VALID_MIGRATIONS_DIR, nb_migrations=1))

    assert check_column_exists(engine, "general", "test", "baz")
    migrations = _get_migrations()
    assert len(migrations) == 2
    last_migration = migrations[0]
    assert last_migration["file_id"] == "00000001"
    assert last_migration["migration_type"] == "up"

    # 3rd migration
    has_applied_migration = loop.run_until_complete(migrate_up(aengine, folder=VALID_MIGRATIONS_DIR, nb_migrations=1))
    assert not has_applied_migration

    # Migrate down
    loop.run_until_complete(
        migrate_down(aengine, folder=VALID_MIGRATIONS_DIR, nb_migrations=1, metadata={"bar": "baz"})
    )
    assert check_table_exists(engine, "general", "test")
    assert check_table_exists(engine, "general", "test2")
    assert not check_table_exists(engine, "general", "baz")
    migrations = _get_migrations()
    assert len(migrations) == 3
    last_migration = migrations[0]
    assert last_migration["migration_type"] == "down"
    assert last_migration["file_id"] == "00000001"
    assert last_migration["meta"] == {"bar": "baz"}
    # Migrate down a second time
    loop.run_until_complete(migrate_down(aengine, folder=VALID_MIGRATIONS_DIR, nb_migrations=1))
    assert not check_table_exists(engine, "general", "test")
    assert not check_table_exists(engine, "general", "test2")
    migrations = _get_migrations()
    assert len(migrations) == 4
    last_migration = migrations[0]
    assert last_migration["file_id"] == "00000000", "Invalid file_id after second down migration"
    assert last_migration["migration_type"] == "down"

    # Migrate down no more rollback
    has_applied_migration = loop.run_until_complete(migrate_down(aengine, folder=VALID_MIGRATIONS_DIR))
    # messages = [rec.message for rec in caplog.records]
    # assert messages[-1] == "No rollback files to apply"
    assert not has_applied_migration

    # Migrating up again

    loop.run_until_complete(migrate_up(aengine, folder=VALID_MIGRATIONS_DIR))

    assert check_table_exists(engine, "general", "test")
    assert check_table_exists(engine, "general", "test2")
    assert check_column_exists(engine, "general", "test", "baz")
    data = fetch_all(engine, "SELECT * FROM public.migration ORDER BY applied_at DESC")
    assert len(data) == 6


class TestMigrationFiles:
    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, migration_dir):
        monkeypatch.setattr("padmy.migration.create_files._get_user_email", lambda: TEST_EMAIL)
        yield
        for _file in migration_dir.glob("*.sql"):
            _file.unlink()

    @pytest.fixture()
    def setup_valid_migration_files(self, migration_dir, monkeypatch):
        monkeypatch.setattr("padmy.migration.create_files._get_user_email", lambda: TEST_EMAIL)
        mock = Mock()
        monkeypatch.setattr("time.time", mock)
        from padmy.migration import create_new_migration

        mock.return_value = 1
        create_new_migration(migration_dir)
        mock.return_value = 2
        create_new_migration(migration_dir)
        mock.return_value = 3
        create_new_migration(migration_dir)

    @pytest.fixture()
    def setup_invalid_up_migration_files(self, migration_dir, monkeypatch):
        monkeypatch.setattr("padmy.migration.create_files._get_user_email", lambda: TEST_EMAIL)
        monkeypatch.setattr("padmy.migration.create_files._get_last_migration_name", lambda _: "0-10000000")
        uuid_mock = Mock()
        mock = Mock()
        monkeypatch.setattr("uuid.uuid4", uuid_mock)
        monkeypatch.setattr("time.time", mock)
        from padmy.migration import create_new_migration

        mock.return_value = 1
        uuid_mock.return_value = "10000000"
        create_new_migration(migration_dir)
        mock.return_value = 2
        uuid_mock.return_value = "20000000"
        up_file, _ = create_new_migration(migration_dir)

    @pytest.mark.parametrize(
        "setup, expected",
        [
            pytest.param("setup_valid_migration_files", nullcontext(False), id="valid"),
            pytest.param(
                "setup_invalid_up_migration_files",
                pytest.raises(
                    ValueError,
                    match=re.escape(
                        "Invalid header for up file 2-20000000-up.sql : (expected '1-10000000-up.sql' got '0-10000000')"
                    ),
                ),
                id="invalid",
            ),
        ],
    )
    def test_verify_migration_files(self, migration_dir, setup, expected, request, capsys):
        request.getfixturevalue(setup)
        from padmy.migration.utils import verify_migration_files

        with expected as e:
            assert verify_migration_files(migration_dir, raise_error=True) == e

        assert capsys.readouterr().out.strip()
