import logging

import pytest

from .conftest import STATIC_DIR


@pytest.fixture(autouse=True)
def restore_log_lvl():
    from padmy.logs import logs

    yield
    logs.setLevel(logging.INFO)


def test_run_anonymize():
    from run import cli

    cli.run_with_args("anonymize", "--db", "test", "-f", str(STATIC_DIR / "config.yml"))


@pytest.fixture()
def setup_test_schemas(engine):
    engine.execute("DROP SCHEMA IF EXISTS test CASCADE")
    engine.execute("CREATE SCHEMA test")
    engine.commit()
    yield
    engine.execute("DROP SCHEMA test CASCADE")
    engine.commit()


@pytest.mark.usefixtures("setup_test_db", "setup_test_schemas")
def test_run_sample(capsys, loop):
    from run import cli

    cli.run_with_args(
        "sample",
        "--db",
        "test",
        "--db-to",
        "test2",
        "--schemas",
        "public, test",
        "--sample",
        5,
        "--copy-db",
    )
    capsys.readouterr()


def test_run_copy_db(capsys):
    from run import cli

    cli.run_with_args(
        "copy-db", "--db", "test", "--db-to", "test2", "--schemas", "public"
    )
    capsys.readouterr()
    # _ = capsys.readouterr().out
