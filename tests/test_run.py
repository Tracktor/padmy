import logging

import pytest

from .conftest import STATIC_DIR
from .utils import exec_req


@pytest.fixture(autouse=True)
def restore_log_lvl():
    from padmy.logs import logs
    yield
    logs.setLevel(logging.INFO)


def test_run_anonymize():
    from run import cli

    cli.run_with_args('anonymize',
                      '--db', 'test',
                      '-f', str(STATIC_DIR / 'config.yml'))


@pytest.fixture()
def setup_test_schemas(engine):
    exec_req(engine, 'DROP SCHEMA IF EXISTS test CASCADE')
    exec_req(engine, 'CREATE SCHEMA test')
    yield
    exec_req(engine, 'DROP SCHEMA test CASCADE')


@pytest.mark.usefixtures('setup_test_db', 'setup_test_schemas')
def test_run_sample(capsys, loop):
    from run import cli

    cli.run_with_args('sample',
                      '--db', 'test',
                      '--to-db', 'test2',
                      '--schemas', 'public, test',
                      '--sample', 5,
                      '--copy-db')
    capsys.readouterr()


def test_run_copy_db(capsys):
    from run import cli

    cli.run_with_args('copy-db',
                      '--db', 'test',
                      '--to-db', 'test2',
                      '--schemas', 'public')
    capsys.readouterr()
    # _ = capsys.readouterr().out
