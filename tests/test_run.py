from .conftest import STATIC_DIR


def test_run_anonymize():
    from run import cli

    cli.run_with_args('anonymize',
                      '--db', 'test',
                      '-f', str(STATIC_DIR / 'config.yml'))


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
