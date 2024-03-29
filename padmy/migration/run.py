import os
import tempfile
from pathlib import Path

from asyncpg import Connection
from piou import Option, Derived, CommandGroup, Password, CommandError

from padmy.env import SQL_DIR, MIGRATION_DIR, CONSOLE
from padmy.logs import logs
from padmy.utils import get_pg, PgHost, PgPort, PgUser, PgDatabase, PgPassword

MIGRATION_DESCRIPTION = """
Utilities to handle schema migrations with PostgresSQL.  
If it's the first time running theses scripts please run 
**`poetry run cli migrate setup`** to setup the database's table
"""

migration = CommandGroup("migrate", help="Migration utilities", description=MIGRATION_DESCRIPTION)

MigrationDir = Option(MIGRATION_DIR or ..., "--sql-dir", help="Directory containing the migration files")
SQLDir = Option(SQL_DIR or ..., "--sql-dir", help="Directory containing the table definitions")


@migration.command(cmd="new-sql")
def new_sql_file(
    sql_dir: Path = SQLDir,
    position: int = Option(1, help="Position where to insert the sql file"),
):
    """
    Utility to insert a new sql file between other sql files.
    """
    from .new_sql import create_sql_file

    create_sql_file(sql_dir, position)


@migration.command(cmd="apply-sql")
async def apply_sql_files(
    pg_conn: Connection = Derived(get_pg),
    sql_dir: Path = SQLDir,
):
    """
    Utility to insert a new sql file between other sql files.
    """
    from ..utils import exec_file

    for file in sorted(sql_dir.glob("*.sql")):
        logs.info(f"Applying {file.name!r}")
        await exec_file(pg_conn, file)


@migration.command(cmd="new", help="Creates 2 new files for a migration (up and down)")
def new_migrate_file(
    migration_folder: Path = MigrationDir, version: str = Option(None, "--version", help="Version of the migration")
):
    """
    Creates 2 new files for a migration (up and down).
    When applying *up.sql* then *down.sql*, the database must be
    in the state it was previously.
    To check if your **down** file works as expected, you can use the `verify` command.
    """
    from .create_files import create_new_migration

    create_new_migration(migration_folder, version=version)


@migration.command(cmd="up", help="Migrate database to the new schema")
async def migrate_up_main(
    pg_conn: Connection = Derived(get_pg),
    sql_dir: Path = MigrationDir,
    nb_migrations: int = Option(1, "-n", "--nb-migrations", help="Number of migrations to apply"),
):
    from .migration import migrate_up, NoSetupTableError

    try:
        await migrate_up(pg_conn, folder=sql_dir, nb_migrations=nb_migrations)
    except NoSetupTableError as e:
        logs.critical(e, exc_info=False)


@migration.command(cmd="down", help="Rollback the database to the specified migration")
async def migrate_down_main(
    conn: Connection = Derived(get_pg),
    sql_dir: Path = MigrationDir,
    nb_rollbacks: int = Option(1, "-n", "--nb-rollbacks", help="Number of rollbacks to apply"),
):
    from .migration import migrate_down, NoSetupTableError

    try:
        await migrate_down(conn, folder=sql_dir, nb_migrations=nb_rollbacks)
    except NoSetupTableError as e:
        logs.critical(e, exc_info=False)


@migration.command(cmd="setup", help="Create the tables that will contain the migration metadata")
async def migrate_setup_main(pg_conn: Connection = Derived(get_pg)):
    from .migration import migrate_setup

    await migrate_setup(pg_conn)


@migration.command(cmd="verify", help="Verify that a migration is valid")
def migrate_verify_main(
    db: str = PgDatabase,
    pg_host: str = PgHost,
    pg_port: int = PgPort,
    pg_user: str = PgUser,
    pg_password: Password = PgPassword,
    schemas: list[str] = Option(..., "--schemas", help="Schemas impacted by the migration"),
    sql_dir: Path = MigrationDir,
):
    os.environ["PG_HOST"] = pg_host
    os.environ["PG_PORT"] = str(pg_port)
    os.environ["PG_USER"] = pg_user
    os.environ["PG_PASSWORD"] = pg_password

    from .migration import migrate_verify, MigrationError

    try:
        with tempfile.TemporaryDirectory() as dump_dir:
            migrate_verify(
                database=db,
                migration_folder=sql_dir,
                schemas=schemas,
                dump_dir=Path(dump_dir),
            )
    except MigrationError as e:
        logs.error(e.msg)
        logs.debug(e.diff)


@migration.command(cmd="verify-files", help="Verify the files are correctly ordered")
def verify_files(
    sql_dir: Path = MigrationDir,
    no_raise_error: bool = Option(False, "--no-raise", help="Raise an error if the files are not correctly ordered"),
):
    from .utils import verify_migration_files

    has_errors = verify_migration_files(sql_dir, raise_error=not no_raise_error)
    if has_errors:
        raise CommandError("Files are not correctly ordered")
    else:
        CONSOLE.print("[green]Files are correctly ordered[/green]")
