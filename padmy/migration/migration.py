import dataclasses
import datetime as dt
import difflib
import filecmp
import functools
from contextlib import nullcontext
from pathlib import Path

from asyncpg import Connection
from asyncpg.exceptions import UndefinedTableError

from tracktolib.pg import insert_many
from padmy.logs import logs
from padmy.utils import exec_file, pg_dump, exec_psql_file
from .utils import get_files, iter_migration_files, MigrationFile

_GET_LATEST_COMMIT_QUERY = """
SELECT file_ts, file_name
FROM public.migration
ORDER BY applied_at DESC, file_ts DESC
LIMIT 1
"""

# PG_GLOBAL_DUMP_DIR = GLOBAL_TMP / 'matching-pg-backup' / 'dump'
SETUP_FILE = Path(__file__).parent / "db.sql"


class MigrationError(Exception):
    def __init__(self, msg: str, diff: str):
        self.msg = msg
        self.diff = diff


async def migrate_setup(conn: Connection):
    logs.info("Setting up migration tables")
    await exec_file(conn, SETUP_FILE)
    logs.info("Done")


def _verify_migration(
    database: str,
    schemas: list[str],
    migration_id: str,
    up_file: Path,
    down_file: Path,
    dump_dir: Path,
):
    _before_dump, _after_dump = (
        f"{migration_id}-before.sql",
        f"{migration_id}-after.sql",
    )

    # Dump before
    pg_dump(
        database,
        schemas,
        dump_path=str(dump_dir / _before_dump),
        options=["-E", "utf8", "--schema-only"],
    )

    logs.info(f"Applying {up_file.name}")
    exec_psql_file(database, str(up_file))

    logs.info(f"Applying {down_file.name}")
    exec_psql_file(database, str(down_file))

    # Dump after
    pg_dump(
        database,
        schemas,
        dump_path=str(dump_dir / _after_dump),
        options=["-E", "utf8", "--schema-only"],
    )

    _before_dump_file, _after_dump_file = (
        dump_dir / _before_dump,
        dump_dir / _after_dump,
    )

    _no_diff = filecmp.cmp(_before_dump_file, _after_dump_file)

    if not _no_diff:
        diff = difflib.unified_diff(
            _before_dump_file.read_text().split("\n"),
            _after_dump_file.read_text().split("\n"),
            fromfile=_before_dump_file.name,
            tofile=_after_dump_file.name,
        )
        raise MigrationError(f"Difference found for migration: {migration_id}", diff="\n".join(diff))


def migrate_verify(
    database: str,
    schemas: list[str],
    dump_dir: Path,
    migration_folder: Path,
):
    """
    Verifies that the up/down migration is correct
    """
    _pg_dump = functools.partial(pg_dump, database=database, schemas=schemas)

    migration_files = get_files(migration_folder)
    logs.info(f"Found {len(migration_files)} migration files")

    logs.info("Checking...")
    _down_files: list[MigrationFile] = []
    try:
        for i, (_up_file, _down_file) in enumerate(iter_migration_files(migration_files)):
            logs.info(f"Checking migration {i + 1}/{int(len(migration_files) / 2)}")
            _verify_migration(
                database=database,
                schemas=schemas,
                migration_id=_up_file.file_id,
                up_file=_up_file.path,
                down_file=_down_file.path,
                dump_dir=dump_dir,
            )
            # Need to be reapplied for the next tests
            exec_psql_file(database, str(_up_file.path))
            _down_files.append(_down_file)
    except MigrationError as e:
        logs.error(e.msg)
        logs.error(e.diff)
        raise e

    # Restoring to initial state
    logs.info("Restoring to initial state")
    for i, _down_file in enumerate(reversed(_down_files)):
        logs.info(f"Applying {_down_file.path.name} ({len(_down_files) - i}/{len(_down_files)})")
        exec_psql_file(database, str(_down_file.path))
    # else:
    #     if last_down_file:
    #         exec_psql_file(database, str(last_down_file.path))

    logs.info("Everything ok!")


@dataclasses.dataclass
class LatestMigration:
    timestamp: dt.datetime
    file_name: str

    @classmethod
    def load(cls, data: dict):
        return cls(timestamp=data["file_ts"], file_name=data["file_name"])


class NoSetupTableError(Exception):
    pass


async def _get_latest_migration(conn: Connection):
    try:
        resp = await conn.fetchrow(_GET_LATEST_COMMIT_QUERY)
    except UndefinedTableError as e:
        if 'relation "public.migration" does not exist' in str(e):
            raise NoSetupTableError(
                'Could not find table table "public.migration", did you forget to setup the table'
                ' by running "migration setup" ?'
            )
        else:
            raise
    return LatestMigration.load(dict(resp)) if resp else None


async def migrate_up(
    conn: Connection,
    folder: Path,
    *,
    nb_migrations: int = -1,
    metadata: dict | None = None,
    use_transaction: bool = True,
):
    """Migrate from the latest migration applied available in the database
    to the latest one in the `folder` dir.
    """
    latest_migration = await _get_latest_migration(conn)
    logs.info(f"Latest timestamp: {latest_migration.timestamp}" if latest_migration else "No previous migration found")
    migration_files = get_files(folder)

    migrations_to_apply = [
        _up_file
        for _up_file, _ in iter_migration_files(migration_files)
        if latest_migration is None
        or (_up_file.ts >= latest_migration.timestamp and _up_file.path.name != latest_migration.file_name)
    ]
    if nb_migrations > 0:
        migrations_to_apply = migrations_to_apply[:nb_migrations]

    if not migrations_to_apply:
        logs.info("No migrations to apply")
        return

    _transaction = conn.transaction if use_transaction else nullcontext
    logs.info(f"Found {len(migrations_to_apply)} migrations to apply")
    async with _transaction():
        for _migration in migrations_to_apply:
            logs.info(f"Running {_migration.path.name}...")
            try:
                await exec_file(conn, _migration.path)
            except Exception as e:
                logs.error(f'Failed to execute migration_id "{_migration.file_id}"')
                raise e
            commit_data = {
                "file_ts": _migration.ts,
                "file_id": _migration.file_id,
                "migration_type": "up",
                "file_name": _migration.path.name,
            }
            if metadata:
                commit_data["meta"] = metadata
            await insert_many(conn, "public.migration", [commit_data])

    logs.info("Done!")


async def migrate_down(
    conn: Connection,
    folder: Path,
    *,
    nb_migrations: int = -1,
    metadata: dict | None = None,
    use_transaction: bool = True,
    # migration_id: str = None
):
    """
    Rollback `nb_migrations` back (default -1 for all available)
    """
    latest_migration = await _get_latest_migration(conn)

    migration_files = get_files(folder, reverse=True)

    rollback_to_apply = [
        _down_file
        for _, _down_file in iter_migration_files(migration_files)
        if latest_migration is None
        or (_down_file.ts <= latest_migration.timestamp and _down_file.path.name != latest_migration.file_name)
    ]

    if nb_migrations > 0:
        rollback_to_apply = rollback_to_apply[:nb_migrations]

    nb_files = len(rollback_to_apply)
    if nb_files == 0:
        logs.info("No rollback files to apply")
        return

    _transaction = conn.transaction if use_transaction else nullcontext
    logs.info(f"Found {nb_files} rollback files to apply")
    async with _transaction():
        for _rollback in rollback_to_apply:
            logs.info(f"Running {_rollback.path.name}...")
            try:
                await exec_file(conn, _rollback.path)
            except Exception as e:
                logs.error(f'Failed to execute migration_id "{_rollback.file_id}"')
                raise e

            commit_data = {
                "file_ts": _rollback.ts,
                "file_id": _rollback.file_id,
                "migration_type": "down",
                "file_name": _rollback.path.name,
            }
            if metadata:
                commit_data["meta"] = metadata
            await insert_many(conn, "public.migration", [commit_data])

    logs.info("Done!")
