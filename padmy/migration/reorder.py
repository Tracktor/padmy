import datetime as dt
from pathlib import Path

from padmy.logs import logs
from .utils import get_files, utc_now, iter_migration_files, MigrationFile, verify_migration_files, MigrationErrorType

__all__ = ("reorder_files", "reorder_files_by_applied_migrations", "rename_files", "reorder_files_by_last")


def reorder_files_by_applied_migrations(folder: Path, last_applied_ids: list[str]):
    """
    Reorder the migration files given the last N commits (in descending order).
    For instance, let's say we have the following migration files:

            0000_file1.sql
            0001_file2.sql
            0002_file3.sql
            0003_file4.sql
            0004_file5.sql

    and we want to reorder the files given the last 2 commits: 0003 and 0001 that have already been applied,
    the new order will be:

            0000_file1.sql
            0001_file2.sql <= last commit -1
            0003_file4.sql <= last commit
            0002_file3.sql <= moved after the last commit
            0004_file5.sql <= moved after the last commit

    This is useful when you have migrations applied on one branch (let's say master) that are not
    in the same order as the branch you are currently working on (like develop).

    """
    if not last_applied_ids:
        logs.warning("No migration ids provided, skipping reorder")
        return

    _migration_ids = set(last_applied_ids)

    to_reorder_files, commit_files, after_files = [], [], []
    last_up_file, last_down_file = None, None
    files = get_files(folder, reverse=True)
    # Iterating files to find the one to reorder starting from the most recent migrations
    for up_file, down_file in iter_migration_files(files):
        # If we don't have any more migrations to reorder, we can stop
        if not _migration_ids:
            # Saving the last up and down files for the rewrite_headers function later
            last_up_file, last_down_file = up_file, down_file
            break

        # If we encounter a migration file in the last commits list, we add it to the commit_files list
        if up_file.file_id in _migration_ids:
            _migration_ids -= {up_file.file_id}
            commit_files.append((up_file, down_file))
            continue
        # If the size in unchanged, we did not meet any migration file to reorder
        if len(_migration_ids) == len(last_applied_ids):
            after_files.append((up_file, down_file))
        # Otherwise, we add it to the to_reorder_files list
        else:
            to_reorder_files.append((up_file, down_file))

    if _migration_ids:
        raise ValueError("Some migration ids were not found")

    new_order_files = commit_files + to_reorder_files + after_files

    # We rename the files to reflect the new order
    rename_files(utc_now(), last_migrations=(last_up_file, last_down_file), migration_files=new_order_files)


def _fix_file(
    files: tuple[MigrationFile, MigrationFile],
    prev_files: tuple[MigrationFile, MigrationFile],
    error_type: MigrationErrorType,
):
    up_file, down_file = files
    prev_up_file, prev_down_file = prev_files

    match error_type:
        case "header":
            if up_file.header is None or down_file.header is None:
                raise ValueError("Header is missing")
            up_file.header.prev_file, down_file.header.prev_file = prev_up_file.path.name, prev_down_file.path.name
            up_file.write_header()
            down_file.write_header()
        case _:
            raise NotImplementedError(f"Reorder error type {error_type} is not implemented")


def reorder_files(folder: Path):
    invalid_files = verify_migration_files(folder, raise_error=False)
    if not invalid_files:
        logs.info("All files are correctly ordered")
        return
    logs.info(f"Found {len(invalid_files)} files to reorder")

    file_errors = {error.file_id: error for _, _, error in invalid_files}

    prev_up_file, prev_down_file = None, None
    for up_file, down_file in iter_migration_files(get_files(folder)):
        # Skipping the first iteration
        if prev_up_file is None or prev_down_file is None:
            prev_up_file, prev_down_file = (up_file, down_file)
            continue

        _error = file_errors.get(up_file.file_id)
        if _error is not None:
            _fix_file((up_file, down_file), (prev_up_file, prev_down_file), _error.error_type)
        prev_up_file, prev_down_file = up_file, down_file

    logs.info("Done")


def rename_files(
    last_ts: dt.datetime,
    last_migrations: tuple[MigrationFile | None, MigrationFile | None],
    migration_files: list[tuple[MigrationFile, MigrationFile]],
):
    """
    Rename the migration files to reflect the new order.
    Also rewrite the headers accordingly.
    """
    _prev_up_file, _prev_down_file = last_migrations
    for i, (_up_file, _down_file) in enumerate(migration_files):
        _up_file.replace_ts(last_ts + dt.timedelta(seconds=i))
        _down_file.replace_ts(last_ts + dt.timedelta(seconds=i))
        # Rewrite headers
        if _prev_down_file is not None and _prev_up_file is not None:
            _fix_file((_up_file, _down_file), (_prev_up_file, _prev_down_file), "header")
        _prev_up_file, _prev_down_file = _up_file, _down_file


def reorder_files_by_last(folder: Path, last_ids: list[str]):
    """
    Reorder the migration files given the last N commits (in descending order).
    For instance, let's say we have the following migration files:

        0000_file1.sql
        0001_file2.sql
        0003_file3.sql
        0004_file4.sql
        0005_file5.sql

    And we want to reorder the files given the last 2 commits: 0004 and 003,
    the new order will be:

        0000_file1.sql
        0001_file2.sql
        0005_file5.sql
        0003_file3.sql <= moved to last -1
        0004_file4.sql <= moved to last
    """
    if not last_ids:
        logs.warning("No migration ids provided, skipping reorder")
        return
    _file_ids = set(last_ids)
    last_ts = utc_now()
    for up_file, down_file in iter_migration_files(get_files(folder)):
        if not _file_ids:
            break
        if up_file.file_id in _file_ids:
            up_file.replace_ts(last_ts)
            down_file.replace_ts(last_ts)
            last_ts += dt.timedelta(microseconds=1)
            _file_ids.remove(up_file.file_id)

    if _file_ids:
        raise ValueError("Some migration ids were not found")

    reorder_files(folder)
