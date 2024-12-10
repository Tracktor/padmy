from pathlib import Path
import datetime as dt
from .utils import get_files, utc_now, iter_migration_files, MigrationFile

__all__ = ("reorder_files", "rename_files")


def reorder_files(folder: Path, last_migration_ids: list[str]):
    """
    Reorder the migration files given the last N commits (in descending order).
    For instance, let's say we have the following migration files:

            0000_file1.sql
            0001_file2.sql
            0002_file3.sql
            0003_file4.sql
            0004_file5.sql

    and we want to reorder the files given the last 2 commits: 0003 and 0001,
    the new order will be:

            0000_file1.sql
            0001_file2.sql <= last commit -1
            0003_file4.sql <= last commit
            0002_file3.sql <= moved after the last commit
            0004_file5.sql <= moved after the last commit

    This is useful when you have migrations applied on one branch (let's say master) that are not
    in the same order as the branch you are currently working on (like develop).

    """
    if not last_migration_ids:
        return

    _migration_ids = set(last_migration_ids)

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
        # If the size in unchanged, we did not meet any migration file to reoder
        if len(_migration_ids) == len(last_migration_ids):
            after_files.append((up_file, down_file))
        # Otherwise, we add it to the to_reorder_files list
        else:
            to_reorder_files.append((up_file, down_file))

    new_order_files = commit_files + to_reorder_files + after_files

    # We rename the files to reflect the new order
    rename_files(utc_now(), last_migrations=(last_up_file, last_down_file), migration_files=new_order_files)


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
            if _up_file.header is None or _down_file.header is None:
                raise ValueError("Header is missing")
            _up_file.header.prev_file, _down_file.header.prev_file = _prev_up_file.path.name, _prev_down_file.path.name
            _up_file.write_header()
            _down_file.write_header()
        _prev_up_file, _prev_down_file = _up_file, _down_file
