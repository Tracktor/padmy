import datetime as dt
import shutil
from pathlib import Path
from typing import Literal

from padmy.logs import logs
from .utils import get_files, utc_now, iter_migration_files, MigrationFile, verify_migration_files, MigrationErrorType

__all__ = (
    "repair_headers",
    "reorder_files_by_applied_migrations",
    "rename_files",
    "reorder_files_by_last",
    "reorder_migration_files",
)


def reorder_files_by_applied_migrations(folder: Path, last_applied_ids: list[str] | None) -> list[Path]:
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

    Returns:
        List of paths to the modified files.
    """
    if not last_applied_ids:
        logs.warning("No migration ids provided, skipping reorder")
        return []

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
    return rename_files(utc_now(), last_migrations=(last_up_file, last_down_file), migration_files=new_order_files)


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


def repair_headers(folder: Path) -> list[Path]:
    """
    Reorder migration files that have incorrect headers.

    Returns:
        List of paths to the modified files.
    """
    modified_files: list[Path] = []
    invalid_files = verify_migration_files(folder, raise_error=False)
    if not invalid_files:
        logs.info("All files are correctly ordered")
        return modified_files
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
            modified_files.extend([up_file.path, down_file.path])
        prev_up_file, prev_down_file = up_file, down_file

    logs.info("Done")
    return modified_files


def rename_files(
    last_ts: dt.datetime,
    last_migrations: tuple[MigrationFile | None, MigrationFile | None],
    migration_files: list[tuple[MigrationFile, MigrationFile]],
) -> list[Path]:
    """
    Rename the migration files to reflect the new order.
    Also rewrite the headers accordingly.

    Returns:
        List of paths to the modified files.
    """
    modified_files: list[Path] = []
    _prev_up_file, _prev_down_file = last_migrations
    for i, (_up_file, _down_file) in enumerate(migration_files):
        _up_file.replace_ts(last_ts + dt.timedelta(seconds=i))
        _down_file.replace_ts(last_ts + dt.timedelta(seconds=i))
        modified_files.extend([_up_file.path, _down_file.path])
        # Rewrite headers
        if _prev_down_file is not None and _prev_up_file is not None:
            _fix_file((_up_file, _down_file), (_prev_up_file, _prev_down_file), "header")
        _prev_up_file, _prev_down_file = _up_file, _down_file
    return modified_files


def reorder_files_by_last(folder: Path, last_ids: list[str] | None) -> list[Path]:
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

    Returns:
        List of paths to the modified files.
    """
    if not last_ids:
        logs.warning("No migration ids provided, skipping reorder")
        return []
    modified_files: list[Path] = []
    _file_ids = set(last_ids)
    last_ts = utc_now()
    for up_file, down_file in iter_migration_files(get_files(folder)):
        if not _file_ids:
            break
        if up_file.file_id in _file_ids:
            up_file.replace_ts(last_ts)
            down_file.replace_ts(last_ts)
            modified_files.extend([up_file.path, down_file.path])
            last_ts += dt.timedelta(microseconds=1)
            _file_ids.remove(up_file.file_id)

    if _file_ids:
        raise ValueError("Some migration ids were not found")

    modified_files.extend(repair_headers(folder))
    return modified_files


def reorder_migration_files(
    # Directory containing migration files.
    migrations_dir: Path,
    # Optional output directory to copy the migrations to before reordering.
    output_dir: Path | None = None,
    #  List of migration IDs to use for reordering.
    migration_ids: list[str] | None = None,
    #  Method to use for reordering ("last-applied" or "last").
    reorder_by: Literal["last-applied", "last"] | None = None,
    #  If True, skip verification after reordering.
    skip_verify: bool = False,
) -> list[Path]:
    """
    Reorder migration files with optional output directory and verification.

    Returns:
        List of paths to the modified files.
    """

    folder = migrations_dir

    if output_dir is not None:
        if output_dir.exists():
            shutil.rmtree(output_dir)
        shutil.copytree(migrations_dir, output_dir)
        folder = output_dir

    match reorder_by:
        case "last-applied":
            modified_files = reorder_files_by_applied_migrations(folder, last_applied_ids=migration_ids)
        case "last":
            modified_files = reorder_files_by_last(folder, last_ids=migration_ids)
        case _:
            modified_files = repair_headers(folder)

    if not skip_verify:
        verify_migration_files(folder)

    return modified_files
