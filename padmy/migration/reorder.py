from pathlib import Path
import datetime as dt
from .utils import get_files, utc_now, iter_migration_files
from padmy.logs import logs

__all__ = ("reorder_files",)


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
            0002_file3.sql
            0004_file5.sql
            0001_file2.sql
            0003_file4.sql
    """
    if not last_migration_ids:
        return

    to_reorder_files = []
    last_commit_files = []
    _migration_ids = set(last_migration_ids)

    _last_up_before_reorder, _last_down_before_reorder = None, None
    _prev_down_file, _prev_up_file = None, None

    files = get_files(folder, reverse=True)
    for up_file, down_file in iter_migration_files(files):
        if _prev_up_file is not None and _prev_up_file.file_id == last_migration_ids[-1]:
            _last_up_before_reorder = up_file
            _last_down_before_reorder = down_file

        if not _migration_ids:
            break

        if up_file.file_id in _migration_ids:
            _migration_ids -= {up_file.file_id}
            last_commit_files.append((up_file, down_file))
        else:
            to_reorder_files.append((up_file, down_file))

        _prev_up_file, _prev_down_file = up_file, down_file

    # Making sure that the files are ordered given the last commits given
    ordered_last_commits = {commit_id: i for i, commit_id in enumerate(last_migration_ids)}
    last_commit_files = sorted(last_commit_files, key=lambda x: ordered_last_commits[x[0].file_id])
    to_reorder_files = to_reorder_files + last_commit_files

    # Note: This can cause an issue is for some reason
    # the last commits are in the future
    _now = utc_now()
    _prev_up_file, _prev_down_file = None, None
    logs.info(f"Found {len(to_reorder_files)} files to reorder")
    for i, (_up_file, _down_file) in enumerate(to_reorder_files):
        # We don't change the timestamp of last commit files
        if _up_file.file_id not in last_migration_ids:
            _up_file.replace_ts(_now - dt.timedelta(seconds=i))
        if _down_file.file_id not in last_migration_ids:
            _down_file.replace_ts(_now - dt.timedelta(seconds=i))

        if _prev_up_file is not None:
            _prev_up_file.header.prev_file = _up_file.path.name
            _prev_up_file.write_header()
        if _prev_down_file is not None:
            _prev_down_file.header.prev_file = _down_file.path.name
            _prev_down_file.write_header()
        _prev_up_file, _prev_down_file = _up_file, _down_file

    if _last_up_before_reorder is not None and _prev_up_file is not None:
        _prev_up_file.header.prev_file = _last_up_before_reorder.path.name
        _prev_up_file.write_header()
    if _last_down_before_reorder is not None and _prev_down_file is not None:
        _prev_down_file.header.prev_file = _last_down_before_reorder.path.name
        _prev_down_file.write_header()
