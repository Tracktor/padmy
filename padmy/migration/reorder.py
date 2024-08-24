import shutil
from pathlib import Path
import datetime as dt
from .utils import get_files, utc_now, iter_migration_files


def reorder_files(folder: Path, output_folder: Path, last_commits: list[str]):
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
    # Creating the output dir
    if output_folder.exists():
        shutil.rmtree(output_folder)
    shutil.copytree(folder, output_folder)

    files = get_files(output_folder, reverse=True)
    # if not last_commits:
    #     return files
    to_reorder_files = []
    commit_idx = 0
    for up_file, down_file in iter_migration_files(files):
        if up_file.file_id == last_commits[commit_idx]:
            commit_idx += 1
        else:
            to_reorder_files.append((up_file, down_file))
        if commit_idx >= len(last_commits):
            break

    _now = utc_now()
    _prev_up_file, _prev_down_file = None, None
    for i, (_up_file, _down_file) in enumerate(to_reorder_files):
        _up_file.replace_ts(_now - dt.timedelta(seconds=i))
        _down_file.replace_ts(_now - dt.timedelta(seconds=i))
        if _prev_up_file is not None:
            _prev_up_file.header.prev_file = _up_file.path.name
        if _prev_down_file is not None:
            _prev_down_file.header.prev_file = _down_file.path.name

        _prev_up_file, _prev_down_file = _up_file, _down_file
