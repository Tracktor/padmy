import dataclasses
import datetime as dt
from itertools import groupby
from operator import attrgetter
from pathlib import Path

from padmy.utils import exec_cmd


def get_git_email():
    return exec_cmd(["git", "config", "user.email"]).strip()


@dataclasses.dataclass
class MigrationFile:
    ts: dt.datetime
    file_id: str
    file_type: str
    path: Path


def get_files(folder: Path, reverse: bool = False) -> list[MigrationFile]:
    """Returns the migration files in ascending order"""
    files = []
    for file in folder.glob("*.sql"):
        ts, file_id, file_type = file.name.split("-")
        files.append(
            MigrationFile(
                ts=dt.datetime.fromtimestamp(int(ts)),
                file_id=file_id,
                file_type=file_type,
                path=file,
            )
        )
    return sorted(files, key=attrgetter("ts", "file_id"), reverse=reverse)


def iter_migration_files(files: list[MigrationFile]):
    for _file_id, _files_it in groupby(files, lambda x: x.file_id):
        _files: list[MigrationFile] = list(_files_it)

        _up_files = [x for x in _files if x.file_type == "up.sql"]
        _down_files = [x for x in _files if x.file_type == "down.sql"]

        if len(_up_files) != 1:
            raise ValueError(f'Found {len(_up_files)} "up" files')
        if len(_down_files) != 1:
            raise ValueError(f'Found {len(_down_files)} "up" files')

        yield _up_files[0], _down_files[0]
