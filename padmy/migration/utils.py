import dataclasses
import datetime as dt
from itertools import groupby
from operator import attrgetter
from pathlib import Path
from padmy.logs import logs

from padmy.utils import exec_cmd


def get_git_email():
    return exec_cmd(["git", "config", "user.email"]).strip()


@dataclasses.dataclass
class Header:
    prev_file: str | None
    author: str | None
    version: str | None

    @property
    def is_empty(self):
        return not any([self.prev_file, self.author, self.version])

    @classmethod
    def from_text(cls, text: str):
        prev_file = None
        author = None
        version = None
        for line in text.split("\n"):
            if line.startswith("-- Prev-file:"):
                prev_file = line.split(":")[1].strip()
            elif line.startswith("-- Author:"):
                author = line.split(":")[1].strip()
            elif line.startswith("-- Version:"):
                version = line.split(":")[1].strip()
        return cls(prev_file, author, version)


@dataclasses.dataclass
class MigrationFile:
    ts: dt.datetime
    file_id: str
    file_type: str
    path: Path
    header: Header | None = None


def get_files(folder: Path, reverse: bool = False) -> list[MigrationFile]:
    """Returns the migration files in ascending order"""
    files = []
    for file in folder.glob("*.sql"):
        ts, file_id, file_type = file.name.split("-")
        header = Header.from_text(file.read_text())
        files.append(
            MigrationFile(
                ts=dt.datetime.fromtimestamp(int(ts)),
                file_id=file_id,
                file_type=file_type,
                path=file,
                header=header if not header.is_empty else None,
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


def verify_migration_files(migration_dir: Path, *, raise_error: bool = True):
    """
    Verifies that the migration files are in order.
    """
    prev_files = None
    has_errors = False
    for _file in iter_migration_files(get_files(migration_dir)):
        if prev_files is None:
            prev_files = _file
            continue
        prev_up, prev_down = prev_files
        up, down = _file

        try:
            if prev_up.ts > up.ts:
                raise ValueError(f"Files are not in order: {prev_up.path.name} > {up.path.name}")
            if prev_down.ts > down.ts:
                raise ValueError(f"Files are not in order: {prev_down.path.name} > {down.path.name}")

            if up.header is not None and up.header.prev_file != prev_up.path.name:
                raise ValueError(
                    f"Invalid header for up file {up.path.name} : "
                    f"(expected {prev_up.path.name!r} got {up.header.prev_file!r})"
                )
            if down.header is not None and down.header.prev_file != prev_down.path.name:
                raise ValueError(
                    f"Invalid header for down file {down.path.name} : "
                    f"(expected {prev_down.path.name!r} got {down.header.prev_file!r})"
                )
        except ValueError as e:
            if raise_error:
                raise e
            logs.warning(e.args[0])
            has_errors = True

        prev_files = _file

    return has_errors
