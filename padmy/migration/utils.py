import datetime as dt
import sys
import uuid

if sys.version_info >= (3, 12):
    UTC = dt.UTC
else:
    from pytz import UTC

import dataclasses
import textwrap
from itertools import groupby
from operator import attrgetter
from pathlib import Path
from padmy.logs import logs

from padmy.utils import exec_cmd

__all__ = (
    "Header",
    "MigrationFile",
    "get_files",
    "iter_migration_files",
    "verify_migration_files",
    "get_git_email",
    "utc_now",
)


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

    def as_text(self):
        _header = [
            f"-- Prev-file: {self.prev_file or ''}",
            f"-- Author: {self.author or ''}",
        ]
        if self.version is not None:
            _header.append(f"-- Version: {self.version}")

        file_header = textwrap.dedent("\n".join(_header)).strip()
        return file_header


@dataclasses.dataclass
class MigrationFile:
    ts: dt.datetime
    file_id: str
    file_type: str
    path: Path
    header: Header | None = None

    @property
    def name(self):
        return f"{int(self.ts.timestamp())}-{self.file_id}-{self.file_type}.sql"

    def replace_ts(self, ts: dt.datetime):
        self.ts = ts
        new_path = self.path.with_name(self.name)
        self.path = self.path.rename(new_path)

    def write_header(self):
        if not self.header:
            return
        # Remove the first lines starting with --
        if self.path.exists():
            lines = [_line for _line in self.path.read_text().split("\n") if not _line.startswith("-- ")]
            _lines = "\n" + "\n".join(lines)
        else:
            _lines = "\n"
        new_text = self.header.as_text() + _lines
        self.path.write_text(new_text)

    @staticmethod
    def generate_base_name(ts: int | None = None, file_id: str | None = None) -> str:
        """
        Get a base name based on the timestamp and file_id
        to be used to generated file name later ({base_name}-{file_type}.sql)
        """
        _file_id = str(uuid.uuid4())[:8] if file_id is None else file_id
        _ts = ts if ts is not None else int(utc_now().timestamp())
        return f"{_ts}-{_file_id}"

    @classmethod
    def from_file(cls, path: Path):
        filename_infos = parse_filename(path.name)
        header = Header.from_text(path.read_text())
        return cls(
            ts=filename_infos["file_ts"],
            file_id=filename_infos["file_id"],
            file_type=filename_infos["migration_type"],
            path=path,
            header=header if not header.is_empty else None,
        )


def parse_filename(filename: str) -> dict:
    ts, file_id, file_type = filename.split("-")
    infos = {
        "file_ts": dt.datetime.fromtimestamp(int(ts), tz=UTC).replace(tzinfo=None),
        "file_id": file_id,
        "migration_type": file_type.replace(".sql", ""),
    }
    return infos


def get_files(folder: Path, reverse: bool = False, up_only: bool = False) -> list[MigrationFile]:
    """Returns the migration files in ascending order"""
    files = []
    pattern = "*.sql" if not up_only else "*-up.sql"
    for file in folder.glob(pattern):
        files.append(MigrationFile.from_file(file))

    return sorted(files, key=attrgetter("ts", "file_id"), reverse=reverse)


def iter_migration_files(files: list[MigrationFile]):
    for _file_id, _files_it in groupby(files, lambda x: x.file_id):
        _files: list[MigrationFile] = list(_files_it)

        _up_files = [x for x in _files if x.file_type == "up"]
        _down_files = [x for x in _files if x.file_type == "down"]

        if len(_up_files) != 1:
            raise ValueError(
                f'Found {len(_up_files)} "up" files (file_id: {_up_files[0].file_id})'
                if _up_files
                else "No up file found"
            )
        if len(_down_files) != 1:
            raise ValueError(
                f'Found {len(_down_files)} "down" files (file_id: {_down_files[0].file_id})'
                if _down_files
                else "No down file found"
            )

        yield _up_files[0], _down_files[0]


def verify_migration_files(migration_dir: Path, *, raise_error: bool = True):
    """
    Verifies that the migration files are in order.
    """
    prev_files = None
    has_errors = False
    _ids = set()
    for _file in iter_migration_files(get_files(migration_dir)):
        if prev_files is None:
            prev_files = _file
            continue
        prev_up, prev_down = prev_files
        up, down = _file

        if up.file_id in _ids:
            raise ValueError(f"Duplicate file id {up.file_id}")
        _ids.add(up.file_id)

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
        else:
            logs.info(f"Valid header for file {up.path.name}")

        prev_files = _file

    return has_errors


def utc_now():
    return dt.datetime.now(UTC).replace(tzinfo=None)
