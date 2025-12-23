import datetime as dt
import pathlib
from typing import Callable, NamedTuple

import pytest

from padmy.migration import Header, MigrationFile, get_files

DEFAULT_TIME = dt.datetime(2024, 1, 1, 0, 0, 0)


class ReorderTestCase(NamedTuple):
    mode: str
    last_ids: list[str]
    expected_order: list[str]
    expected_modified_ids: set[str]


class ReorderFilesTestCase(NamedTuple):
    setup_fn: Callable[[pathlib.Path], None]
    expected_order: list[str]
    expected_modified_ids: set[str]


@pytest.fixture()
def migration_folder(tmp_path):
    return tmp_path


class TestReorderWithIds:
    @pytest.fixture()
    def setup_migration_folder(self, migration_folder):
        files = [
            ("0000000", DEFAULT_TIME),
            ("0000001", DEFAULT_TIME),
            ("0000002", DEFAULT_TIME),
            ("0000003", DEFAULT_TIME),
            ("0000004", DEFAULT_TIME),
        ]

        _prev_file_up, _prev_file_down = None, None
        for _file_id, _file_ts in files:
            _base_name = MigrationFile.generate_base_name(ts=int(_file_ts.timestamp()), file_id=_file_id)
            for _file_type in ["up", "down"]:
                _prev_file = _prev_file_up if _file_type == "up" else _prev_file_down
                _file = MigrationFile(
                    ts=_file_ts,
                    file_id=_file_id,
                    file_type=_file_type,
                    path=migration_folder / f"{_base_name}-{_file_type}.sql",
                    header=Header(prev_file=_prev_file, author="foo", version=None),
                )
                _file.write_header()
                if _file_type == "up":
                    _prev_file_up = _file.name
                else:
                    _prev_file_down = _file.name

    _empty_folder_last_applied = pytest.param(
        ReorderTestCase(
            mode="last-applied",
            last_ids=[],
            expected_order=["0000000", "0000001", "0000002", "0000003", "0000004"],
            expected_modified_ids=set(),
        ),
        id="empty folder - last applied",
    )

    _one_file_change_last_applied = pytest.param(
        ReorderTestCase(
            mode="last-applied",
            last_ids=["0000002", "0000004"],
            expected_order=["0000000", "0000001", "0000004", "0000002", "0000003"],
            expected_modified_ids={"0000002", "0000003", "0000004"},
        ),
        id="one file change - last applied",
    )

    _empty_folder_last = pytest.param(
        ReorderTestCase(
            mode="last",
            last_ids=[],
            expected_order=["0000000", "0000001", "0000002", "0000003", "0000004"],
            expected_modified_ids=set(),
        ),
        id="empty folder - last",
    )

    _one_file_change_last = pytest.param(
        ReorderTestCase(
            mode="last",
            last_ids=["0000002"],
            expected_order=["0000000", "0000001", "0000003", "0000004", "0000002"],
            # 0000002 is moved, 0000003 and 0000002 headers are fixed by reorder_files
            expected_modified_ids={"0000002", "0000003"},
        ),
        id="one file change - last",
    )

    _multiple_files_change_last = pytest.param(
        ReorderTestCase(
            mode="last",
            last_ids=["0000002", "0000000"],
            expected_order=["0000001", "0000003", "0000004", "0000000", "0000002"],
            # 0000000 and 0000002 are moved, headers are fixed by reorder_files
            expected_modified_ids={"0000000", "0000002", "0000003"},
        ),
        id="multiple files change - last",
    )

    @pytest.mark.usefixtures("setup_migration_folder")
    @pytest.mark.parametrize(
        "case",
        [
            _empty_folder_last_applied,
            _one_file_change_last_applied,
            _empty_folder_last,
            _one_file_change_last,
            _multiple_files_change_last,
        ],
    )
    def test_reorder(self, case: ReorderTestCase, migration_folder):
        from padmy.migration import reorder_files_by_applied_migrations, verify_migration_files, reorder_files_by_last
        from padmy.migration.utils import MigrationFile

        if case.mode == "last-applied":
            modified_files = reorder_files_by_applied_migrations(migration_folder, last_applied_ids=case.last_ids)
        elif case.mode == "last":
            modified_files = reorder_files_by_last(migration_folder, last_ids=case.last_ids)
        else:
            raise ValueError(f"Unknown mode {case.mode!r}")

        new_order = [x.file_id for x in get_files(migration_folder, up_only=True)]
        assert new_order == case.expected_order
        assert not verify_migration_files(migration_folder)

        # Check returned files are the right ones
        modified_ids = {MigrationFile.from_file(f).file_id for f in modified_files}
        assert modified_ids == case.expected_modified_ids


class TestRepairHeaders:
    @staticmethod
    def _get_valid_order(folder: pathlib.Path):
        _prev_file_up, _prev_file_down = None, None
        for _file_id in ["0000000", "0000001", "0000002"]:
            _base_name = MigrationFile.generate_base_name(ts=int(DEFAULT_TIME.timestamp()), file_id=_file_id)
            for _file_type in ["up", "down"]:
                _prev_file = _prev_file_up if _file_type == "up" else _prev_file_down
                _file = MigrationFile(
                    ts=DEFAULT_TIME,
                    file_id=_file_id,
                    file_type=_file_type,
                    path=folder / f"{_base_name}-{_file_type}.sql",
                    header=Header(prev_file=_prev_file, author="foo", version=None),
                )
                _file.write_header()
                if _file_type == "up":
                    _prev_file_up = _file.name
                else:
                    _prev_file_down = _file.name

    @staticmethod
    def _get_invalid_header(folder: pathlib.Path):
        _base_name_0 = MigrationFile.generate_base_name(ts=int(DEFAULT_TIME.timestamp()), file_id="0000000")
        # 0000001 has wrong prev_file (points to itself instead of 0000000)
        for _file_id, _prev_file_id in [
            ("0000000", None),
            ("0000001", "0000001"),  # Invalid: points to itself
            ("0000002", "0000001"),
        ]:
            _base_name = MigrationFile.generate_base_name(ts=int(DEFAULT_TIME.timestamp()), file_id=_file_id)
            for _file_type in ["up", "down"]:
                if _prev_file_id is not None:
                    _prev_base = MigrationFile.generate_base_name(
                        ts=int(DEFAULT_TIME.timestamp()), file_id=_prev_file_id
                    )
                    _prev_file = f"{_prev_base}-{_file_type}.sql"
                else:
                    _prev_file = None
                _file = MigrationFile(
                    ts=DEFAULT_TIME,
                    file_id=_file_id,
                    file_type=_file_type,
                    path=folder / f"{_base_name}-{_file_type}.sql",
                    header=Header(prev_file=_prev_file, author="foo", version=None),
                )
                _file.write_header()

    _valid = pytest.param(
        ReorderFilesTestCase(
            setup_fn=lambda folder: TestRepairHeaders._get_valid_order(folder),
            expected_order=["0000000", "0000001", "0000002"],
            expected_modified_ids=set(),
        ),
        id="valid order",
    )

    _invalid_header_order = pytest.param(
        ReorderFilesTestCase(
            setup_fn=lambda folder: TestRepairHeaders._get_invalid_header(folder),
            expected_order=["0000000", "0000001", "0000002"],
            expected_modified_ids={"0000001"},
        ),
        id="invalid header order",
    )

    @pytest.mark.parametrize("case", [_valid, _invalid_header_order])
    def test_repair_headers(self, case: ReorderFilesTestCase, migration_folder):
        from padmy.migration import repair_headers, verify_migration_files
        from padmy.migration.utils import MigrationFile

        if case.setup_fn is not None:
            case.setup_fn(migration_folder)

        modified_files = repair_headers(migration_folder)

        new_order = [x.file_id for x in get_files(migration_folder, up_only=True)]
        assert new_order == case.expected_order
        assert not verify_migration_files(migration_folder)

        # Check returned files are the right ones
        modified_ids = {MigrationFile.from_file(f).file_id for f in modified_files}
        assert modified_ids == case.expected_modified_ids
