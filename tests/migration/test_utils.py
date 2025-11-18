import datetime as dt
import pathlib

import pytest

from padmy.migration import Header, MigrationFile, get_files

DEFAULT_TIME = dt.datetime(2024, 1, 1, 0, 0, 0)


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
        "last-applied", [], ["0000000", "0000001", "0000002", "0000003", "0000004"], id="empty folder - last applied"
    )

    _one_file_change_last_applied = pytest.param(
        "last-applied",
        ["0000002", "0000004"],
        ["0000000", "0000001", "0000004", "0000002", "0000003"],
        id="one file change - last applied",
    )

    _empty_folder_last = pytest.param(
        "last", [], ["0000000", "0000001", "0000002", "0000003", "0000004"], id="empty folder - last"
    )

    _one_file_change_last = pytest.param(
        "last", ["0000002"], ["0000000", "0000001", "0000003", "0000004", "0000002"], id="one file change - last"
    )

    _multiple_files_change_last = pytest.param(
        "last",
        ["0000002", "0000000"],
        ["0000001", "0000003", "0000004", "0000000", "0000002"],
        id="multiple files change - last",
    )

    @pytest.mark.usefixtures("setup_migration_folder")
    @pytest.mark.parametrize(
        "mode, last_ids, expected",
        [
            _empty_folder_last_applied,
            _one_file_change_last_applied,
            _empty_folder_last,
            _one_file_change_last,
            _multiple_files_change_last,
        ],
    )
    def test_reorder(self, mode, last_ids, migration_folder, expected):
        from padmy.migration import reorder_files_by_applied_migrations, verify_migration_files, reorder_files_by_last

        if mode == "last-applied":
            reorder_files_by_applied_migrations(migration_folder, last_applied_ids=last_ids)
        elif mode == "last":
            reorder_files_by_last(migration_folder, last_ids=last_ids)
        else:
            raise ValueError(f"Unknown mode {mode!r}")
        new_order = list([x.file_id for x in get_files(migration_folder, up_only=True)])
        assert new_order == expected
        assert not verify_migration_files(migration_folder)


class TestReorder:
    @staticmethod
    def _get_valid_order(folder: pathlib.Path):
        for _file_id, _header in [
            ("0000000", Header(prev_file=None, author="foo", version=None)),
            ("0000001", Header(prev_file="0000000", author="foo", version=None)),
            ("0000002", Header(prev_file="0000001", author="foo", version=None)),
        ]:
            _base_name = MigrationFile.generate_base_name(ts=int(DEFAULT_TIME.timestamp()), file_id=_file_id)
            for _file_type in ["up", "down"]:
                _file = MigrationFile(
                    ts=DEFAULT_TIME,
                    file_id=_file_id,
                    file_type=_file_type,
                    path=folder / f"{_base_name}-{_file_type}.sql",
                    header=_header,
                )
                _file.write_header()
                if _file_type == "up":
                    _prev_file_up = _file.name
                else:
                    _prev_file_down = _file.name

    @staticmethod
    def _get_invalid_header(folder: pathlib.Path):
        for _file_id, _header in [
            ("0000000", Header(prev_file=None, author="foo", version=None)),
            ("0000001", Header(prev_file="0000001", author="foo", version=None)),
            ("0000002", Header(prev_file="0000001", author="foo", version=None)),
        ]:
            _base_name = MigrationFile.generate_base_name(ts=int(DEFAULT_TIME.timestamp()), file_id=_file_id)
            for _file_type in ["up", "down"]:
                _file = MigrationFile(
                    ts=DEFAULT_TIME,
                    file_id=_file_id,
                    file_type=_file_type,
                    path=folder / f"{_base_name}-{_file_type}.sql",
                    header=_header,
                )
                _file.write_header()
                if _file_type == "up":
                    _prev_file_up = _file.name
                else:
                    _prev_file_down = _file.name

    _valid = pytest.param(
        lambda folder: TestReorder._get_valid_order(folder),
        ["0000000", "0000001", "0000002"],
        id="valid order",
    )

    _invalid_header_order = pytest.param(
        lambda folder: TestReorder._get_invalid_header(folder),
        ["0000000", "0000001", "0000002"],
        id="invalid header order",
    )

    # @pytest.mark.usefixtures("setup_migration_folder")
    @pytest.mark.parametrize("setup_fn, expected", [_valid, _invalid_header_order])
    def test_reorder(self, setup_fn, migration_folder, expected):
        from padmy.migration import reorder_files, verify_migration_files

        if setup_fn is not None:
            setup_fn(migration_folder)

        reorder_files(migration_folder)

        new_order = list([x.file_id for x in get_files(migration_folder, up_only=True)])
        assert new_order == expected
        assert not verify_migration_files(migration_folder)
