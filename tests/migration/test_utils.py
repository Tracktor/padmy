import datetime as dt

import pytest

from padmy.migration import Header, MigrationFile, get_files

DEFAULT_TIME = dt.datetime(2024, 1, 1, 0, 0, 0)


@pytest.fixture()
def migration_folder(tmp_path):
    return tmp_path


@pytest.fixture()
def setup_migration_folder(migration_folder):
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


_empty_folder = pytest.param(None, [], ["0000000", "0000001", "0000002", "0000003", "0000004"], id="empty folder")

_one_file_change = pytest.param(
    None, ["0000002", "0000004"], ["0000000", "0000001", "0000004", "0000002", "0000003"], id="one file change"
)


@pytest.mark.usefixtures("setup_migration_folder")
@pytest.mark.parametrize("setup_fn, last_migration_ids, expected", [_empty_folder, _one_file_change])
def test_reorder(setup_fn, last_migration_ids, migration_folder, expected):
    from padmy.migration import reorder_files, verify_migration_files

    if setup_fn is not None:
        setup_fn()

    reorder_files(migration_folder, last_migration_ids=last_migration_ids)

    new_order = list([x.file_id for x in get_files(migration_folder, up_only=True)])
    assert new_order == expected
    assert not verify_migration_files(migration_folder)
