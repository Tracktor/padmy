from pathlib import Path


def create_sql_file(sql_dir: Path, position: int) -> Path:
    """
    Utility to insert a new sql file between other sql files.
    For instance, let's say we have:

        0000_file1.sql
        0001_file2.sql
        0002_file3.sql
        0003_file4.sql
        0004_file5.sql

    and we want to insert a new file between [bold]0002_file2.sql[/bold] and [bold]0003_file3.sql[/bold],
    by specifying a [bold]position[/bold] of 3 we will have:

        0000_file1.sql
        0001_file2.sql
        0002_file3.sql
        0003_new_file.sql
        0004_file4.sql
        0005_file5.sql
    """
    is_before = True
    sql_files = sorted(sql_dir.glob("*.sql"))
    for _file in sql_files:
        _curr_pos, *_file_name = _file.name.split("_")
        _curr_pos, _file_name = int(_curr_pos), "_".join(_file_name)
        if _curr_pos == position:
            is_before = False
        if not is_before:
            _file.rename(f"{sql_dir}/{_curr_pos + 1:04}_{_file_name}")

    _new_file = sql_dir / f"{position:04}_new_file.sql"
    _new_file.touch()
    return _new_file
