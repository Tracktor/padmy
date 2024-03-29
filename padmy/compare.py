import difflib
import filecmp
from pathlib import Path
from typing import Iterator
from padmy.utils import pg_dump, temp_pg_env


def compare_databases(
    from_pg_url: str,
    database: str,
    schemas: list[str],
    dump_dir: Path,
    *,
    to_pg_url: str | None = None,
    db_to: str | None = None,
    no_privileges: bool = True,
) -> Iterator[str] | None:
    """
    Compare the schemas of 2 databases
    """
    dump_1 = dump_dir / f"{database}-from.sql"
    db_to = db_to or database
    dump_2 = dump_dir / f"{db_to}-to.sql"
    _cmd = ["-E", "utf8", "--schema-only"]
    if no_privileges:
        _cmd.append("--no-privileges")

    with temp_pg_env(from_pg_url):
        pg_dump(database, schemas, dump_path=str(dump_1), options=_cmd, get_env=False)
    _to_pg_url = to_pg_url or from_pg_url
    with temp_pg_env(_to_pg_url):
        pg_dump(db_to, schemas, dump_path=str(dump_2), options=_cmd, get_env=False)

    _no_diff = filecmp.cmp(dump_1, dump_2)

    if not _no_diff:
        diff = difflib.unified_diff(
            dump_1.read_text().split("\n"),
            dump_2.read_text().split("\n"),
            fromfile=dump_1.name,
            tofile=dump_2.name,
        )
        return diff
    return None
