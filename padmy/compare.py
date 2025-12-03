import difflib
import filecmp
from pathlib import Path
from typing import Iterator

from padmy.utils import pg_dump, remove_restrict_clauses, PGConnectionInfo


def compare_databases(
    from_pg_url: str | PGConnectionInfo,
    database: str,
    schemas: list[str],
    dump_dir: Path,
    *,
    to_pg_url: str | PGConnectionInfo | None = None,
    db_to: str | None = None,
    no_privileges: bool = True,
    ignore_restrict: bool = True,
) -> Iterator[str] | None:
    """
    Compare the schemas of 2 databases.
    If ignore_restrict is True, ignores \restrict clauses in the
    diff ([PostgreSQL 17.6+](https://www.postgresql.org/docs/17/release-17-6.html#RELEASE-17-6-CHANGES) feature).

    """
    dump_1 = dump_dir / f"{database}-from.sql"
    db_to = db_to or database
    dump_2 = dump_dir / f"{db_to}-to.sql"
    _cmd = ["-E", "utf8", "--schema-only"]
    if no_privileges:
        _cmd.append("--no-privileges")

    # Determine source connection info
    pg_from_info = from_pg_url if isinstance(from_pg_url, PGConnectionInfo) else PGConnectionInfo.from_uri(from_pg_url)

    with pg_from_info.temp_env(include_database=False):
        pg_dump(database, schemas, dump_path=str(dump_1), options=_cmd)
        if ignore_restrict:
            remove_restrict_clauses(dump_1)

    # Determine target connection info (defaults to from_pg_url if not provided)
    if to_pg_url is not None:
        pg_to_info = to_pg_url if isinstance(to_pg_url, PGConnectionInfo) else PGConnectionInfo.from_uri(to_pg_url)
    else:
        pg_to_info = pg_from_info

    with pg_to_info.temp_env(include_database=False):
        pg_dump(db_to, schemas, dump_path=str(dump_2), options=_cmd)
        if ignore_restrict:
            remove_restrict_clauses(dump_2)

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
