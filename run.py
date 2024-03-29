import asyncio
import logging
import sys
import tempfile
from pathlib import Path

import asyncpg
from faker import Faker
from piou import Cli, Derived, Option

from padmy.anonymize import anonymize_db
from padmy.config import Config
from padmy.db import pretty_print_stats, pprint_compared_dbs, Database
from padmy.logs import setup_logging, logs
from padmy.migration import migration
from padmy.sampling import sample_database, copy_database
from padmy.utils import get_pg_root, get_pg_root_from, init_connection
from padmy.env import CONSOLE

cli = Cli("Padmy utility commands")

cli.add_option("-v", "--verbose", help="Verbosity")
cli.add_option("-vv", "--verbose2", help="Increased verbosity")

cli.add_command_group(migration)


def on_process(verbose: bool = False, verbose2: bool = False):
    setup_logging(logging.DEBUG if verbose2 else logging.INFO if verbose else logging.WARNING)


cli.set_options_processor(on_process)


async def get_explored_db(pg_url: str, db_name: str, schemas: list[str]):
    db = Database(name=db_name)
    logs.info(f"Gathering information about {db_name!r}...")
    async with asyncpg.create_pool(f"{pg_url}/{db_name}", init=init_connection) as pool:
        await db.explore(pool, schemas)
    return db


@cli.command(cmd="anonymize", help="Run anonymization")
async def ano_main(
    pg_url: str = Derived(get_pg_root),
    db_name: str = Option(..., "--db", help="Database to anonymize"),
    config_path: Path = Option(..., "-f", "--file", help="Path to the configuration file"),
):
    config = Config.load_from_file(config_path)
    faker = Faker()
    async with asyncpg.create_pool(f"{pg_url}/{db_name}") as pool:
        await anonymize_db(pool, config, faker)


@cli.command(cmd="sample", help="Sample database")
async def sample_main(
    from_db: str = Option(..., "--db", help="Database to migrate from"),
    to_db: str = Option(..., "--db-to", help="Database to migrate to"),
    schemas: list[str] | None = Option(None, "--schemas", help="Schemas to samples"),
    sample: int | None = Option(None, "--sample", help="Sample size"),
    config_path: Path | None = Option(None, "-f", "--file", help="Path to the configuration file"),
    copy_db: bool = Option(False, "--copy-db", help="Copy database before sampling"),
    # pg_url: str = Derived(get_pg_root),
    pg_from: str = Derived(get_pg_root_from("from")),
    pg_to: str = Derived(get_pg_root_from("to")),
    progress: bool = Option(False, "--progress", help="Show sampling progress"),
):
    """
    Create a copy of a given database and inserts a sample to a new database
    """

    if config_path:
        config = Config.load_from_file(config_path)
    elif sample is not None and schemas is not None:
        config = Config.load(sample, schemas=schemas)
    else:
        raise NotImplementedError("Please specify either a sample size and schemas or a config path")

    _schemas = [x.schema for x in config.schemas]
    if copy_db:
        logs.info(f"Copying {from_db!r} to {to_db!r}")
        try:
            copy_database(
                from_pg_uri=pg_from,
                to_pg_uri=pg_to,
                from_db=from_db,
                to_db=to_db,
                schemas=_schemas,
            )
        except Exception as e:
            logs.error(e)
            sys.exit(1)
        else:
            logs.info("Done!")

    # TODO clean if error
    conn = await asyncpg.connect(f"{pg_from}/{from_db}", statement_cache_size=0)
    target_conn = await asyncpg.connect(f"{pg_to}/{to_db}")

    db = await get_explored_db(pg_from, from_db, _schemas)
    db.load_config(config)

    pretty_print_stats(db)

    try:
        await sample_database(conn, target_conn, db, show_progress=progress)
    except Exception as e:
        raise e
    finally:
        await asyncio.wait_for(conn.close(), timeout=1)
        await asyncio.wait_for(target_conn.close(), timeout=1)

    new_db = await get_explored_db(pg_to, to_db, _schemas)

    pprint_compared_dbs(db, new_db)


@cli.command(cmd="copy-db", help="Copy a database schema to a new database")
def copy_main(
    from_db: str = Option(..., "--db", help="Database to migrate from"),
    to_db: str = Option(..., "--db-to", help="Database to migrate to"),
    schemas: list[str] = Option(..., "--schemas", help="Schemas to samples"),
    pg_from: str = Derived(get_pg_root_from("from")),
    pg_to: str = Derived(get_pg_root_from("to")),
):
    from padmy.sampling import copy_database

    logs.info(f"Copying {from_db!r} to {to_db!r}")
    copy_database(
        from_pg_uri=pg_from,
        to_pg_uri=pg_to,
        from_db=from_db,
        to_db=to_db,
        schemas=schemas,
        drop_public=False,
    )
    logs.info("Done!")


@cli.command(cmd="analyze", help="Analyze database")
async def analyze_main(
    db_name: str = Option(..., "--db", help="Database to explore"),
    pg_url: str = Derived(get_pg_root),
    schemas: list[str] = Option(..., "--schemas", help="Schemas to analyze"),
    show_graphs: bool = Option(False, "--show-graphs", help="Show graphs of the database"),
    port: int = Option(5555, "--graph-port", help="Port for the graph"),
):
    """
    Show information about a database
    """
    from padmy.db import pretty_print_stats

    db = await get_explored_db(pg_url, db_name, schemas)

    if not show_graphs:
        pretty_print_stats(db)
        return

    from padmy.sampling.viz import run_simple_app

    run_simple_app(db, port=port)


@cli.command(cmd="compare", help="Compare 2 databases")
async def compare_db_main(
    from_db: str = Option(..., "--db", help="Database to explore"),
    to_db: str = Option(..., "--db-to", help="Database to explore"),
    pg_url: str = Derived(get_pg_root),
    schemas: list[str] = Option(..., "--schemas", help="Schemas to analyze"),
):
    from padmy.db import pprint_compared_dbs

    db1, db2 = await asyncio.gather(
        get_explored_db(pg_url, from_db, schemas),
        get_explored_db(pg_url, to_db, schemas),
    )

    pprint_compared_dbs(db1, db2)


@cli.command(cmd="schema-diff", help="Compare the schemas of 2 databases")
def schema_diff(
    pg_from: str = Derived(get_pg_root_from("from")),
    pg_to: str = Derived(get_pg_root_from("to")),
    database: str = Option(..., "--db", help="Database to compare"),
    schemas: list[str] = Option(..., "--schemas", help="Schemas to analyze"),
    no_privileges: bool = Option(False, "-x", "--no-privileges", help="Exclude privileges from the dump"),
):
    from padmy.compare import compare_databases

    with tempfile.TemporaryDirectory() as dump_dir:
        _dump_dir = Path(dump_dir)
        diff = compare_databases(
            pg_from,
            to_pg_url=pg_to,
            schemas=schemas,
            database=database,
            dump_dir=_dump_dir,
            no_privileges=no_privileges,
        )
    if diff is not None:
        CONSOLE.print("[orange]Differences found[/orange]")
        CONSOLE.print("\n".join(diff))
    else:
        CONSOLE.print("[green]No differences found[/green]")


def run():
    cli.run()


if __name__ == "__main__":
    run()
