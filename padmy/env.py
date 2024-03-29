import os
from pathlib import Path
from rich.console import Console

PG_DATABASE = os.getenv("PG_DATABASE", "postgres")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

# Migration
SQL_DIR = os.getenv("SQL_DIR")
MIGRATION_DIR = os.getenv("MIGRATION_DIR")

_PADMY_FOLDER = Path(os.getenv("PADMY_FOLDER", Path.home() / ".padmy"))
PADMY_CONFIG = _PADMY_FOLDER / "config.json"


CONSOLE = Console(markup=True, highlight=False)
