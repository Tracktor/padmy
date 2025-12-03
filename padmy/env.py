import os
from pathlib import Path
from rich.console import Console

PG_DATABASE = os.getenv("PG_DATABASE", "postgres")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

# SSL/TLS Configuration for mTLS
PG_SSL_MODE = os.getenv("PGSSLMODE")
# Convert to Path only if the value is not empty and not a special value like "system"
_ssl_ca = os.getenv("PGSSLROOTCERT")
PG_SSL_CA = Path(_ssl_ca) if _ssl_ca and _ssl_ca != "system" else None
_ssl_cert = os.getenv("PGSSLCERT")
PG_SSL_CERT = Path(_ssl_cert) if _ssl_cert else None
_ssl_key = os.getenv("PGSSLKEY")
PG_SSL_KEY = Path(_ssl_key) if _ssl_key else None
PG_SSL_PASSWORD = os.getenv("PGSSLPASSWORD")

# Migration
SQL_DIR = os.getenv("SQL_DIR")
MIGRATION_DIR = os.getenv("MIGRATION_DIR")

_PADMY_FOLDER = Path(os.getenv("PADMY_FOLDER", Path.home() / ".padmy"))
PADMY_CONFIG = _PADMY_FOLDER / "config.json"


CONSOLE = Console(markup=True, highlight=False)
