import time
from pathlib import Path

from rich.markup import escape
from rich.prompt import Prompt

from padmy.env import CONSOLE
from padmy.logs import logs
from .migration import MigrationFile
from .config import MigrationConfig
from .utils import get_files, iter_migration_files, Header


def _get_user_email() -> str | None:
    _config = MigrationConfig.load()
    author = Prompt.ask("[blue]Author[/blue]", default=_config.author, console=CONSOLE)
    _config.author = author
    _config.save()
    return author


def _get_last_migration_name(folder: Path) -> str | None:
    """Returns the most recent migration files"""
    files = get_files(reverse=True, folder=folder)
    if not files:
        return None
    up_file, down_file = next(iter_migration_files(files))
    return up_file.path.name


def create_new_migration(folder: Path, version: str | None = None) -> tuple[Path, Path]:
    """
    Creates 2 new files, up and down
    """
    folder.mkdir(exist_ok=True, parents=True)

    _base_name = MigrationFile.generate_base_name(ts=int(time.time()))
    CONSOLE.print(f"\nCreating new migration file ([green]{escape(_base_name)}[/green]):\n")

    last_migration = _get_last_migration_name(folder)
    logs.debug(f"Last migration files: {last_migration}")
    author = _get_user_email()
    logs.debug(f"User email: {author}")

    up_file = folder / Path(f"{_base_name}-up.sql")
    down_file = folder / Path(f"{_base_name}-down.sql")

    _header = Header(last_migration, author, version).as_text()
    up_file.write_text(_header)
    down_file.write_text(_header.replace("-up", "-down"))

    CONSOLE.print("\nNew files created!\n")
    return up_file, down_file
