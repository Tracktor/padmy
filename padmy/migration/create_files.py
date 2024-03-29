import textwrap
import time
import uuid
from pathlib import Path

from rich.markup import escape
from rich.prompt import Prompt

from padmy.logs import logs
from .utils import get_files, iter_migration_files
from .config import MigrationConfig
from padmy.env import CONSOLE


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

    _base_name = f"{int(time.time())}-{str(uuid.uuid4())[:8]}"
    CONSOLE.print(f"\nCreating new migration file ([green]{escape(_base_name)}[/green]):\n")

    last_migration = _get_last_migration_name(folder)
    logs.debug(f"Last migration files: {last_migration}")
    author = _get_user_email()
    logs.debug(f"User email: {author}")

    up_file = folder / Path(f"{_base_name}-up.sql")
    down_file = folder / Path(f"{_base_name}-down.sql")

    _header = [
        f"-- Prev-file: {last_migration or ''}",
        f"-- Author: {author or ''}",
    ]
    if version:
        _header.append(f"-- Version: {version}")

    file_header = textwrap.dedent("\n".join(_header)).strip()

    up_file.write_text(file_header)
    down_file.write_text(file_header.replace("-up", "-down"))

    CONSOLE.print("\nNew files created!\n")
    return up_file, down_file
