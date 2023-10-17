import logging
from rich.logging import RichHandler

logs = logging.getLogger("padmy")


def setup_logging(level: int):
    logging.basicConfig(
        datefmt="%H:%M:%S",
        format="%(message)s",
        handlers=[RichHandler(rich_tracebacks=False, show_path=False)],
    )
    logs.setLevel(level)
