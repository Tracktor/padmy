from importlib import metadata

from padmy.run import run

try:
    __version__ = metadata.version("padmy")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = (
    "__version__",
    "run",
)
