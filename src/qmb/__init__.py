"""Query Me Big – BigQuery CLI with Textual TUI."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("qmb")
except PackageNotFoundError:  # pragma: no cover - editable install fallback
    __version__ = "0.0.0+dev"

__all__ = ["__version__"]
