"""Application/orchestration layer for qmb.

This package owns the pure-Python orchestration logic that ties together
SQL resolution, query execution, and export. It must not import from
Typer, Click, Rich, or Textual — those concerns belong to the CLI/TUI
layers.
"""
