"""Export picker controller.

Owns the two-phase export flow: first a format choice, then a path entry.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from textual.containers import Vertical
from textual.widgets import Input, OptionList

from qmb.types import ExportFormat

if TYPE_CHECKING:
    from qmb.tui.app import QueryResultApp


EXPORT_OPTIONS: list[tuple[ExportFormat, str, str]] = [
    (ExportFormat.CSV, "CSV (.csv)", ".csv"),
    (ExportFormat.JSON, "JSON (.json)", ".json"),
    (ExportFormat.PARQUET, "Parquet (.parquet)", ".parquet"),
]


class ExportController:
    """Controls the inline export picker."""

    def __init__(self, app: QueryResultApp) -> None:
        self.app = app
        self.filtered_indices: list[int] = []
        self.format: ExportFormat | None = None

    # -- open / populate ----------------------------------------------------

    def open(self) -> None:
        self.format = None
        picker = self.app.query_one("#export-picker", Vertical)
        inp = self.app.query_one("#export-filter", Input)
        opt = self.app.query_one("#export-list", OptionList)
        inp.display = False
        opt.display = True
        picker.display = True
        self.populate("")
        opt.focus()

    def populate(self, query: str) -> None:
        opt = self.app.query_one("#export-list", OptionList)
        opt.clear_options()
        self.filtered_indices.clear()
        q = query.strip().lower()
        for i, (_, label, _) in enumerate(EXPORT_OPTIONS):
            if not q or q in label.lower():
                opt.add_option(label)
                self.filtered_indices.append(i)
        if self.filtered_indices:
            opt.highlighted = 0

    # -- filter input handlers ---------------------------------------------

    def on_filter_changed(self, value: str) -> None:
        if self.format is not None:
            return
        self.populate(value)

    def on_filter_submitted(self) -> None:
        if self.format is None:
            opt = self.app.query_one("#export-list", OptionList)
            if self.filtered_indices and opt.highlighted is not None:
                self.select_format(opt.highlighted)
            return
        # Phase 2: path submitted -> do the export
        inp = self.app.query_one("#export-filter", Input)
        export_format = self.format
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        ext = next(e for f, _, e in EXPORT_OPTIONS if f == export_format)
        path = Path(inp.value or f"{ts}{ext}")
        self.app._dismiss_picker()
        # Lazy resolve through the app module so tests can monkeypatch
        # ``qmb.tui.app.export_results``.
        from qmb.tui import app as _app_module

        try:
            count = self._export(export_format, path, _app_module)
            self.app._info(f"Exported {count:,} rows to {path}")
        except Exception as exc:
            self.app._error(f"Export failed: {exc}")

    # -- selection ---------------------------------------------------------

    def on_option_selected(self, option_index: int) -> None:
        self.select_format(option_index)

    def select_format(self, option_idx: int) -> None:
        if option_idx < 0 or option_idx >= len(self.filtered_indices):
            return
        i = self.filtered_indices[option_idx]
        fmt, _, ext = EXPORT_OPTIONS[i]
        self.format = fmt
        # Switch to path entry phase
        opt = self.app.query_one("#export-list", OptionList)
        opt.display = False
        inp = self.app.query_one("#export-filter", Input)
        inp.display = True
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        inp.placeholder = "Output path…"
        inp.value = f"{ts}{ext}"
        inp.focus()

    # -- quick export ------------------------------------------------------

    def quick_export(self, fmt: ExportFormat, ext: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = Path(f"{ts}{ext}")
        from qmb.tui import app as _app_module

        try:
            count = self._export(fmt, path, _app_module)
            self.app._info(f"Exported {count:,} rows to {path}")
        except Exception as e:
            self.app._error(f"Export failed: {e}")

    def _export(self, fmt: ExportFormat, path: Path, app_module) -> int:
        if self.app.result_source is not None:
            return app_module.export_rows(
                self.app.result_source.iter_rows(),
                self.app.handle.schema_fields,
                fmt,
                path,
            )
        return app_module.export_results(self.app.bq_client, self.app.handle, fmt, path)
