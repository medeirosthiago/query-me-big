"""Archived-jobs picker controller.

Owns the inline picker for the local qmb job archive (~/.qmb/jobs):
fetching records, filtering, selection, and swapping the active TUI
state to an archived job's preview.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual.containers import Vertical
from textual.widgets import Input, OptionList

from qmb.jobs.models import JobRecord
from qmb.types import fmt_bytes

if TYPE_CHECKING:
    from qmb.tui.app import QueryResultApp


class JobsController:
    """Controls the inline archived-jobs picker."""

    def __init__(self, app: QueryResultApp) -> None:
        self.app = app
        self.records: list[JobRecord] = []
        self.filtered_indices: list[int] = []

    # -- load --------------------------------------------------------------

    def load_and_open(self) -> None:
        # Local FS scan; cheap enough to do synchronously.
        from qmb.jobs.store import JobStore

        try:
            self.records = JobStore().list()
        except Exception as exc:  # pragma: no cover - defensive
            self.app._error(f"Failed to read job archive: {exc}")
            return

        if not self.records:
            self.app._warn("No archived qmb jobs found")
            return
        self.open()

    # -- open / populate ---------------------------------------------------

    def open(self) -> None:
        picker = self.app.query_one("#jobs-picker", Vertical)
        inp = self.app.query_one("#jobs-filter", Input)
        inp.value = ""
        picker.display = True
        inp.focus()
        self.app.call_after_refresh(self.populate, "")

    def populate(self, query: str) -> None:
        opt = self.app.query_one("#jobs-list", OptionList)
        opt.clear_options()
        self.filtered_indices.clear()
        q = query.strip().lower()
        avail = (opt.size.width or self.app.size.width) - 6  # border/padding/scrollbar
        for i, record in enumerate(self.records):
            date_str = f"{record.created_at:%Y-%m-%d %H:%M}"
            label = record.source.label
            job_id = record.qmb_job_id
            short_id = job_id.rsplit("_", 1)[-1]
            if q and not any(
                q in s
                for s in (
                    label.lower(),
                    job_id.lower(),
                    short_id.lower(),
                    date_str,
                )
            ):
                continue
            prefix = (
                f"{date_str} · {record.total_rows:,} rows · "
                f"{fmt_bytes(record.bytes_processed)} · "
            )
            remaining = max(avail - len(prefix), 20)
            tail = f"{label} [{short_id}]"
            if len(tail) > remaining:
                tail = tail[: remaining - 3] + "..."
            opt.add_option(f"{prefix}{tail}")
            self.filtered_indices.append(i)
        if self.filtered_indices:
            opt.highlighted = 0

    # -- input handlers ----------------------------------------------------

    def on_filter_changed(self, value: str) -> None:
        self.populate(value)

    def on_filter_submitted(self) -> None:
        opt = self.app.query_one("#jobs-list", OptionList)
        if self.filtered_indices and opt.highlighted is not None:
            self.select(opt.highlighted)

    def on_option_selected(self, option_index: int) -> None:
        self.select(option_index)

    def select(self, option_idx: int) -> None:
        if option_idx < 0 or option_idx >= len(self.filtered_indices):
            return
        record = self.records[self.filtered_indices[option_idx]]
        self.app._dismiss_picker()
        self.app._open_archived_job(record)

    def on_resize(self) -> None:
        if self.app.query_one("#jobs-picker", Vertical).display:
            inp = self.app.query_one("#jobs-filter", Input)
            self.populate(inp.value)
