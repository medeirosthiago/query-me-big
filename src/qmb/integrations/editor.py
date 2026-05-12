"""Open content in ``$EDITOR`` (nvim by default) with a temp file.

Split into two small pure helpers so the TUI keeps the Textual-specific
``app.suspend()`` context manager around the actual ``subprocess.run`` call.

- ``temp_file_for_editor`` — write content to a temp file, yield the path,
  delete it on exit (even if the caller errors out mid-editor).
- ``build_editor_command`` — assemble the argv to launch ``$EDITOR`` on the
  temp file, adding ``-R`` for nvim/vim/vi in read-only mode.
"""

from __future__ import annotations

import os
import shlex
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

_READ_ONLY_EDITORS = frozenset({"nvim", "vim", "vi"})


@contextmanager
def temp_file_for_editor(
    content: str, *, suffix: str, prefix: str
) -> Iterator[str]:
    """Create a temp file containing *content*, yield its path, delete on exit."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=suffix, prefix=prefix, delete=False
    ) as f:
        f.write(content)
        tmp_path = f.name
    try:
        yield tmp_path
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def build_editor_command(tmp_path: str, *, read_only: bool) -> list[str]:
    """Return the editor command argv for the given temp file.

    Reads ``$EDITOR`` (defaulting to ``nvim``) and appends ``-R`` when
    *read_only* is true and the editor is nvim/vim/vi.
    """
    editor = os.environ.get("EDITOR", "nvim")
    cmd = shlex.split(editor)
    exe = Path(cmd[0]).name

    if read_only and exe in _READ_ONLY_EDITORS:
        cmd.append("-R")

    cmd.append(tmp_path)
    return cmd
