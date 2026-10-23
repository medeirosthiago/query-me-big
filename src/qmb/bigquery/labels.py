"""BigQuery job label validation."""

import re
from collections.abc import Mapping

MAX_LABELS = 64

_KEY_PATTERN = re.compile(r"[a-z][a-z0-9_-]{0,62}\Z")
_VALUE_PATTERN = re.compile(r"[a-z0-9_-]{0,63}\Z")


def validate_labels(labels: Mapping[str, str]) -> dict[str, str]:
    """Return ``labels`` as a dict, raising ``ValueError`` if BigQuery would reject them.

    Keys are 1-63 chars, start with a lowercase letter and contain only
    lowercase letters, digits, ``_`` and ``-``. Values are 0-63 chars of
    the same alphabet. At most 64 labels per job.
    """
    if len(labels) > MAX_LABELS:
        raise ValueError(f"Too many labels: {len(labels)} (BigQuery allows at most {MAX_LABELS}).")
    validated: dict[str, str] = {}
    for key, value in labels.items():
        if not isinstance(key, str) or not _KEY_PATTERN.match(key):
            raise ValueError(
                f"Invalid label key {key!r}: use 1-63 lowercase letters, digits, '_' or '-', "
                "starting with a lowercase letter."
            )
        if not isinstance(value, str) or not _VALUE_PATTERN.match(value):
            raise ValueError(
                f"Invalid value {value!r} for label {key!r}: use 0-63 lowercase letters, "
                "digits, '_' or '-'."
            )
        validated[key] = value
    return validated
