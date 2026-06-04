"""Tests for the on-disk regions cache used by ``--via-info-schema``."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from qmb.bigquery import regions_cache


def test_load_returns_none_when_file_missing(tmp_path: Path) -> None:
    assert regions_cache.load_regions("some-project", cache_dir=tmp_path) is None


def test_save_then_load_roundtrip(tmp_path: Path) -> None:
    regions_cache.save_regions(
        "data-platform-production-3573",
        ["us-west1", "US"],
        cache_dir=tmp_path,
    )

    loaded = regions_cache.load_regions(
        "data-platform-production-3573", cache_dir=tmp_path
    )

    # Sorted & deduplicated by the cache layer.
    assert loaded == ["US", "us-west1"]


def test_load_returns_none_when_cache_is_stale(tmp_path: Path) -> None:
    cache_path = tmp_path / "stale-project.json"
    cache_path.write_text(
        json.dumps(
            {
                "project": "stale-project",
                "fetched_at": (datetime.now(UTC) - timedelta(days=60)).isoformat(),
                "locations": ["us-west1"],
            }
        )
    )

    assert regions_cache.load_regions("stale-project", cache_dir=tmp_path) is None


def test_load_returns_none_for_corrupt_file(tmp_path: Path) -> None:
    (tmp_path / "broken.json").write_text("{ this is not json")

    assert regions_cache.load_regions("broken", cache_dir=tmp_path) is None


def test_unsafe_project_id_is_not_persisted(tmp_path: Path) -> None:
    regions_cache.save_regions("../etc/passwd", ["us-west1"], cache_dir=tmp_path)

    # Refuse to write outside the cache dir for unsafe ids.
    assert not any(tmp_path.iterdir())
    assert regions_cache.load_regions("../etc/passwd", cache_dir=tmp_path) is None
