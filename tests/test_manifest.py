from pathlib import Path

from qmb.dbt.manifest import discover_manifest_path


def test_discover_manifest_path_searches_parent_directories(
    tmp_path: Path, monkeypatch
) -> None:
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("{}", encoding="utf-8")

    nested = tmp_path / "analytics" / "models" / "staging"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)

    assert discover_manifest_path() == manifest_path
