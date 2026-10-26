"""Tests for the named remote archives config (``[remote_archive]`` + ``[[remote_archives]]``)."""

from __future__ import annotations

from pathlib import Path

import pytest

from qmb import config
from qmb.config import ConfigError, RemoteArchiveConfig


@pytest.fixture
def write_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    path = tmp_path / "config.toml"
    monkeypatch.setattr(config, "default_config_path", lambda: path)

    def write(text: str) -> None:
        path.write_text(text, encoding="utf-8")

    return write


EXTRAS = """
[[remote_archives]]
name = "mcp-staging"
uri = "gs://example-bucket/mcp/staging/"

[[remote_archives]]
name = "mcp"
uri = "gs://example-bucket/mcp/production/"
"""


def test_no_config_means_no_archives() -> None:
    assert config.remote_archives() == []


def test_primary_only_defaults_name_to_remote(write_config) -> None:
    write_config('[remote_archive]\nuri = "gs://bucket/qmb/"\n')

    assert config.remote_archives() == [
        RemoteArchiveConfig(name="remote", uri="gs://bucket/qmb/", primary=True)
    ]


def test_primary_and_extras_in_order(write_config) -> None:
    write_config(
        '[remote_archive]\nuri = "gs://example-bucket/qmb/"\nname = "qmb"\n' + EXTRAS
    )

    assert config.remote_archives() == [
        RemoteArchiveConfig(name="qmb", uri="gs://example-bucket/qmb/", primary=True),
        RemoteArchiveConfig(
            name="mcp-staging", uri="gs://example-bucket/mcp/staging/"
        ),
        RemoteArchiveConfig(
            name="mcp", uri="gs://example-bucket/mcp/production/"
        ),
    ]
    # The single-archive helper is unaffected by the extras.
    assert config.remote_archive_uri() == "gs://example-bucket/qmb/"


def test_extras_without_primary(write_config) -> None:
    write_config(EXTRAS)

    archives = config.remote_archives()

    assert [archive.name for archive in archives] == ["mcp-staging", "mcp"]
    assert not any(archive.primary for archive in archives)
    assert config.remote_archive_uri() is None


def test_env_and_destination_override_only_the_primary(
    write_config, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_config('[remote_archive]\nuri = "gs://bucket/qmb/"\nname = "qmb"\n' + EXTRAS)
    monkeypatch.setenv("QMB_REMOTE_ARCHIVE_URI", "gs://env-bucket/qmb/")

    archives = config.remote_archives()
    assert archives[0] == RemoteArchiveConfig(
        name="qmb", uri="gs://env-bucket/qmb/", primary=True
    )
    assert [archive.uri for archive in archives[1:]] == [
        "gs://example-bucket/mcp/staging/",
        "gs://example-bucket/mcp/production/",
    ]

    archives = config.remote_archives("gs://cli-bucket/qmb/")
    assert archives[0].uri == "gs://cli-bucket/qmb/"
    assert len(archives) == 3


def test_remote_archive_named_finds_primary_and_extras(write_config) -> None:
    write_config('[remote_archive]\nuri = "gs://bucket/qmb/"\n' + EXTRAS)

    assert config.remote_archive_named("remote").primary is True
    assert config.remote_archive_named("mcp").uri == (
        "gs://example-bucket/mcp/production/"
    )
    with pytest.raises(ConfigError, match="Unknown remote archive 'nope'.*mcp-staging"):
        config.remote_archive_named("nope")


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (
            '[[remote_archives]]\nname = "Gusta"\nuri = "gs://b/p/"\n',
            r"\[\[remote_archives\]\] entry 1 \('Gusta'\): name must match",
        ),
        (
            '[[remote_archives]]\nname = "local"\nuri = "gs://b/p/"\n',
            r"entry 1 \('local'\): name 'local' is reserved",
        ),
        (
            '[[remote_archives]]\nuri = "gs://b/p/"\n',
            r"entry 1: name must match",
        ),
        (
            '[[remote_archives]]\nname = "' + "a" * 33 + '"\nuri = "gs://b/p/"\n',
            r"name must match",
        ),
        (
            '[[remote_archives]]\nname = "gq"\nuri = "s3://b/p/"\n',
            r"entry 1 \('gq'\): uri must look like gs://bucket/prefix/",
        ),
        (
            '[[remote_archives]]\nname = "gq"\n',
            r"entry 1 \('gq'\): uri must look like",
        ),
        (
            '[remote_archive]\nuri = "gs://b/qmb/"\nname = "Bad Name"\n',
            r"\[remote_archive\]: name must match",
        ),
        (
            '[remote_archive]\nuri = "gs://b/qmb/"\n'
            '[[remote_archives]]\nname = "remote"\nuri = "gs://b/other/"\n',
            r"Duplicate remote archive name 'remote'",
        ),
        (
            '[[remote_archives]]\nname = "a"\nuri = "gs://b/p/"\n'
            '[[remote_archives]]\nname = "b"\nuri = "gs://b/p"\n',
            r"Remote archives 'a' and 'b' share the same uri",
        ),
        (
            '[remote_archive]\nuri = "gs://b/qmb/"\n'
            '[[remote_archives]]\nname = "copy"\nuri = "gs://b/qmb/"\n',
            r"'remote' and 'copy' share the same uri",
        ),
        ('remote_archives = "gs://b/p/"\n', r"must be an array of tables"),
    ],
)
def test_invalid_archive_config_is_rejected(write_config, text: str, message: str) -> None:
    write_config(text)

    with pytest.raises(ConfigError, match=message):
        config.remote_archives()
