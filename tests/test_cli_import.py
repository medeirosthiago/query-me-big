import importlib


def test_cli_module_imports_cleanly() -> None:
    module = importlib.import_module("qmb.cli")

    assert module.app.info.name == "qmb"
