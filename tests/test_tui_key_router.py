from qmb.tui.key_router import PendingKeyRouter


def test_router_has_no_pending_initially() -> None:
    router = PendingKeyRouter()
    assert router.pending is None
    assert router.is_pending("y") is False


def test_router_start_records_pending_key() -> None:
    router = PendingKeyRouter()
    router.start("y")
    assert router.pending == "y"
    assert router.is_pending("y") is True
    assert router.is_pending("x") is False


def test_router_clear_resets_pending() -> None:
    router = PendingKeyRouter()
    router.start("g")
    router.clear()
    assert router.pending is None
    assert router.is_pending("g") is False


def test_router_uses_default_timeout() -> None:
    assert PendingKeyRouter().timeout == 0.4
    assert PendingKeyRouter(timeout=1.0).timeout == 1.0
