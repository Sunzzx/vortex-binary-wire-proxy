"""Runner smoke tests for vortex-binary-wire-proxy."""

from __future__ import annotations

from vortex_binary_wire_proxy.runner import smoke


def test_smoke_all_passed():
    result = smoke()
    assert result["all_passed"] is True
    assert result["frames"] == 1
    assert result["proxy_accepted"] == 1
    assert result["frame_ok"] is True
    assert result["pool_reuse"] is True
