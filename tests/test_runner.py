import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from vortex_binary_wire_proxy.runner import ApiAssertionEngine

class TestVortexWireProxy(unittest.TestCase):
    def test_assertions_pass(self):
        res = ApiAssertionEngine.assert_response(200, 200, 45.0, 100.0)
        self.assertTrue(res["all_passed"])
        self.assertTrue(res["status_pass"])
        self.assertTrue(res["latency_pass"])

    def test_assertions_fail_status(self):
        res = ApiAssertionEngine.assert_response(500, 200, 10.0, 100.0)
        self.assertFalse(res["all_passed"])
        self.assertFalse(res["status_pass"])

    def test_assertions_fail_latency(self):
        res = ApiAssertionEngine.assert_response(200, 200, 150.0, 100.0)
        self.assertFalse(res["all_passed"])
        self.assertFalse(res["latency_pass"])

if __name__ == "__main__":
    unittest.main()
