import unittest

from digital_trade_foundation.air_exception.acceptance import run


class AirAcceptanceTest(unittest.TestCase):
    def test_offline_acceptance(self):
        result = run()
        self.assertEqual("ok", result["status"])
        self.assertTrue(result["audit_valid"])
        self.assertTrue(result["conserved"])
        self.assertTrue(result["scan_replayed"])
        self.assertTrue(result["decision_late"])
        self.assertFalse(result["freeze_scope_expanded"])
        self.assertEqual("SEG2", result["recovery_target"])
        self.assertTrue(result["restart_consistent"])
        self.assertTrue(result["declaration_restricted"])
        self.assertTrue(result["declaration_visible"])


if __name__ == "__main__":
    unittest.main()
