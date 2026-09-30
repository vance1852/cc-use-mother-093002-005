import unittest

from digital_trade_foundation.api import route
from digital_trade_foundation.service import DomainService
from digital_trade_foundation.storage import Database


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.service = DomainService(self.database)

    def tearDown(self):
        self.database.close()

    def test_health_is_available_without_actor(self):
        status, payload = route(self.service, "GET", "/health", None)
        self.assertEqual(200, status)
        self.assertEqual("ok", payload["status"])

    def test_unknown_route_returns_404(self):
        status, payload = route(self.service, "GET", "/missing", None)
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", payload["error"])

    def test_invalid_json_shape_returns_400(self):
        status, payload = route(self.service, "POST", "/organizations", {"request_id": "x"},
                                {"X-Actor-Id": "bootstrap"})
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", payload["error"])


if __name__ == "__main__":
    unittest.main()
