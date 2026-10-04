import unittest
from datetime import datetime, timezone

from digital_trade_foundation.air_exception import AirExceptionService
from digital_trade_foundation.air_exception.api import route
from digital_trade_foundation.clock import FixedClock
from digital_trade_foundation.service import DomainService
from digital_trade_foundation.storage import Database


class AirApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.foundation = DomainService(
            self.database, FixedClock(datetime(2026, 10, 4, 8, 0, tzinfo=timezone.utc)))
        self.air = AirExceptionService(self.foundation)
        self.foundation.register_organization(request_id="org", actor_id="bootstrap",
                                              organization_id="o1", name="枢纽运营")
        self.foundation.register_actor(request_id="a-admin", actor_id="bootstrap", new_actor_id="admin",
                                       display_name="管理员", role="admin", organization_id="o1")
        for role in ("carrier", "sorter", "customs"):
            self.foundation.register_actor(request_id=f"a-{role}", actor_id="admin", new_actor_id=role,
                                           display_name=role, role=role, organization_id="o1")
        self.foundation.register_site(request_id="site", actor_id="admin", site_id="hub",
                                      organization_id="o1", name="枢纽", timezone_name="Asia/Shanghai")

    def tearDown(self):
        self.database.close()

    def to_request(self, method, path, body=None, actor="admin"):
        headers = {"X-Actor-Id": actor} if actor else {}
        return route(self.foundation, self.air, method, path, body, headers)

    def test_air_routes_require_actor_header(self):
        status, payload = self.to_request("GET", "/air/desk-board?site_id=hub", actor=None)
        self.assertEqual(403, status)
        self.assertEqual("permission_denied", payload["error"])

    def test_unknown_air_route_returns_404(self):
        status, payload = self.to_request("GET", "/air/unknown")
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", payload["error"])

    def test_invalid_body_returns_400(self):
        status, payload = self.to_request("POST", "/air/segments", {"request_id": "x"})
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", payload["error"])

    def test_foundation_routes_still_work(self):
        status, payload = self.to_request("GET", "/health")
        self.assertEqual(200, status)
        self.assertEqual("ok", payload["status"])

    def test_full_chain_over_http(self):
        status, segment = self.to_request("POST", "/air/segments", {
            "request_id": "seg", "site_id": "hub", "segment_id": "SEG1", "flight_no": "CA1",
            "origin": "HUB", "destination": "FRA", "departs_at": "2026-10-04T14:00:00Z",
            "arrives_at": "2026-10-04T20:00:00Z", "capacity_pieces": 10, "capacity_weight": 100},
            actor="carrier")
        self.assertEqual(201, status)
        status, replay = self.to_request("POST", "/air/segments", {
            "request_id": "seg", "site_id": "hub", "segment_id": "SEG1", "flight_no": "CA1",
            "origin": "HUB", "destination": "FRA", "departs_at": "2026-10-04T14:00:00Z",
            "arrives_at": "2026-10-04T20:00:00Z", "capacity_pieces": 10, "capacity_weight": 100},
            actor="carrier")
        self.assertEqual(200, status)
        self.assertTrue(replay["replayed"])
        self.assertEqual(segment["resource_id"], replay["resource_id"])

        status, _ = self.to_request("POST", "/air/waybills", {
            "request_id": "wb", "site_id": "hub", "waybill_id": "WB1", "origin": "HUB",
            "destination": "FRA", "declared_pieces": 2, "declared_weight": 20}, actor="carrier")
        self.assertEqual(201, status)
        status, _ = self.to_request("POST", "/air/waybills/WB1/packages", {
            "request_id": "scan", "packages": [
                {"package_id": "P1", "pieces": 1, "weight": 10},
                {"package_id": "P2", "pieces": 1, "weight": 10}]}, actor="carrier")
        self.assertEqual(201, status)
        status, arrival = self.to_request("POST", "/air/waybills/WB1/arrive",
                                          {"request_id": "arrive"}, actor="carrier")
        self.assertEqual(201, status)
        batch_id = arrival["resource_id"]

        status, _ = self.to_request("POST", "/air/decisions", {
            "request_id": "hold", "waybill_id": "WB1", "decision_type": "hold",
            "package_ids": ["P1"], "effective_at": "2026-10-04T07:00:00Z"}, actor="customs")
        self.assertEqual(201, status)
        status, _ = self.to_request("POST", "/air/decisions", {
            "request_id": "hold2", "waybill_id": "WB1", "decision_type": "hold",
            "package_ids": ["P1"], "effective_at": "2026-10-04T07:00:00Z"}, actor="sorter")
        self.assertEqual(403, status)

        status, board = self.to_request("GET", "/air/desk-board?site_id=hub", actor="customs")
        self.assertEqual(200, status)
        self.assertEqual(2, len(board["in_transit"]))
        self.assertEqual("inspection_due", board["todos"][0]["kind"])

        status, detail = self.to_request("GET", "/air/waybills/WB1", actor="customs")
        self.assertEqual(200, status)
        self.assertTrue(detail["conservation"]["conserved"])
        self.assertEqual(batch_id, next(b["batch_id"] for b in detail["batches"]
                                        if b["status"] == "open"))

    def test_declaration_payload_filtered_by_role(self):
        self.to_request("POST", "/air/waybills", {
            "request_id": "wb", "site_id": "hub", "waybill_id": "WB1", "origin": "HUB",
            "destination": "FRA", "declared_pieces": 1, "declared_weight": 10}, actor="carrier")
        status, _ = self.to_request("POST", "/air/waybills/WB1/declarations", {
            "request_id": "dec", "data": {"secret": "敏感申报"}}, actor="carrier")
        self.assertEqual(201, status)
        status, customs_view = self.to_request("GET", "/air/waybills/WB1/declarations", actor="customs")
        self.assertEqual(200, status)
        self.assertEqual("敏感申报", customs_view["items"][0]["payload"]["secret"])
        status, sorter_view = self.to_request("GET", "/air/waybills/WB1/declarations", actor="sorter")
        self.assertEqual(200, status)
        self.assertIsNone(sorter_view["items"][0]["payload"])
        self.assertTrue(sorter_view["items"][0]["restricted"])


if __name__ == "__main__":
    unittest.main()
