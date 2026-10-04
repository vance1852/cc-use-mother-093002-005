import unittest

from digital_trade_foundation.api import route
from digital_trade_foundation.cargo import CargoService
from digital_trade_foundation.service import DomainService
from digital_trade_foundation.storage import Database


class CargoApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.service = DomainService(self.database)
        self.cargo = CargoService(self.database)
        self.headers = {"X-Actor-Id": "car"}
        self.service.register_organization(request_id="org", actor_id="bootstrap",
                                           organization_id="hub", name="枢纽")
        self.service.register_actor(request_id="adm", actor_id="bootstrap", new_actor_id="admin-1",
                                    display_name="管理员", role="admin", organization_id="hub")
        for req, aid, role in [
            ("ac", "car", "operator"), ("as", "sor", "operator"),
            ("aw", "wh", "operator"), ("ax", "cus", "reviewer"),
            ("ae", "exc", "operator"),
        ]:
            self.service.register_actor(request_id=req, actor_id="admin-1", new_actor_id=aid,
                                        display_name=aid, role=role, organization_id="hub")
        status, payload = self.call("POST", "/cargo/shipments", {
            "request_id": "sh", "master_waybill": "MWB-9", "origin": "PVG",
            "destination": "FRA", "promised_delivery_at": "2026-10-10T00:00:00Z"})
        self.assertEqual(201, status)
        self.sid = payload["data"]["shipment_id"]
        for req, aid, duty in [("ds", "sor", "sorting"), ("dw", "wh", "warehouse"),
                               ("dc", "cus", "customs"), ("de", "exc", "exception")]:
            self.call("POST", "/cargo/duties",
                      {"request_id": req, "shipment_id": self.sid,
                       "target_actor_id": aid, "duty": duty})

    def tearDown(self):
        self.database.close()

    def call(self, method, path, body=None, actor="car"):
        return route(self.service, method, path, body or {},
                     {"X-Actor-Id": actor}, cargo=self.cargo)

    def test_full_partial_release_flow_over_http(self):
        status, _ = self.call("POST", "/cargo/packages", {
            "request_id": "pk", "shipment_id": self.sid,
            "packages": [{"package_id": "K1", "quantity": 5},
                         {"package_id": "K2", "quantity": 7}]})
        self.assertEqual(201, status)
        status, payload = self.call("POST", "/cargo/arrivals", {
            "request_id": "ar", "shipment_id": self.sid,
            "package_ids": ["K1", "K2"], "at": "2026-10-01T08:00:00Z"})
        self.assertEqual(201, status)
        self.assertEqual(["K1", "K2"], payload["data"]["newly_arrived"])

        # 分拣无权签发海关决定
        status, payload = self.call("POST", "/cargo/decisions", {
            "request_id": "dx", "shipment_id": self.sid, "kind": "release",
            "package_ids": ["K1"]}, actor="sor")
        self.assertEqual(403, status)

        # 海关部分放行
        status, payload = self.call("POST", "/cargo/decisions", {
            "request_id": "d1", "shipment_id": self.sid, "kind": "release",
            "package_ids": ["K1"], "effective_at": "2026-10-01T09:00:00Z"}, actor="cus")
        self.assertEqual(201, status)
        status, payload = self.call("POST", "/cargo/decisions", {
            "request_id": "d2", "shipment_id": self.sid, "kind": "hold",
            "package_ids": ["K2"], "effective_at": "2026-10-01T09:00:00Z"}, actor="cus")
        self.assertEqual(201, status)

        status, payload = self.call(
            "GET", f"/cargo/shipments/{self.sid}/status", None, actor="exc")
        self.assertEqual(200, status)
        views = {p["package_id"]: p for b in payload["batches"] for p in b["packages"]}
        self.assertTrue(views["K1"]["regulatory"]["released"])
        self.assertTrue(views["K2"]["regulatory"]["held"])

    def test_sensitive_declaration_is_masked_for_non_customs(self):
        self.call("POST", "/cargo/packages", {
            "request_id": "pk", "shipment_id": self.sid,
            "packages": [{"package_id": "K1", "quantity": 5}]})
        status, payload = self.call("POST", "/cargo/declarations", {
            "request_id": "sd2", "shipment_id": self.sid, "package_ids": ["K1"],
            "payload": {"secret": 1}, "sensitive": True,
            "effective_at": "2026-10-01T08:00:00Z"})
        self.assertEqual(201, status)
        declaration_id = payload["data"]["declaration_id"]
        status, payload = self.call(
            "GET", f"/cargo/declarations/{declaration_id}", None, actor="sor")
        self.assertEqual(403, status)
        status, payload = self.call(
            "GET", f"/cargo/declarations/{declaration_id}", None, actor="cus")
        self.assertEqual(200, status)
        self.assertEqual(1, payload["payload"]["secret"])

    def test_delay_impact_endpoint(self):
        self.call("POST", "/cargo/packages", {
            "request_id": "pk", "shipment_id": self.sid,
            "packages": [{"package_id": "K1", "quantity": 5}]})
        status, payload = self.call(
            "GET", f"/cargo/shipments/{self.sid}/delay-impact?at=2026-10-01T10:00:00Z",
            None, actor="exc")
        self.assertEqual(200, status)
        self.assertEqual(self.sid, payload["shipment_id"])
        self.assertIn("recovery_plan", payload)

    def test_unknown_cargo_route_returns_404(self):
        status, payload = self.call("GET", "/cargo/nope", None)
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", payload["error"])


if __name__ == "__main__":
    unittest.main()
