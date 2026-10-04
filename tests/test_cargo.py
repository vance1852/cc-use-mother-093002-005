import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from digital_trade_foundation.cargo import CargoService
from digital_trade_foundation.clock import FixedClock
from digital_trade_foundation.errors import ConflictError, PermissionDenied
from digital_trade_foundation.service import DomainService
from digital_trade_foundation.storage import Database


def build_environment(database: Database, clock) -> tuple[DomainService, CargoService, str]:
    base = DomainService(database, clock)
    cargo = CargoService(database, clock)
    base.register_organization(request_id="org", actor_id="bootstrap",
                               organization_id="hub", name="枢纽")
    base.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="adm",
                        display_name="管理员", role="admin", organization_id="hub")
    for req, aid, role in [
        ("ac", "car", "operator"), ("as", "sor", "operator"),
        ("aw", "wh", "operator"), ("ax", "cus", "reviewer"),
        ("ae", "exc", "operator"),
    ]:
        base.register_actor(request_id=req, actor_id="adm", new_actor_id=aid,
                            display_name=aid, role=role, organization_id="hub")
    created = cargo.create_shipment(
        request_id="sh", actor_id="car", master_waybill="MWB-1",
        origin="PVG", destination="FRA",
        promised_delivery_at="2026-10-10T00:00:00Z")
    sid = created.data["shipment_id"]
    for req, aid, duty in [("ds", "sor", "sorting"), ("dw", "wh", "warehouse"),
                           ("dc", "cus", "customs"), ("de", "exc", "exception")]:
        cargo.grant_duty(request_id=req, actor_id="car", shipment_id=sid,
                         target_actor_id=aid, duty=duty)
    return base, cargo, sid


class CargoFixture(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.clock = FixedClock(datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc))
        self.base, self.cargo, self.sid = build_environment(self.database, self.clock)
        self.cargo.register_packages(
            request_id="pk", actor_id="car", shipment_id=self.sid,
            packages=[{"package_id": "R1", "quantity": 10, "weight": 100},
                      {"package_id": "R2", "quantity": 6, "weight": 60}])
        self.cargo.register_location(request_id="loc", actor_id="wh", location_id="L1",
                                     code="A1", capacity_qty=8)
        self.cargo.register_segment(request_id="seg", actor_id="car", segment_id="S1",
                                    flight_no="CA1", origin="PVG", destination="FRA",
                                    departs_at="2026-10-02T00:00:00Z",
                                    capacity_qty=20, capacity_weight=1000)

    def tearDown(self):
        self.database.close()

    def _arrive(self):
        self.cargo.arrive_packages(request_id="ar", actor_id="car", shipment_id=self.sid,
                                   package_ids=["R1", "R2"], at="2026-10-01T08:00:00Z")


class ConservationTest(CargoFixture):
    def test_split_quantities_must_equal_source(self):
        self._arrive()
        with self.assertRaises(ConflictError):
            self.cargo.split_packages(
                request_id="split-bad", actor_id="sor", shipment_id=self.sid, reason="x",
                splits=[{"source_package_id": "R2",
                         "children": [{"package_id": "A", "quantity": 3},
                                      {"package_id": "B", "quantity": 2}]}])

    def test_split_then_merge_conserves_quantity(self):
        self._arrive()
        self.cargo.split_packages(
            request_id="sp", actor_id="sor", shipment_id=self.sid, reason="分流",
            splits=[{"source_package_id": "R2",
                     "children": [{"package_id": "R2A", "quantity": 2, "weight": 20},
                                  {"package_id": "R2B", "quantity": 4, "weight": 40}]}])
        # 把 R1 与 R2A 合并：10+2=12，叶节点总量仍是 16
        self.cargo.handoff(request_id="h0", actor_id="car", shipment_id=self.sid,
                           kind="intake", from_actor_id="car", to_actor_id="wh",
                           package_ids=["R1", "R2A", "R2B"], at="2026-10-01T09:00:00Z")
        merged = self.cargo.merge_packages(
            request_id="mg", actor_id="sor", shipment_id=self.sid,
            source_package_ids=["R1", "R2A"], reason="集拼")
        self.assertEqual(12.0, merged.data["quantity"])
        status = self.cargo.shipment_status(actor_id="exc", shipment_id=self.sid)
        leaf = sum(p["quantity"] for b in status["batches"] for p in b["packages"]
                   if p["live"])
        self.assertEqual(16.0, leaf)


class IdempotencyTest(CargoFixture):
    def test_duplicate_scan_does_not_double_count_arrival(self):
        first = self.cargo.arrive_packages(
            request_id="a1", actor_id="car", shipment_id=self.sid,
            package_ids=["R1", "R2"], at="2026-10-01T08:00:00Z", scan_token="TOK-1")
        second = self.cargo.arrive_packages(
            request_id="a2", actor_id="car", shipment_id=self.sid,
            package_ids=["R1", "R2"], at="2026-10-01T08:00:00Z", scan_token="TOK-1")
        self.assertEqual(["R1", "R2"], first.data["newly_arrived"])
        self.assertTrue(second.data.get("replayed_scan"))
        status = self.cargo.shipment_status(actor_id="exc", shipment_id=self.sid)
        arrived = [p for b in status["batches"] for p in b["packages"] if p["arrived_at"]]
        self.assertEqual(2, len(arrived))

    def test_same_request_id_replays_receipt(self):
        first = self.cargo.register_location(request_id="rl", actor_id="wh",
                                             location_id="L9", code="Z9", capacity_qty=5)
        second = self.cargo.register_location(request_id="rl", actor_id="wh",
                                              location_id="L9", code="Z9", capacity_qty=5)
        self.assertFalse(first.replayed)
        self.assertTrue(second.replayed)

    def test_repeated_placement_does_not_exceed_capacity(self):
        self._arrive()
        self.cargo.lease_space(request_id="le", actor_id="wh", shipment_id=self.sid,
                               location_id="L1", quantity=10)
        # L1 容量 8，R1 数量 10，首次上架应被拒
        with self.assertRaises(ConflictError):
            self.cargo.place_packages(request_id="pl1", actor_id="wh", shipment_id=self.sid,
                                      package_ids=["R1"], location_id="L1")

    def test_repeat_same_placement_is_idempotent(self):
        self.cargo.register_location(request_id="loc2", actor_id="wh", location_id="L2",
                                     code="A2", capacity_qty=50)
        self._arrive()
        self.cargo.place_packages(request_id="p1", actor_id="wh", shipment_id=self.sid,
                                  package_ids=["R1"], location_id="L2",
                                  at="2026-10-01T08:00:00Z")
        self.cargo.place_packages(request_id="p2", actor_id="wh", shipment_id=self.sid,
                                  package_ids=["R1"], location_id="L2",
                                  at="2026-10-01T09:00:00Z")
        status = self.cargo.shipment_status(actor_id="exc", shipment_id=self.sid)
        r1 = next(p for b in status["batches"] for p in b["packages"]
                  if p["package_id"] == "R1")
        self.assertEqual("L2", r1["placement"]["location_id"])

    def test_package_cannot_occupy_two_locations(self):
        self.cargo.register_location(request_id="loc2", actor_id="wh", location_id="L2",
                                     code="A2", capacity_qty=50)
        self._arrive()
        self.cargo.place_packages(request_id="p1", actor_id="wh", shipment_id=self.sid,
                                  package_ids=["R1"], location_id="L2")
        with self.assertRaises(ConflictError):
            self.cargo.place_packages(request_id="p2", actor_id="wh", shipment_id=self.sid,
                                      package_ids=["R1"], location_id="L1")


class AllocationTest(CargoFixture):
    def test_package_cannot_hold_two_segments_concurrently(self):
        self._arrive()
        self.cargo.register_segment(request_id="seg2", actor_id="car", segment_id="S2",
                                    flight_no="CA2", origin="PVG", destination="FRA",
                                    departs_at="2026-10-03T00:00:00Z", capacity_qty=20)
        self.cargo.allocate_segment(request_id="al1", actor_id="car", shipment_id=self.sid,
                                    segment_id="S1", package_ids=["R1"])
        with self.assertRaises(ConflictError):
            self.cargo.allocate_segment(request_id="al2", actor_id="car", shipment_id=self.sid,
                                        segment_id="S2", package_ids=["R1"])

    def test_capacity_is_enforced_under_parallel_holds(self):
        # 两个独立连接指向同一文件库，BEGIN IMMEDIATE 串行化写入，
        # 容量 20 时 R1=10 与 R3=15 不能同时占位成功
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "parallel.sqlite3")
            db1 = Database(path)
            _, cargo1, sid = build_environment(db1, self.clock)
            cargo1.register_packages(
                request_id="pk", actor_id="car", shipment_id=sid,
                packages=[{"package_id": "R1", "quantity": 10, "weight": 100},
                          {"package_id": "R2", "quantity": 6, "weight": 60}])
            cargo1.register_segment(request_id="seg", actor_id="car", segment_id="S1",
                                    flight_no="CA1", origin="PVG", destination="FRA",
                                    departs_at="2026-10-02T00:00:00Z", capacity_qty=20)
            cargo1.register_packages(
                request_id="pk3", actor_id="car", shipment_id=sid,
                packages=[{"package_id": "R3", "quantity": 15, "weight": 10}])
            cargo1.arrive_packages(request_id="ar", actor_id="car", shipment_id=sid,
                                   package_ids=["R1", "R2", "R3"],
                                   at="2026-10-01T08:00:00Z")
            db2 = Database(path)
            cargo2 = CargoService(db2, self.clock)
            outcomes: list[bool] = []

            def hold(service, package_id, request_id, box):
                try:
                    service.allocate_segment(
                        request_id=request_id, actor_id="car", shipment_id=sid,
                        segment_id="S1", package_ids=[package_id])
                    box.append(True)
                except Exception:  # noqa: BLE001
                    box.append(False)

            box1: list[bool] = []
            box2: list[bool] = []
            t1 = threading.Thread(target=hold, args=(cargo1, "R1", "h1", box1))
            t2 = threading.Thread(target=hold, args=(cargo2, "R3", "h2", box2))
            t1.start(); t2.start(); t1.join(); t2.join()
            self.assertEqual(1, sum(box1 + box2))
            db1.close(); db2.close()


class RegulatoryTest(CargoFixture):
    def test_partial_hold_does_not_block_released_package(self):
        self._arrive()
        self.cargo.issue_decision(request_id="d1", actor_id="cus", shipment_id=self.sid,
                                  kind="hold", package_ids=["R1"],
                                  effective_at="2026-10-01T10:00:00Z")
        self.cargo.issue_decision(request_id="d2", actor_id="cus", shipment_id=self.sid,
                                  kind="release", package_ids=["R2"],
                                  effective_at="2026-10-01T10:00:00Z")
        self.cargo.allocate_segment(request_id="al", actor_id="car", shipment_id=self.sid,
                                    segment_id="S1", package_ids=["R1", "R2"])
        with self.assertRaises(ConflictError):
            self.cargo.confirm_allocation(request_id="cf", actor_id="car", shipment_id=self.sid,
                                          segment_id="S1", package_ids=["R1", "R2"])
        # 只确认 R2 可以成功并发运
        self.cargo.confirm_allocation(request_id="cf2", actor_id="car", shipment_id=self.sid,
                                      segment_id="S1", package_ids=["R2"])
        self.cargo.depart_packages(request_id="dp", actor_id="car", shipment_id=self.sid,
                                   segment_id="S1", package_ids=["R2"],
                                   at="2026-10-02T00:00:00Z")
        status = self.cargo.shipment_status(actor_id="exc", shipment_id=self.sid)
        r2 = next(p for b in status["batches"] for p in b["packages"]
                  if p["package_id"] == "R2")
        self.assertEqual("departed", r2["stage"])

    def test_merge_requires_release_on_every_source(self):
        self._arrive()
        self.cargo.handoff(request_id="h0", actor_id="car", shipment_id=self.sid,
                           kind="intake", from_actor_id="car", to_actor_id="wh",
                           package_ids=["R1", "R2"], at="2026-10-01T09:00:00Z")
        self.cargo.handoff(request_id="h1", actor_id="wh", shipment_id=self.sid,
                           kind="back", from_actor_id="wh", to_actor_id="car",
                           package_ids=["R1", "R2"], at="2026-10-01T09:30:00Z")
        merged = self.cargo.merge_packages(
            request_id="mg", actor_id="sor", shipment_id=self.sid,
            source_package_ids=["R1", "R2"], reason="集拼")
        child = merged.data["package_id"]
        self.cargo.issue_decision(request_id="dr1", actor_id="cus", shipment_id=self.sid,
                                  kind="release", package_ids=["R1"],
                                  effective_at="2026-10-01T10:00:00Z")
        self.cargo.allocate_segment(request_id="al", actor_id="car", shipment_id=self.sid,
                                    segment_id="S1", package_ids=[child])
        with self.assertRaises(ConflictError):
            self.cargo.confirm_allocation(request_id="cf", actor_id="car", shipment_id=self.sid,
                                          segment_id="S1", package_ids=[child])
        self.cargo.issue_decision(request_id="dr2", actor_id="cus", shipment_id=self.sid,
                                  kind="release", package_ids=["R2"],
                                  effective_at="2026-10-01T11:00:00Z")
        # 两个来源都获放行后，合并件视为整件放行（任一来源未放行则仍被拦截）
        self.cargo.confirm_allocation(request_id="cf2", actor_id="car", shipment_id=self.sid,
                                      segment_id="S1", package_ids=[child])

    def test_merge_blocked_when_one_source_is_held(self):
        self._arrive()
        self.cargo.handoff(request_id="h0", actor_id="car", shipment_id=self.sid,
                           kind="intake", from_actor_id="car", to_actor_id="wh",
                           package_ids=["R1", "R2"], at="2026-10-01T09:00:00Z")
        self.cargo.handoff(request_id="h1", actor_id="wh", shipment_id=self.sid,
                           kind="back", from_actor_id="wh", to_actor_id="car",
                           package_ids=["R1", "R2"], at="2026-10-01T09:30:00Z")
        merged = self.cargo.merge_packages(
            request_id="mg", actor_id="sor", shipment_id=self.sid,
            source_package_ids=["R1", "R2"], reason="集拼")
        child = merged.data["package_id"]
        self.cargo.issue_decision(request_id="dr1", actor_id="cus", shipment_id=self.sid,
                                  kind="release", package_ids=["R1"],
                                  effective_at="2026-10-01T10:00:00Z")
        self.cargo.issue_decision(request_id="dh2", actor_id="cus", shipment_id=self.sid,
                                  kind="hold", package_ids=["R2"],
                                  effective_at="2026-10-01T11:00:00Z")
        self.cargo.allocate_segment(request_id="al", actor_id="car", shipment_id=self.sid,
                                    segment_id="S1", package_ids=[child])
        with self.assertRaises(ConflictError):
            self.cargo.confirm_allocation(request_id="cf", actor_id="car", shipment_id=self.sid,
                                          segment_id="S1", package_ids=[child])

    def test_late_decision_does_not_rewrite_completed_handoff(self):
        self._arrive()
        self.cargo.issue_decision(request_id="rel", actor_id="cus", shipment_id=self.sid,
                                  kind="release", package_ids=["R1", "R2"],
                                  effective_at="2026-10-01T09:00:00Z")
        self.cargo.handoff(request_id="h", actor_id="car", shipment_id=self.sid,
                           kind="intake", from_actor_id="car", to_actor_id="wh",
                           package_ids=["R1", "R2"], at="2026-10-01T09:30:00Z",
                           handling_fee=80)
        # 迟到的退运决定只作用于仍在途的 R2，R1 继续走完
        self.cargo.allocate_segment(request_id="al", actor_id="car", shipment_id=self.sid,
                                    segment_id="S1", package_ids=["R1"])
        self.cargo.confirm_allocation(request_id="cf", actor_id="car", shipment_id=self.sid,
                                      segment_id="S1", package_ids=["R1"])
        self.cargo.depart_packages(request_id="dp", actor_id="car", shipment_id=self.sid,
                                   segment_id="S1", package_ids=["R1"],
                                   at="2026-10-02T00:00:00Z")
        self.cargo.issue_decision(request_id="late-ret", actor_id="cus", shipment_id=self.sid,
                                  kind="return", package_ids=["R2"],
                                  reason="事后退运", effective_at="2026-10-01T08:30:00Z")
        status = self.cargo.shipment_status(actor_id="exc", shipment_id=self.sid)
        # 交接事实与 80 元费用原样保留
        self.assertEqual(1, len(status["handoffs"]))
        self.assertEqual(80.0, status["fees"][0]["amount"])
        r1 = next(p for b in status["batches"] for p in b["packages"]
                  if p["package_id"] == "R1")
        self.assertEqual("departed", r1["stage"])


class SensitiveDeclarationTest(CargoFixture):
    def test_only_customs_can_read_sensitive_payload(self):
        decl = self.cargo.submit_declaration(
            request_id="sd", actor_id="car", shipment_id=self.sid,
            package_ids=["R1"], payload={"secret": "x"}, sensitive=True)
        with self.assertRaises(PermissionDenied):
            self.cargo.get_declaration(actor_id="sor",
                                       declaration_id=decl.data["declaration_id"])
        with self.assertRaises(PermissionDenied):
            self.cargo.get_declaration(actor_id="exc",
                                       declaration_id=decl.data["declaration_id"])
        visible = self.cargo.get_declaration(actor_id="cus",
                                             declaration_id=decl.data["declaration_id"])
        self.assertEqual("x", visible["payload"]["secret"])
        # 列表视图中同样脱敏
        status = self.cargo.shipment_status(actor_id="sor", shipment_id=self.sid)
        item = next(d for d in status["declarations"]
                    if d["declaration_id"] == decl.data["declaration_id"])
        self.assertIsNone(item["payload"])


class ReturnTest(CargoFixture):
    def test_return_requires_effective_decision_for_each_package(self):
        self._arrive()
        with self.assertRaises(ConflictError):
            self.cargo.return_cargo(request_id="ret", actor_id="cus", shipment_id=self.sid,
                                    package_ids=["R1"], at="2026-10-01T12:00:00Z")
        self.cargo.issue_decision(request_id="dret", actor_id="cus", shipment_id=self.sid,
                                  kind="return", package_ids=["R1"],
                                  effective_at="2026-10-01T11:00:00Z")
        result = self.cargo.return_cargo(request_id="ret2", actor_id="cus",
                                         shipment_id=self.sid, package_ids=["R1"],
                                         to_actor_id="car",
                                         at="2026-10-01T12:00:00Z")
        self.assertEqual(["R1"], result.data["package_ids"])
        status = self.cargo.shipment_status(actor_id="exc", shipment_id=self.sid)
        r1 = next(p for b in status["batches"] for p in b["packages"]
                  if p["package_id"] == "R1")
        self.assertEqual("returned", r1["stage"])


class DutyTest(CargoFixture):
    def test_sorting_cannot_issue_customs_decision(self):
        with self.assertRaises(PermissionDenied):
            self.cargo.issue_decision(request_id="dx", actor_id="sor", shipment_id=self.sid,
                                      kind="release", package_ids=["R1"])

    def test_sorting_cannot_register_segment(self):
        with self.assertRaises(PermissionDenied):
            self.cargo.register_segment(
                request_id="sx", actor_id="sor", segment_id="SX", flight_no="X1",
                origin="PVG", destination="FRA", departs_at="2026-10-03T00:00:00Z",
                capacity_qty=10)

    def test_actor_without_duty_cannot_read_status(self):
        # 新增一个没有该运单职责的操作者
        self.base.register_actor(request_id="an", actor_id="adm", new_actor_id="nobody",
                                 display_name="无关人员", role="operator", organization_id="hub")
        with self.assertRaises(PermissionDenied):
            self.cargo.shipment_status(actor_id="nobody", shipment_id=self.sid)


if __name__ == "__main__":
    unittest.main()
