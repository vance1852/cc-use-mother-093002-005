import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from digital_trade_foundation.air_exception import AirExceptionService
from digital_trade_foundation.clock import FixedClock
from digital_trade_foundation.errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from digital_trade_foundation.service import DomainService
from digital_trade_foundation.storage import Database

NOW = datetime(2026, 10, 4, 8, 0, tzinfo=timezone.utc)


class AirExceptionTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.foundation = DomainService(self.database, FixedClock(NOW))
        self.air = AirExceptionService(self.foundation)
        self.foundation.register_organization(request_id="org", actor_id="bootstrap",
                                              organization_id="o1", name="枢纽运营")
        self.foundation.register_actor(request_id="a-admin", actor_id="bootstrap", new_actor_id="admin",
                                       display_name="管理员", role="admin", organization_id="o1")
        for role in ("carrier", "sorter", "warehouse", "customs", "desk"):
            self.foundation.register_actor(request_id=f"a-{role}", actor_id="admin", new_actor_id=role,
                                           display_name=role, role=role, organization_id="o1")
        self.foundation.register_site(request_id="site", actor_id="admin", site_id="hub",
                                      organization_id="o1", name="枢纽", timezone_name="Asia/Shanghai")
        self.air.register_segment(request_id="seg-1", actor_id="carrier", site_id="hub",
                                  segment_id="SEG1", flight_no="CA1", origin="HUB", destination="FRA",
                                  departs_at="2026-10-04T14:00:00Z", arrives_at="2026-10-04T20:00:00Z",
                                  capacity_pieces=10, capacity_weight=100)
        self.air.register_segment(request_id="seg-2", actor_id="carrier", site_id="hub",
                                  segment_id="SEG2", flight_no="CA2", origin="HUB", destination="FRA",
                                  departs_at="2026-10-04T16:00:00Z", arrives_at="2026-10-04T20:45:00Z",
                                  capacity_pieces=10, capacity_weight=100)
        self.air.register_slot(request_id="slot-a1", actor_id="warehouse", site_id="hub", slot_code="A1")
        self.air.register_slot(request_id="slot-a2", actor_id="warehouse", site_id="hub", slot_code="A2")

    def tearDown(self):
        self.database.close()

    def _make_waybill(self, waybill_id="WB1", pieces=4):
        self.air.register_waybill(request_id=f"req-{waybill_id}", actor_id="carrier", site_id="hub",
                                  waybill_id=waybill_id, origin="HUB", destination="FRA",
                                  declared_pieces=pieces, declared_weight=pieces * 10,
                                  promised_arrival_at="2026-10-04T21:00:00Z")
        packages = [{"package_id": f"{waybill_id}-P{i}", "pieces": 1, "weight": 10}
                    for i in range(1, pieces + 1)]
        self.air.scan_packages(request_id=f"scan-{waybill_id}", actor_id="carrier",
                               waybill_id=waybill_id, packages=packages)
        return [item["package_id"] for item in packages]

    def _arrive(self, waybill_id="WB1", pieces=4):
        packages = self._make_waybill(waybill_id, pieces)
        receipt = self.air.arrive(request_id=f"arrive-{waybill_id}", actor_id="carrier",
                                  waybill_id=waybill_id)
        return receipt.resource_id, packages

    def _detail(self, waybill_id="WB1"):
        return self.air.waybill_detail(actor_id="desk", waybill_id=waybill_id)

    def _batch(self, detail, batch_id):
        return next(b for b in detail["batches"] if b["batch_id"] == batch_id)

    # ------------------------------------------------------------------
    # 扫描幂等与到港
    # ------------------------------------------------------------------

    def test_repeated_scan_does_not_double_count(self):
        self.air.register_waybill(request_id="wb", actor_id="carrier", site_id="hub", waybill_id="WB1",
                                  origin="HUB", destination="FRA", declared_pieces=2, declared_weight=20)
        packages = [{"package_id": "WB1-P1", "pieces": 1, "weight": 10},
                    {"package_id": "WB1-P2", "pieces": 1, "weight": 10}]
        first = self.air.scan_packages(request_id="s1", actor_id="carrier", waybill_id="WB1",
                                       packages=packages)
        replay = self.air.scan_packages(request_id="s1", actor_id="carrier", waybill_id="WB1",
                                        packages=packages)
        self.assertFalse(first.replayed)
        self.assertTrue(replay.replayed)
        # 不同请求号重复扫同一包裹：自然去重，不重复计数
        self.air.scan_packages(request_id="s2", actor_id="sorter", waybill_id="WB1",
                               packages=[{"package_id": "WB1-P1", "pieces": 1, "weight": 10}])
        detail = self._detail()
        self.assertEqual(2, detail["conservation"]["scanned_pieces"])
        # 同一包裹号不同内容：冲突
        with self.assertRaises(ConflictError):
            self.air.scan_packages(request_id="s3", actor_id="carrier", waybill_id="WB1",
                                   packages=[{"package_id": "WB1-P1", "pieces": 1, "weight": 99}])
        # 超出申报件数：拒绝
        with self.assertRaises(ValidationError):
            self.air.scan_packages(request_id="s4", actor_id="carrier", waybill_id="WB1",
                                   packages=[{"package_id": "WB1-P9", "pieces": 1, "weight": 10}])

    def test_arrive_requires_complete_scan(self):
        self.air.register_waybill(request_id="wb", actor_id="carrier", site_id="hub", waybill_id="WB1",
                                  origin="HUB", destination="FRA", declared_pieces=2, declared_weight=20)
        self.air.scan_packages(request_id="s1", actor_id="carrier", waybill_id="WB1",
                               packages=[{"package_id": "WB1-P1", "pieces": 1, "weight": 10}])
        with self.assertRaises(ValidationError):
            self.air.arrive(request_id="ar", actor_id="carrier", waybill_id="WB1")

    # ------------------------------------------------------------------
    # 拆分合并守恒
    # ------------------------------------------------------------------

    def test_split_merge_conserve_quantity(self):
        batch, packages = self._arrive(pieces=5)
        split = self.air.split_batch(request_id="sp1", actor_id="sorter", batch_id=batch,
                                     package_ids=packages[:2])
        detail = self._detail()
        self.assertEqual(3, self._batch(detail, batch)["pieces"])
        self.assertEqual(2, self._batch(detail, split.resource_id)["pieces"])
        self.assertEqual(5, sum(b["pieces"] for b in detail["batches"] if b["status"] != "closed"))
        replay = self.air.split_batch(request_id="sp1", actor_id="sorter", batch_id=batch,
                                      package_ids=packages[:2])
        self.assertTrue(replay.replayed)
        self.assertEqual(split.resource_id, replay.resource_id)
        self.air.merge_batches(request_id="mg1", actor_id="sorter", target_batch_id=batch,
                               source_batch_ids=[split.resource_id])
        detail = self._detail()
        self.assertEqual(5, self._batch(detail, batch)["pieces"])
        self.assertEqual("closed", self._batch(detail, split.resource_id)["status"])
        self.assertTrue(detail["conservation"]["conserved"])
        with self.assertRaises(ValidationError):
            self.air.split_batch(request_id="sp2", actor_id="sorter", batch_id=batch,
                                 package_ids=["WB9-P1"])
        with self.assertRaises(ValidationError):
            self.air.split_batch(request_id="sp3", actor_id="sorter", batch_id=batch,
                                 package_ids=packages)

    # ------------------------------------------------------------------
    # 部分决定只作用于覆盖单元
    # ------------------------------------------------------------------

    def test_partial_decision_keeps_unaffected_units_flowing(self):
        batch, packages = self._arrive(pieces=5)
        self.air.book_segment(request_id="bk", actor_id="carrier", batch_id=batch, segment_id="SEG1")
        decision = self.air.record_decision(request_id="d1", actor_id="customs", waybill_id="WB1",
                                            decision_type="hold", package_ids=packages[:2],
                                            effective_at="2026-10-04T07:00:00Z")
        detail = self._detail()
        main = self._batch(detail, batch)
        self.assertEqual("open", main["status"])
        self.assertEqual(3, main["pieces"])
        self.assertEqual(3, main["active_booking"]["pieces"])
        held = next(b for b in detail["batches"] if b["status"] == "held")
        self.assertEqual(2, held["pieces"])
        self.assertIsNone(held["active_booking"])
        record = self.air.get_decision(actor_id="customs", decision_id=decision.resource_id)
        self.assertEqual(sorted(packages[:2]), record["package_ids"])
        # 未涉及单元继续流转：交接并出港
        self.air.record_handover(request_id="h1", actor_id="carrier", batch_id=batch, to_role="carrier")
        self.air.update_segment_status(request_id="dep", actor_id="carrier",
                                       segment_id="SEG1", status="departed")
        detail = self._detail()
        self.assertEqual("departed", self._batch(detail, batch)["status"])
        self.assertEqual("held", self._batch(detail, held["batch_id"])["status"])

    def test_decision_replay_and_overlap(self):
        batch, packages = self._arrive(pieces=3)
        first = self.air.record_decision(request_id="d1", actor_id="customs", waybill_id="WB1",
                                         decision_type="hold", package_ids=[packages[0]],
                                         effective_at="2026-10-04T07:00:00Z")
        replay = self.air.record_decision(request_id="d1", actor_id="customs", waybill_id="WB1",
                                          decision_type="hold", package_ids=[packages[0]],
                                          effective_at="2026-10-04T07:00:00Z")
        self.assertTrue(replay.replayed)
        self.assertEqual(first.resource_id, replay.resource_id)
        # 第二个决定覆盖已扣留包裹（自然跳过）与新包裹（继续扣留）
        second = self.air.record_decision(request_id="d2", actor_id="customs", waybill_id="WB1",
                                          decision_type="hold", package_ids=packages[:2],
                                          effective_at="2026-10-04T07:30:00Z")
        record = self.air.get_decision(actor_id="customs", decision_id=second.resource_id)
        self.assertEqual(1, len(record["effect"]["adjusted_batches"]))
        detail = self._detail()
        held_pieces = sum(b["pieces"] for b in detail["batches"] if b["status"] == "held")
        self.assertEqual(2, held_pieces)
        self.assertEqual(1, self._batch(detail, batch)["pieces"])

    # ------------------------------------------------------------------
    # 仓位与容量互斥
    # ------------------------------------------------------------------

    def test_slot_lease_is_exclusive(self):
        batch, packages = self._arrive(pieces=4)
        other = self.air.split_batch(request_id="sp", actor_id="sorter", batch_id=batch,
                                     package_ids=packages[:2])
        lease = self.air.lease_slot(request_id="l1", actor_id="warehouse", batch_id=batch, slot_code="A1")
        with self.assertRaises(ConflictError):
            self.air.lease_slot(request_id="l2", actor_id="warehouse",
                                batch_id=other.resource_id, slot_code="A1")
        with self.assertRaises(ConflictError):
            self.air.lease_slot(request_id="l3", actor_id="warehouse", batch_id=batch, slot_code="A2")
        replay = self.air.lease_slot(request_id="l1", actor_id="warehouse", batch_id=batch, slot_code="A1")
        self.assertTrue(replay.replayed)
        self.air.release_slot(request_id="l4", actor_id="warehouse", lease_id=lease.resource_id)
        self.air.lease_slot(request_id="l5", actor_id="warehouse",
                            batch_id=other.resource_id, slot_code="A1")
        # 任意包装只会出现在一个批次里
        detail = self._detail()
        locations = [p["package_id"] for p in detail["packages"]]
        self.assertEqual(len(locations), len(set(locations)))

    def test_booking_capacity_and_natural_replay(self):
        batch, _ = self._arrive("WB1", pieces=6)
        booking = self.air.book_segment(request_id="b1", actor_id="carrier",
                                        batch_id=batch, segment_id="SEG1")
        again = self.air.book_segment(request_id="b2", actor_id="carrier",
                                      batch_id=batch, segment_id="SEG1")
        self.assertTrue(again.replayed)
        self.assertEqual(booking.resource_id, again.resource_id)
        with self.assertRaises(ConflictError):
            self.air.book_segment(request_id="b3", actor_id="carrier", batch_id=batch, segment_id="SEG2")
        other, _ = self._arrive("WB2", pieces=6)
        with self.assertRaises(ConflictError):
            self.air.book_segment(request_id="b4", actor_id="carrier",
                                  batch_id=other, segment_id="SEG1")
        board = self.air.desk_board(actor_id="desk", site_id="hub")
        seg1 = next(s for s in board["locked_capacity"]["segments"] if s["segment_id"] == "SEG1")
        self.assertEqual(6, seg1["booked_pieces"])

    # ------------------------------------------------------------------
    # 迟到决定只调整未完成路径
    # ------------------------------------------------------------------

    def test_late_decision_preserves_completed_facts(self):
        batch, packages = self._arrive(pieces=3)
        self.air.lease_slot(request_id="l1", actor_id="warehouse", batch_id=batch, slot_code="A1")
        self.air.book_segment(request_id="b1", actor_id="carrier", batch_id=batch, segment_id="SEG1")
        self.air.record_handover(request_id="h1", actor_id="carrier", batch_id=batch, to_role="carrier")
        before = self._detail()
        self.assertEqual(2, len(before["fees"]))
        self.assertEqual(2, len(before["handovers"]))
        decision = self.air.record_decision(request_id="d1", actor_id="customs", waybill_id="WB1",
                                            decision_type="hold", package_ids=[packages[0]],
                                            effective_at="2026-10-04T07:00:00Z")
        record = self.air.get_decision(actor_id="customs", decision_id=decision.resource_id)
        self.assertTrue(record["effect"]["late"])
        # 生效时间之后才到达的决定：已发生的交接与费用事实全部保留
        self.assertEqual(4, len(record["effect"]["preserved_facts"]))
        after = self._detail()
        self.assertEqual([f["amount"] for f in before["fees"]],
                         [f["amount"] for f in after["fees"]])
        self.assertEqual(len(before["handovers"]), len(after["handovers"]))
        # 整批扣留会取消尚未完成的路径并释放占用，但仍不改写事实
        decision2 = self.air.record_decision(request_id="d2", actor_id="customs", waybill_id="WB1",
                                             decision_type="hold", package_ids=packages[1:],
                                             effective_at="2026-10-04T07:30:00Z")
        record2 = self.air.get_decision(actor_id="customs", decision_id=decision2.resource_id)
        self.assertEqual(3, len(record2["effect"]["cancelled_steps"]))
        self.assertEqual(1, len(record2["effect"]["released_leases"]))
        self.assertEqual(1, len(record2["effect"]["released_bookings"]))
        final = self._detail()
        self.assertEqual(len(before["fees"]), len(final["fees"]))
        self.assertEqual(len(before["handovers"]), len(final["handovers"]))

    def test_release_resumes_path_and_allows_booking(self):
        batch, packages = self._arrive(pieces=2)
        self.air.record_decision(request_id="d1", actor_id="customs", waybill_id="WB1",
                                 decision_type="hold", package_ids=[packages[0]],
                                 effective_at="2026-10-04T07:00:00Z")
        self.air.record_decision(request_id="d2", actor_id="customs", waybill_id="WB1",
                                 decision_type="release", package_ids=[packages[0]],
                                 effective_at="2026-10-04T08:00:00Z")
        detail = self._detail()
        resumed = next(b for b in detail["batches"]
                       if b["status"] == "open" and b["batch_id"] != batch)
        self.assertEqual(["store", "load", "handover", "depart"],
                         [s["step_type"] for s in resumed["steps"] if s["status"] == "pending"])
        self.air.book_segment(request_id="b9", actor_id="desk", batch_id=resumed["batch_id"],
                              segment_id="SEG2")

    def test_return_flow_keeps_conservation(self):
        batch, packages = self._arrive(pieces=2)
        self.air.record_decision(request_id="d1", actor_id="customs", waybill_id="WB1",
                                 decision_type="return", package_ids=[packages[0]],
                                 effective_at="2026-10-04T07:00:00Z")
        detail = self._detail()
        returning = next(b for b in detail["batches"] if b["kind"] == "return")
        self.air.execute_return(request_id="r1", actor_id="carrier", batch_id=returning["batch_id"])
        detail = self._detail()
        self.assertEqual("returned", self._batch(detail, returning["batch_id"])["status"])
        self.assertTrue(detail["conservation"]["conserved"])
        self.assertIn("return", {f["category"] for f in detail["fees"]})
        with self.assertRaises(ConflictError):
            self.air.record_decision(request_id="d2", actor_id="customs", waybill_id="WB1",
                                     decision_type="release", package_ids=[packages[0]],
                                     effective_at="2026-10-04T08:00:00Z")

    # ------------------------------------------------------------------
    # 申报与敏感可见性
    # ------------------------------------------------------------------

    def test_declaration_payload_only_for_customs(self):
        self._arrive(pieces=2)
        self.air.submit_declaration(request_id="dec", actor_id="carrier", waybill_id="WB1",
                                    data={"hs_code": "850760", "secret": "敏感内容"})
        as_customs = self.air.list_declarations(actor_id="customs", waybill_id="WB1")
        self.assertEqual("敏感内容", as_customs[0]["payload"]["secret"])
        self.assertFalse(as_customs[0]["restricted"])
        for role in ("sorter", "warehouse", "desk", "carrier"):
            view = self.air.list_declarations(actor_id=role, waybill_id="WB1")
            self.assertIsNone(view[0]["payload"])
            self.assertTrue(view[0]["restricted"])

    def test_supplement_flow_updates_todos(self):
        self._arrive(pieces=2)
        self.air.submit_declaration(request_id="dec", actor_id="carrier", waybill_id="WB1",
                                    data={"hs_code": "850760"})
        packages = [p["package_id"] for p in self._detail()["packages"]]
        self.air.record_decision(request_id="d1", actor_id="customs", waybill_id="WB1",
                                 decision_type="supplement", package_ids=packages[:1],
                                 effective_at="2026-10-04T07:00:00Z")
        board = self.air.desk_board(actor_id="desk", site_id="hub")
        self.assertIn("supplement_due", {t["kind"] for t in board["todos"]})
        self.air.submit_declaration(request_id="dec2", actor_id="carrier", waybill_id="WB1",
                                    data={"hs_code": "850760", "supplement": "补充材料"})
        board = self.air.desk_board(actor_id="desk", site_id="hub")
        self.assertNotIn("supplement_due", {t["kind"] for t in board["todos"]})
        self.air.record_decision(request_id="d2", actor_id="customs", waybill_id="WB1",
                                 decision_type="release", package_ids=packages[:1],
                                 effective_at="2026-10-04T08:00:00Z")
        declarations = self.air.list_declarations(actor_id="customs", waybill_id="WB1")
        self.assertEqual("cleared", declarations[-1]["status"])

    # ------------------------------------------------------------------
    # 权限
    # ------------------------------------------------------------------

    def test_role_permissions(self):
        batch, packages = self._arrive(pieces=2)
        with self.assertRaises(PermissionDenied):
            self.air.record_decision(request_id="x1", actor_id="sorter", waybill_id="WB1",
                                     decision_type="hold", package_ids=packages[:1],
                                     effective_at="2026-10-04T07:00:00Z")
        with self.assertRaises(PermissionDenied):
            self.air.lease_slot(request_id="x2", actor_id="customs", batch_id=batch, slot_code="A1")
        with self.assertRaises(PermissionDenied):
            self.air.split_batch(request_id="x3", actor_id="warehouse", batch_id=batch,
                                 package_ids=packages[:1])
        with self.assertRaises(PermissionDenied):
            self.air.submit_declaration(request_id="x4", actor_id="customs", waybill_id="WB1",
                                        data={"a": 1})
        with self.assertRaises(PermissionDenied):
            self.air.record_handover(request_id="x5", actor_id="warehouse",
                                     batch_id=batch, to_role="carrier")

    # ------------------------------------------------------------------
    # 交接与出港
    # ------------------------------------------------------------------

    def test_depart_requires_completed_handover(self):
        batch, _ = self._arrive(pieces=2)
        self.air.book_segment(request_id="b1", actor_id="carrier", batch_id=batch, segment_id="SEG1")
        with self.assertRaises(ConflictError):
            self.air.update_segment_status(request_id="dep", actor_id="carrier",
                                           segment_id="SEG1", status="departed")
        self.air.record_handover(request_id="h1", actor_id="sorter", batch_id=batch, to_role="carrier")
        self.air.update_segment_status(request_id="dep2", actor_id="carrier",
                                       segment_id="SEG1", status="departed")
        detail = self._detail()
        main = self._batch(detail, batch)
        self.assertEqual("departed", main["status"])
        self.assertIsNone(main["active_booking"])
        custodian_chain = [(h["from_role"], h["to_role"]) for h in detail["handovers"]]
        self.assertEqual([("carrier", "sorter"), ("sorter", "carrier")], custodian_chain)

    def test_segment_cancel_releases_capacity(self):
        batch, _ = self._arrive(pieces=4)
        self.air.book_segment(request_id="b1", actor_id="carrier", batch_id=batch, segment_id="SEG1")
        self.air.update_segment_status(request_id="cx", actor_id="carrier",
                                       segment_id="SEG1", status="cancelled")
        board = self.air.desk_board(actor_id="desk", site_id="hub")
        self.assertIn("booking_needed", {t["kind"] for t in board["todos"]})
        self.assertNotIn("SEG1", {s["segment_id"] for s in board["locked_capacity"]["segments"]})
        self.air.book_segment(request_id="b2", actor_id="carrier", batch_id=batch, segment_id="SEG2")
        board = self.air.desk_board(actor_id="desk", site_id="hub")
        seg2 = next(s for s in board["locked_capacity"]["segments"] if s["segment_id"] == "SEG2")
        self.assertEqual(4, seg2["booked_pieces"])

    # ------------------------------------------------------------------
    # 延误影响与恢复路线
    # ------------------------------------------------------------------

    def test_delay_impact_proposes_recovery_without_expanding_freeze(self):
        batch, packages = self._arrive(pieces=5)
        self.air.book_segment(request_id="b1", actor_id="carrier", batch_id=batch, segment_id="SEG1")
        self.air.record_decision(request_id="d1", actor_id="customs", waybill_id="WB1",
                                 decision_type="hold", package_ids=packages[:2],
                                 effective_at="2026-10-04T07:00:00Z")
        self.air.update_segment_status(request_id="dl", actor_id="carrier", segment_id="SEG1",
                                       status="delayed", new_departs_at="2026-10-04T20:00:00Z",
                                       new_arrives_at="2026-10-05T02:00:00Z")
        impact = self.air.delay_impact(actor_id="desk", segment_id="SEG1")
        self.assertEqual(360, impact["delay_minutes"])
        self.assertEqual(3, len(impact["affected"]["packages"]))
        self.assertEqual(2, len(impact["recovery"]["excluded_held_packages"]))
        self.assertFalse(impact["recovery"]["freeze_scope_expanded"])
        proposal = impact["recovery"]["proposals"][0]
        self.assertEqual(batch, proposal["batch_id"])
        self.assertEqual("SEG2", proposal["to_segment_id"])
        self.assertTrue(proposal["meets_commitment"])
        commitment = impact["affected"]["commitments"][0]
        self.assertTrue(commitment["at_risk"])
        projected = impact["affected"]["projected_fees"]
        self.assertEqual(80.0, projected["rebooking"])
        self.assertEqual(3.75, projected["extra_storage"])
        self.assertEqual({"SEG1", "SEG2"}, set(impact["affected"]["segments"]))
        # 被扣留的包裹不在恢复路线里
        self.assertEqual(sorted(packages[:2]),
                         sorted(p["package_id"] for p in impact["recovery"]["excluded_held_packages"]))

    def test_desk_board_orders_todos(self):
        batch, packages = self._arrive("WB1", pieces=4)
        self.air.record_decision(request_id="d1", actor_id="customs", waybill_id="WB1",
                                 decision_type="hold", package_ids=packages[:1],
                                 effective_at="2026-10-04T07:00:00Z")
        other, _ = self._arrive("WB2", pieces=2)
        self.air.book_segment(request_id="b2", actor_id="carrier", batch_id=other, segment_id="SEG1")
        self.air.update_segment_status(request_id="dl", actor_id="carrier", segment_id="SEG1",
                                       status="delayed", new_departs_at="2026-10-04T18:00:00Z",
                                       new_arrives_at="2026-10-04T23:00:00Z")
        board = self.air.desk_board(actor_id="desk", site_id="hub")
        kinds = [t["kind"] for t in board["todos"]]
        self.assertEqual("inspection_due", kinds[0])
        self.assertIn("rebook_advised", kinds)
        self.assertIn("handover_due", kinds)
        in_transit = {b["batch_id"]: b for b in board["in_transit"]}
        self.assertEqual("customs", in_transit[[b for b in in_transit
                                                if in_transit[b]["status"] == "held"][0]]["location"])
        seg1 = next(s for s in board["locked_capacity"]["segments"] if s["segment_id"] == "SEG1")
        self.assertEqual(2, seg1["booked_pieces"])

    # ------------------------------------------------------------------
    # 系统恢复后状态准确
    # ------------------------------------------------------------------

    def test_state_survives_restart(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "air.sqlite3"
            database = Database(path)
            foundation = DomainService(database, FixedClock(NOW))
            air = AirExceptionService(foundation)
            foundation.register_organization(request_id="org", actor_id="bootstrap",
                                             organization_id="o1", name="枢纽运营")
            foundation.register_actor(request_id="a1", actor_id="bootstrap", new_actor_id="admin",
                                      display_name="管理员", role="admin", organization_id="o1")
            for role in ("carrier", "sorter", "customs", "desk"):
                foundation.register_actor(request_id=f"a-{role}", actor_id="admin", new_actor_id=role,
                                          display_name=role, role=role, organization_id="o1")
            foundation.register_site(request_id="s1", actor_id="admin", site_id="hub",
                                     organization_id="o1", name="枢纽", timezone_name="Asia/Shanghai")
            air.register_segment(request_id="seg", actor_id="carrier", site_id="hub", segment_id="SEG1",
                                 flight_no="CA1", origin="HUB", destination="FRA",
                                 departs_at="2026-10-04T14:00:00Z", arrives_at="2026-10-04T20:00:00Z",
                                 capacity_pieces=10, capacity_weight=100)
            air.register_waybill(request_id="wb", actor_id="carrier", site_id="hub", waybill_id="WB1",
                                 origin="HUB", destination="FRA", declared_pieces=3, declared_weight=30)
            air.scan_packages(request_id="sc", actor_id="carrier", waybill_id="WB1",
                              packages=[{"package_id": f"P{i}", "pieces": 1, "weight": 10}
                                        for i in range(1, 4)])
            batch = air.arrive(request_id="ar", actor_id="carrier", waybill_id="WB1").resource_id
            air.book_segment(request_id="bk", actor_id="carrier", batch_id=batch, segment_id="SEG1")
            air.record_decision(request_id="d1", actor_id="customs", waybill_id="WB1",
                                decision_type="hold", package_ids=["P1"],
                                effective_at="2026-10-04T07:00:00Z")
            before = air.desk_board(actor_id="desk", site_id="hub")
            database.close()

            database2 = Database(path)
            foundation2 = DomainService(database2, FixedClock(NOW))
            air2 = AirExceptionService(foundation2)
            after = air2.desk_board(actor_id="desk", site_id="hub")
            self.assertEqual([b["batch_id"] for b in before["in_transit"]],
                             [b["batch_id"] for b in after["in_transit"]])
            self.assertEqual(before["locked_capacity"], after["locked_capacity"])
            self.assertEqual([t["kind"] for t in before["todos"]],
                             [t["kind"] for t in after["todos"]])
            valid, count = foundation2.verify_audit()
            self.assertTrue(valid)
            self.assertGreater(count, 0)
            database2.close()

    def test_concurrent_lease_keeps_single_holder(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "air.sqlite3"
            database = Database(path)
            foundation = DomainService(database, FixedClock(NOW))
            air = AirExceptionService(foundation)
            foundation.register_organization(request_id="org", actor_id="bootstrap",
                                             organization_id="o1", name="枢纽运营")
            foundation.register_actor(request_id="a1", actor_id="bootstrap", new_actor_id="admin",
                                      display_name="管理员", role="admin", organization_id="o1")
            for role in ("carrier", "sorter", "warehouse", "desk"):
                foundation.register_actor(request_id=f"a-{role}", actor_id="admin", new_actor_id=role,
                                          display_name=role, role=role, organization_id="o1")
            foundation.register_site(request_id="s1", actor_id="admin", site_id="hub",
                                     organization_id="o1", name="枢纽", timezone_name="Asia/Shanghai")
            air.register_slot(request_id="slot", actor_id="warehouse", site_id="hub", slot_code="A1")
            air.register_waybill(request_id="wb", actor_id="carrier", site_id="hub", waybill_id="WB1",
                                 origin="HUB", destination="FRA", declared_pieces=2, declared_weight=20)
            air.scan_packages(request_id="sc", actor_id="carrier", waybill_id="WB1",
                              packages=[{"package_id": "P1", "pieces": 1, "weight": 10},
                                        {"package_id": "P2", "pieces": 1, "weight": 10}])
            arrival = air.arrive(request_id="ar", actor_id="carrier", waybill_id="WB1")
            split = air.split_batch(request_id="sp", actor_id="sorter",
                                    batch_id=arrival.resource_id, package_ids=["P2"])
            batches = [arrival.resource_id, split.resource_id]
            database.close()

            outcomes = []
            barrier = threading.Barrier(2)

            def attempt(name, batch_id):
                connection = Database(path)
                service = AirExceptionService(DomainService(connection, FixedClock(NOW)))
                barrier.wait()
                try:
                    service.lease_slot(request_id=f"lease-{name}", actor_id="warehouse",
                                       batch_id=batch_id, slot_code="A1")
                    outcomes.append("ok")
                except ConflictError:
                    outcomes.append("conflict")
                finally:
                    connection.close()

            threads = [threading.Thread(target=attempt, args=(f"t{i}", batches[i])) for i in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(["conflict", "ok"], sorted(outcomes))

            verify = Database(path)
            service = AirExceptionService(DomainService(verify, FixedClock(NOW)))
            board = service.desk_board(actor_id="desk", site_id="hub")
            self.assertEqual(1, len(board["locked_capacity"]["slots"]["active"]))
            verify.close()

    def test_unknown_references_raise_not_found(self):
        with self.assertRaises(NotFoundError):
            self.air.get_decision(actor_id="customs", decision_id="missing")
        with self.assertRaises(NotFoundError):
            self.air.waybill_detail(actor_id="desk", waybill_id="missing")


if __name__ == "__main__":
    unittest.main()
