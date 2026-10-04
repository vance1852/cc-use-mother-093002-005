"""运行航空物流异常协同服务的离线端到端验收。

场景对应业务描述：一票货物到港后被拆到多个子批次，整体原定同一航段；
一部分包装获准放行并继续航程，另一部分因申报冲突被扣留，其航段占位与
仓位租约保持锁定；承运人随后改签，原定仓位、交接责任和费用归属被打乱；
迟到的查验决定只调整尚未完成的路径；异常席计算延误牵连与恢复路线，
且不扩大冻结范围。系统用同一数据库重建后，在途状态依然准确。
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .cargo import (
    CargoService,
    DUTY_AIRLINE,
    DUTY_CUSTOMS,
    DUTY_EXCEPTION,
    DUTY_SORTING,
    DUTY_WAREHOUSE,
)
from .clock import ManualClock
from .errors import ConflictError, PermissionDenied
from .service import DomainService
from .storage import Database

DAY1 = "2026-10-01T08:00:00Z"
DAY2 = "2026-10-02T09:00:00Z"
DAY3 = "2026-10-03T10:00:00Z"
DAY4 = "2026-10-04T06:00:00Z"
DAY5 = "2026-10-05T09:00:00Z"
PROMISED = "2026-10-04T00:00:00Z"


def run() -> dict[str, object]:
    with tempfile.TemporaryDirectory() as directory:
        db_path = Path(directory) / "cargo_acceptance.sqlite3"
        database = Database(db_path)
        clock = ManualClock(datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc))
        base = DomainService(database, clock)
        cargo = CargoService(database, clock)

        # ------------------------------------------------------------ 人员建档
        base.register_organization(request_id="org", actor_id="bootstrap",
                                   organization_id="hub", name="全球航空物流枢纽")
        base.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="admin-1",
                            display_name="管理员", role="admin", organization_id="hub")
        for req, aid, name, role in [
            ("a-carrier", "carrier-1", "承运人值班", "operator"),
            ("a-sorting", "sorting-1", "分拣值班", "operator"),
            ("a-warehouse", "warehouse-1", "仓储值班", "operator"),
            ("a-customs", "customs-1", "海关值班", "reviewer"),
            ("a-exception", "exception-1", "异常席值班", "operator"),
        ]:
            base.register_actor(request_id=req, actor_id="admin-1", new_actor_id=aid,
                                display_name=name, role=role, organization_id="hub")

        # ------------------------------------------------------------ 运单与授权
        shipment = cargo.create_shipment(
            request_id="ship-1", actor_id="carrier-1", master_waybill="MWB-7788",
            origin="PVG", destination="FRA", promised_delivery_at=PROMISED)
        sid = shipment.data["shipment_id"]
        for req, aid, duty in [
            ("d-sort", "sorting-1", DUTY_SORTING),
            ("d-wh", "warehouse-1", DUTY_WAREHOUSE),
            ("d-cust", "customs-1", DUTY_CUSTOMS),
            ("d-exc", "exception-1", DUTY_EXCEPTION),
        ]:
            cargo.grant_duty(request_id=req, actor_id="carrier-1", shipment_id=sid,
                             target_actor_id=aid, duty=duty)

        # 初始两件：P1=10，P2=6
        cargo.register_packages(
            request_id="pkgs", actor_id="carrier-1", shipment_id=sid,
            packages=[{"package_id": "P1", "quantity": 10, "weight": 100},
                      {"package_id": "P2", "quantity": 6, "weight": 60}])

        # 仓位与两个航段
        cargo.register_location(request_id="loc", actor_id="warehouse-1",
                                location_id="L1", code="PVG-A-01", capacity_qty=20)
        lease = cargo.lease_space(request_id="lease", actor_id="warehouse-1", shipment_id=sid,
                                  location_id="L1", quantity=16)
        cargo.register_segment(request_id="seg-a", actor_id="carrier-1", segment_id="SEG-A",
                               flight_no="CA100", origin="PVG", destination="FRA",
                               departs_at=DAY2, capacity_qty=20, capacity_weight=500)
        cargo.register_segment(request_id="seg-b", actor_id="carrier-1", segment_id="SEG-B",
                               flight_no="CA200", origin="PVG", destination="FRA",
                               departs_at=DAY5, capacity_qty=20, capacity_weight=500)

        # ------------------------------------------------------------ 到港（重复扫描幂等）
        arrival = cargo.arrive_packages(request_id="arr", actor_id="carrier-1",
                                        shipment_id=sid, package_ids=["P1", "P2"],
                                        at=DAY1, scan_token="SCAN-ARR-1")
        arrival_replay = cargo.arrive_packages(request_id="arr-replay", actor_id="carrier-1",
                                               shipment_id=sid, package_ids=["P1", "P2"],
                                               at=DAY1, scan_token="SCAN-ARR-1")
        assert arrival.data["newly_arrived"] == ["P1", "P2"]
        assert arrival_replay.data.get("replayed_scan") is True

        # 地面接收交接，产生 50 元操作费
        cargo.handoff(request_id="ho-1", actor_id="carrier-1", shipment_id=sid,
                      kind="ground_intake", from_actor_id="carrier-1",
                      to_actor_id="warehouse-1", package_ids=["P1", "P2"],
                      location="PVG-A", at=DAY1, handling_fee=50)

        # ------------------------------------------------------------ 分拣拆分（数量守恒）
        split = cargo.split_packages(
            request_id="split-1", actor_id="sorting-1", shipment_id=sid, reason="查验分流",
            splits=[{"source_package_id": "P2",
                     "children": [{"package_id": "P2A", "quantity": 2, "weight": 20},
                                  {"package_id": "P2B", "quantity": 4, "weight": 40}]}])
        split_batch = split.data["batch_id"]

        cargo.place_packages(request_id="place", actor_id="warehouse-1", shipment_id=sid,
                             package_ids=["P1", "P2A", "P2B"], location_id="L1",
                             lease_id=lease.data["lease_id"], at=DAY1, scan_token="SCAN-PLACE-1")

        # ------------------------------------------------------------ 申报与整体配舱
        cargo.submit_declaration(request_id="decl-1", actor_id="carrier-1", shipment_id=sid,
                                 package_ids=["P1", "P2A", "P2B"],
                                 payload={"goods": "通用机电零件", "value": 10000})
        # 监管决定前整票先在原航段占位（占位即锁定容量，但未放行不能确认）
        cargo.allocate_segment(request_id="alloc-all", actor_id="carrier-1", shipment_id=sid,
                               segment_id="SEG-A", package_ids=["P1", "P2A", "P2B"],
                               scan_token="SCAN-ALLOC-1")

        # P2A 申报冲突：海关要求补件并扣留；P1、P2B 放行
        cargo.issue_decision(request_id="dec-supp", actor_id="customs-1", shipment_id=sid,
                             kind="request_supplement", package_ids=["P2A"],
                             reason="申报品名与发票冲突", effective_at=DAY1)
        sensitive_decl = cargo.submit_declaration(
            request_id="decl-sensitive", actor_id="carrier-1", shipment_id=sid,
            package_ids=["P2A"], payload={"goods": "精密温控元件", "hs_code": "90321000",
                                          "end_user": "受限名单核查"},
            sensitive=True, effective_at=DAY1)
        cargo.issue_decision(request_id="dec-hold", actor_id="customs-1", shipment_id=sid,
                             kind="hold", package_ids=["P2A"], reason="申报冲突待补件",
                             effective_at=DAY1)
        cargo.issue_decision(request_id="dec-rel-ok", actor_id="customs-1", shipment_id=sid,
                             kind="release", package_ids=["P1", "P2B"],
                             reason="无争议放行", effective_at=DAY1)

        # 敏感申报：分拣/异常席不可见，海关可见
        status_for_sorting = cargo.shipment_status(actor_id="sorting-1", shipment_id=sid)
        sensitive_items = [d for d in status_for_sorting["declarations"] if d["sensitive"]]
        assert sensitive_items and all(d["payload"] is None for d in sensitive_items), \
            "敏感申报对非海关泄露"
        try:
            cargo.get_declaration(actor_id="exception-1",
                                  declaration_id=sensitive_decl.data["declaration_id"])
            raise AssertionError("异常席不应读到敏感申报")
        except PermissionDenied:
            pass
        customs_decl = cargo.get_declaration(actor_id="customs-1",
                                            declaration_id=sensitive_decl.data["declaration_id"])
        assert customs_decl["payload"]["hs_code"] == "90321000"

        # ------------------------------------------------------------ 无争议货物继续航程
        clock.set(datetime(2026, 10, 2, 9, 0, tzinfo=timezone.utc))
        cargo.confirm_allocation(request_id="conf-a", actor_id="carrier-1", shipment_id=sid,
                                 segment_id="SEG-A", package_ids=["P1", "P2B"])
        cargo.pickup_packages(request_id="pick-ok", actor_id="warehouse-1", shipment_id=sid,
                              package_ids=["P1", "P2B"], at=DAY2)
        cargo.handoff(request_id="ho-2", actor_id="warehouse-1", shipment_id=sid,
                      kind="airline_delivery", from_actor_id="warehouse-1",
                      to_actor_id="carrier-1", package_ids=["P1", "P2B"], at=DAY2)
        cargo.depart_packages(request_id="dep-a", actor_id="carrier-1", shipment_id=sid,
                              segment_id="SEG-A", package_ids=["P1", "P2B"], at=DAY2)

        # 被扣留的 P2A 不得确认配舱；其占位仍然锁定原航段容量
        try:
            cargo.confirm_allocation(request_id="conf-a-p2a", actor_id="carrier-1",
                                     shipment_id=sid, segment_id="SEG-A",
                                     package_ids=["P2A"])
            raise AssertionError("被扣留包装不应允许确认配舱")
        except ConflictError as exc:
            assert "放行条件" in str(exc) or "held" in str(exc)

        # 同一包装不能在第二个航段并发占位
        try:
            cargo.allocate_segment(request_id="alloc-b-p2a-dup", actor_id="carrier-1",
                                   shipment_id=sid, segment_id="SEG-B",
                                   package_ids=["P2A"])
            raise AssertionError("同一包装不应同时出现在两个航段")
        except ConflictError:
            pass

        # ------------------------------------------------------------ 异常席：事中延误牵连
        clock.set(datetime(2026, 10, 3, 10, 0, tzinfo=timezone.utc))
        mid_impact = cargo.delay_impact(actor_id="exception-1", shipment_id=sid, at=DAY3,
                                        reason="申报冲突扣留导致改签")
        assert mid_impact["affected_package_count"] == 1
        affected = mid_impact["affected_packages"][0]
        assert affected["package_id"] == "P2A"
        assert {"held", "supplement_requested"} <= set(affected["reasons"])
        actions = {step["action"] for step in mid_impact["recovery_plan"]}
        assert {"await_release", "submit_supplement"} <= actions
        assert {step["package_id"] for step in mid_impact["recovery_plan"]} == {"P2A"}, \
            "恢复路线不得扩大冻结范围"
        # 待办次序中 P2A 的事项排在已离场包装之前，且已离场包装不产生待办
        todo_packages = {item["package_id"] for item in mid_impact["todo"]}
        assert "P1" not in todo_packages and "P2B" not in todo_packages
        assert todo_packages == {"P2A"}

        # ------------------------------------------------------------ 补件、迟到决定、放行
        supp_req = cargo.issue_decision(
            request_id="dec-supp-ref", actor_id="customs-1", shipment_id=sid,
            kind="request_supplement", package_ids=["P2A"],
            declaration_id=sensitive_decl.data["declaration_id"],
            reason="请补充温控说明", effective_at=DAY3)
        supplement = cargo.submit_supplement(request_id="supp-1", actor_id="carrier-1",
                                             decision_id=supp_req.data["decision_id"],
                                             payload={"goods": "精密温控元件",
                                                      "temp_control": "2-8C",
                                                      "end_user_checked": True})
        cargo.issue_decision(request_id="dec-rel-p2a", actor_id="customs-1", shipment_id=sid,
                             kind="release", package_ids=["P2A"], reason="补件合格放行",
                             effective_at=DAY3)

        # 迟到的查验决定：生效时间回溯到 DAY1，但记录更晚；只拦住尚未起飞的 P2A
        late_inspect = cargo.issue_decision(request_id="dec-late-inspect",
                                            actor_id="customs-1", shipment_id=sid,
                                            kind="inspect", package_ids=["P2A"],
                                            reason="风险系统事后命中，按生效时间补查验",
                                            effective_at=DAY1)
        assert late_inspect.data["late"] is True
        status_late = cargo.shipment_status(actor_id="exception-1", shipment_id=sid)
        p2a_late = next(p for b in status_late["batches"] for p in b["packages"]
                        if p["package_id"] == "P2A")
        assert p2a_late["regulatory"]["inspection_pending"] is True
        flown = {p["package_id"]: p for b in status_late["batches"] for p in b["packages"]
                 if p["package_id"] in ("P1", "P2B")}
        assert all(p["stage"] == "departed" for p in flown.values())

        # 已发生的交接与费用事实保持原样（记录此刻快照）
        facts_before = {
            "handoffs": [(h["handoff_id"], h["occurred_at"]) for h in status_late["handoffs"]],
            "fees": sorted((f["fee_id"], f["amount"], f["responsible_party"])
                           for f in status_late["fees"]),
        }

        clock.set(datetime(2026, 10, 4, 6, 0, tzinfo=timezone.utc))
        cargo.inspect_packages(request_id="insp-p2a", actor_id="customs-1", shipment_id=sid,
                               package_ids=["P2A"], at=DAY4)
        # 重复查验请求不得重复扣减或重复登记
        cargo.inspect_packages(request_id="insp-p2a", actor_id="customs-1", shipment_id=sid,
                               package_ids=["P2A"], at=DAY4)
        cargo.clear_inspection(request_id="clear-insp", actor_id="customs-1",
                               decision_id=late_inspect.data["decision_id"])
        # 补查验无异常，海关针对该包装重新放行（不影响其他包装）
        cargo.issue_decision(request_id="dec-rel-final", actor_id="customs-1", shipment_id=sid,
                             kind="release", package_ids=["P2A"], reason="补查验无异常放行",
                             effective_at=DAY4)

        # ------------------------------------------------------------ 改签：旧仓位作废、新航段占位
        rebook = cargo.rebook_segment(
            request_id="rebook-1", actor_id="carrier-1", shipment_id=sid,
            package_ids=["P2A"], from_segment_id="SEG-A", to_segment_id="SEG-B",
            at=DAY4, rebooking_fee=300, fee_party="carrier-1")
        assert rebook.data["state"] == "held"
        # 旧占位保留为 rebooked 事实，容量只计新占位：SEG-A 剩余容量不含 P2A
        status_now = cargo.shipment_status(actor_id="carrier-1", shipment_id=sid)
        p2a_allocs = [a for a in status_now["allocations"] if a["package_id"] == "P2A"]
        states = {a["state"] for a in p2a_allocs}
        assert {"rebooked", "held"} <= states

        cargo.pickup_packages(request_id="pick-p2a", actor_id="warehouse-1", shipment_id=sid,
                              package_ids=["P2A"], at=DAY4)
        cargo.handoff(request_id="ho-3", actor_id="warehouse-1", shipment_id=sid,
                      kind="airline_delivery", from_actor_id="warehouse-1",
                      to_actor_id="carrier-1", package_ids=["P2A"], at=DAY4)
        cargo.confirm_allocation(request_id="conf-b", actor_id="carrier-1", shipment_id=sid,
                                 segment_id="SEG-B", package_ids=["P2A"])

        # 异常席事后影响面（P2A 仍在途、SEG-B 已锁定）：航段、费用归属、承诺
        impact = cargo.delay_impact(actor_id="exception-1", shipment_id=sid, at=DAY4,
                                    reason="复盘")
        seg_ids = {s["segment_id"] for s in impact["locked_segments"]}
        assert "SEG-B" in seg_ids
        seg_b = next(s for s in impact["locked_segments"] if s["segment_id"] == "SEG-B")
        assert seg_b["package_ids"] == ["P2A"] and seg_b["quantity"] == 2
        assert impact["fees_total"] == 350.0
        assert impact["fees_by_party"]["carrier-1"] == 350.0
        assert impact["commitment"]["at_risk"] is True

        clock.set(datetime(2026, 10, 5, 9, 0, tzinfo=timezone.utc))
        cargo.depart_packages(request_id="dep-b", actor_id="carrier-1", shipment_id=sid,
                              segment_id="SEG-B", package_ids=["P2A"], at=DAY5)

        # ------------------------------------------------------------ 事后核对
        final_status = cargo.shipment_status(actor_id="exception-1", shipment_id=sid)
        facts_after = {
            "handoffs": [(h["handoff_id"], h["occurred_at"]) for h in final_status["handoffs"]],
            "fees": sorted((f["fee_id"], f["amount"], f["responsible_party"])
                           for f in final_status["fees"]),
        }
        # 早期交接与 50 元操作费在迟到决定、改签后保持原样
        assert facts_before["handoffs"][0] in facts_after["handoffs"]
        early_fees = [f for f in facts_before["fees"] if f[1] == 50.0]
        assert early_fees and all(f in facts_after["fees"] for f in early_fees)
        # 改签费 300 元归属承运人
        rebooking_fees = [f for f in facts_after["fees"] if f[1] == 300.0]
        assert rebooking_fees and rebooking_fees[0][2] == "carrier-1"

        # 数量守恒：初始 10+6，现存叶节点 P1(10)+P2A(2)+P2B(4)=16
        leaf_qty = sum(p["quantity"] for b in final_status["batches"] for p in b["packages"]
                       if p["package_id"] in ("P1", "P2A", "P2B"))
        assert leaf_qty == 16
        assert final_status["live_package_count"] == 0

        # ------------------------------------------------------------ 系统恢复：同库重建
        database.close()
        database2 = Database(db_path)
        base2 = DomainService(database2, clock)
        cargo2 = CargoService(database2, clock)
        recovered = cargo2.shipment_status(actor_id="exception-1", shipment_id=sid)
        stages = {p["package_id"]: p["stage"] for b in recovered["batches"] for p in b["packages"]}
        assert stages["P1"] == "departed" and stages["P2B"] == "departed"
        assert stages["P2A"] == "departed"
        # 锁定容量视图仍可从历史分配重建
        assert {a["segment_id"] for a in recovered["allocations"]} >= {"SEG-A", "SEG-B"}
        valid, event_count = base2.verify_audit()
        assert valid and event_count > 0
        database2.close()
        return {"status": "ok", "audit_valid": valid, "audit_events": event_count,
                "split_batch": split_batch,
                "supplement_declaration_id": supplement.data["declaration_id"],
                "rebooking_allocation_ids": len(rebook.data["allocation_ids"]),
                "mid_affected_packages": mid_impact["affected_package_count"],
                "fees_total": impact["fees_total"],
                "commitment_at_risk": impact["commitment"]["at_risk"],
                "leaf_quantity": leaf_qty,
                "todo_after_recovery": len(recovered["todo"])}


def main() -> int:
    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "ok" and result["audit_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
