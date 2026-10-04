"""航空枢纽异常协同服务的离线端到端验收。

演练完整异常链路：登记 → 扫描（含重复扫描）→ 到港 → 拆分 → 合并 → 仓位租约 →
航段占位 → 敏感申报 → 迟到的补件决定（只覆盖部分包裹）→ 查验 → 补件 →
部分放行 → 交接 → 航段延误 → 影响计算与恢复路线 → 改签 → 退运 → 出港，
最后关闭并重新打开数据库，确认异常席在系统恢复后仍看到一致的在途状态与审计链。
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from ..clock import FixedClock
from ..service import DomainService
from ..storage import Database
from .service import AirExceptionService

CLOCK = FixedClock(datetime(2026, 10, 4, 8, 0, tzinfo=timezone.utc))


def _services(path) -> tuple[Database, DomainService, AirExceptionService]:
    database = Database(path)
    foundation = DomainService(database, CLOCK)
    return database, foundation, AirExceptionService(foundation)


def _bootstrap(foundation: DomainService) -> None:
    foundation.register_organization(request_id="req-org-hub", actor_id="bootstrap",
                                     organization_id="org-hub", name="枢纽运营方")
    foundation.register_actor(request_id="req-admin", actor_id="bootstrap", new_actor_id="admin-1",
                              display_name="系统管理员", role="admin", organization_id="org-hub")
    foundation.register_organization(request_id="req-org-carrier", actor_id="admin-1",
                                     organization_id="org-carrier", name="承运航空公司")
    foundation.register_organization(request_id="req-org-customs", actor_id="admin-1",
                                     organization_id="org-customs", name="口岸海关")
    foundation.register_actor(request_id="req-carrier", actor_id="admin-1", new_actor_id="carrier-1",
                              display_name="承运人代表", role="carrier", organization_id="org-carrier")
    foundation.register_actor(request_id="req-sorter", actor_id="admin-1", new_actor_id="sorter-1",
                              display_name="分拣班长", role="sorter", organization_id="org-hub")
    foundation.register_actor(request_id="req-warehouse", actor_id="admin-1", new_actor_id="warehouse-1",
                              display_name="仓储主管", role="warehouse", organization_id="org-hub")
    foundation.register_actor(request_id="req-customs", actor_id="admin-1", new_actor_id="customs-1",
                              display_name="海关关员", role="customs", organization_id="org-customs")
    foundation.register_actor(request_id="req-desk", actor_id="admin-1", new_actor_id="desk-1",
                              display_name="异常席", role="desk", organization_id="org-hub")
    foundation.register_site(request_id="req-site", actor_id="admin-1", site_id="hub-01",
                             organization_id="org-hub", name="一号航空物流枢纽",
                             timezone_name="Asia/Shanghai")


def _phase_one(air: AirExceptionService) -> dict[str, object]:
    air.register_slot(request_id="req-slot-a1", actor_id="warehouse-1", site_id="hub-01", slot_code="A1")
    air.register_slot(request_id="req-slot-a2", actor_id="warehouse-1", site_id="hub-01", slot_code="A2")
    air.register_segment(request_id="req-seg-1", actor_id="carrier-1", site_id="hub-01",
                         segment_id="SEG1", flight_no="CA101", origin="HUB", destination="FRA",
                         departs_at="2026-10-04T14:00:00Z", arrives_at="2026-10-04T20:00:00Z",
                         capacity_pieces=100, capacity_weight=1000)
    air.register_segment(request_id="req-seg-2", actor_id="carrier-1", site_id="hub-01",
                         segment_id="SEG2", flight_no="CA102", origin="HUB", destination="FRA",
                         departs_at="2026-10-04T16:00:00Z", arrives_at="2026-10-04T20:45:00Z",
                         capacity_pieces=100, capacity_weight=1000)
    air.register_waybill(request_id="req-wb-1", actor_id="carrier-1", site_id="hub-01",
                         waybill_id="WB1", origin="HUB", destination="FRA",
                         declared_pieces=10, declared_weight=100,
                         promised_arrival_at="2026-10-04T21:00:00Z")
    packages = [{"package_id": f"P{i:02d}", "pieces": 1, "weight": 10} for i in range(1, 11)]
    air.scan_packages(request_id="req-scan-1", actor_id="carrier-1", waybill_id="WB1", packages=packages)
    # 重复扫描：同一包裹再次到达不产生第二次计数
    air.scan_packages(request_id="req-scan-2", actor_id="sorter-1", waybill_id="WB1",
                      packages=[{"package_id": "P01", "pieces": 1, "weight": 10}])
    replay = air.scan_packages(request_id="req-scan-1", actor_id="carrier-1", waybill_id="WB1",
                               packages=packages)
    arrival = air.arrive(request_id="req-arrive", actor_id="carrier-1", waybill_id="WB1")
    batch_main = arrival.resource_id
    first_split = air.split_batch(request_id="req-split-1", actor_id="sorter-1",
                                  batch_id=batch_main, package_ids=["P01", "P02"])
    second_split = air.split_batch(request_id="req-split-2", actor_id="sorter-1",
                                   batch_id=batch_main, package_ids=["P03", "P04"])
    air.merge_batches(request_id="req-merge", actor_id="sorter-1", target_batch_id=batch_main,
                      source_batch_ids=[first_split.resource_id, second_split.resource_id])
    lease = air.lease_slot(request_id="req-lease", actor_id="warehouse-1",
                           batch_id=batch_main, slot_code="A1")
    air.complete_sort(request_id="req-sort", actor_id="sorter-1", batch_id=batch_main)
    air.book_segment(request_id="req-book-1", actor_id="carrier-1", batch_id=batch_main, segment_id="SEG1")
    air.submit_declaration(request_id="req-decl-1", actor_id="carrier-1", waybill_id="WB1",
                           data={"hs_code": "850760", "items": ["锂电池"], "secret": "仅海关可见"})
    # 迟到的监管决定：07:00 生效，08:00 才到达，只覆盖 P05、P06
    hold = air.record_decision(request_id="req-dec-hold", actor_id="customs-1", waybill_id="WB1",
                               decision_type="supplement", package_ids=["P05", "P06"],
                               effective_at="2026-10-04T07:00:00Z", note="申报冲突，要求补件")
    detail = air.waybill_detail(actor_id="desk-1", waybill_id="WB1")
    hold_batch = next(b for b in detail["batches"] if b["status"] == "held")
    air.complete_inspection(request_id="req-inspect", actor_id="customs-1",
                            batch_id=hold_batch["batch_id"])
    air.submit_declaration(request_id="req-decl-2", actor_id="carrier-1", waybill_id="WB1",
                           data={"hs_code": "850760", "items": ["锂电池"], "supplement": "补充鉴定报告"})
    # 部分放行：P05 恢复流转，P06 继续扣留
    air.record_decision(request_id="req-dec-release", actor_id="customs-1", waybill_id="WB1",
                        decision_type="release", package_ids=["P05"],
                        effective_at="2026-10-04T08:00:00Z")
    air.record_handover(request_id="req-handover", actor_id="carrier-1",
                        batch_id=batch_main, to_role="carrier")
    air.release_slot(request_id="req-release-slot", actor_id="warehouse-1", lease_id=lease.resource_id)
    air.update_segment_status(request_id="req-delay", actor_id="carrier-1", segment_id="SEG1",
                              status="delayed", new_departs_at="2026-10-04T20:00:00Z",
                              new_arrives_at="2026-10-05T02:00:00Z")
    impact = air.delay_impact(actor_id="desk-1", segment_id="SEG1")
    air.rebook_segment(request_id="req-rebook", actor_id="desk-1", batch_id=batch_main,
                       new_segment_id="SEG2", reason="SEG1 延误六小时")
    air.record_decision(request_id="req-dec-return", actor_id="customs-1", waybill_id="WB1",
                        decision_type="return", package_ids=["P06"],
                        effective_at="2026-10-04T08:00:00Z", note="补件仍不合规，责令退运")
    board = air.desk_board(actor_id="desk-1", site_id="hub-01")
    air.execute_return(request_id="req-return", actor_id="carrier-1", batch_id=hold_batch["batch_id"])
    detail = air.waybill_detail(actor_id="desk-1", waybill_id="WB1")
    resumed = next(b for b in detail["batches"]
                   if b["status"] == "open" and b["batch_id"] != batch_main)
    air.book_segment(request_id="req-book-2", actor_id="carrier-1",
                     batch_id=resumed["batch_id"], segment_id="SEG2")
    air.record_handover(request_id="req-handover-2", actor_id="carrier-1",
                        batch_id=resumed["batch_id"], to_role="carrier")
    air.update_segment_status(request_id="req-depart", actor_id="carrier-1",
                              segment_id="SEG2", status="departed")
    return {"replay": replay, "hold_decision": hold, "impact": impact,
            "board_midflow": board, "batch_main": batch_main}


def run() -> dict[str, object]:
    """执行完整异常链路并返回验收结果。"""

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "air.sqlite3"
        database, foundation, air = _services(path)
        _bootstrap(foundation)
        outcome = _phase_one(air)
        detail = air.waybill_detail(actor_id="desk-1", waybill_id="WB1")
        decision = air.get_decision(actor_id="customs-1",
                                    decision_id=outcome["hold_decision"].resource_id)
        declarations_as_customs = air.list_declarations(actor_id="customs-1", waybill_id="WB1")
        declarations_as_sorter = air.list_declarations(actor_id="sorter-1", waybill_id="WB1")
        valid, event_count = foundation.verify_audit()
        board_before = air.desk_board(actor_id="desk-1", site_id="hub-01")
        database.close()

        # 模拟系统恢复：重新打开同一数据库，异常席看到的在途状态必须一致
        database2, foundation2, air2 = _services(path)
        board_after = air2.desk_board(actor_id="desk-1", site_id="hub-01")
        valid_after, _ = foundation2.verify_audit()
        restart_consistent = (
            [b["batch_id"] for b in board_before["in_transit"]]
            == [b["batch_id"] for b in board_after["in_transit"]]
            and board_before["locked_capacity"] == board_after["locked_capacity"]
        )
        database2.close()

        impact = outcome["impact"]
        result = {
            "status": "ok",
            "audit_valid": valid and valid_after,
            "audit_events": event_count,
            "scan_replayed": outcome["replay"].replayed,
            "conserved": detail["conservation"]["conserved"],
            "decision_late": decision["effect"]["late"],
            "preserved_facts": len(decision["effect"]["preserved_facts"]),
            "impact_at_risk": impact["affected"]["commitments"][0]["at_risk"],
            "recovery_target": impact["recovery"]["proposals"][0]["to_segment_id"],
            "freeze_scope_expanded": impact["recovery"]["freeze_scope_expanded"],
            "held_excluded": len(impact["recovery"]["excluded_held_packages"]),
            "midflow_todos": sorted({todo["kind"] for todo in outcome["board_midflow"]["todos"]}),
            "declaration_restricted": declarations_as_sorter[0]["restricted"]
            and declarations_as_sorter[0]["payload"] is None,
            "declaration_visible": declarations_as_customs[0]["payload"] is not None,
            "restart_consistent": restart_consistent,
            "batches": len(detail["batches"]),
            "fees": len(detail["fees"]),
        }
        checks = [
            result["audit_valid"], result["scan_replayed"], result["conserved"],
            result["decision_late"], result["preserved_facts"] >= 3,
            result["impact_at_risk"], result["recovery_target"] == "SEG2",
            result["freeze_scope_expanded"] is False, result["held_excluded"] == 1,
            "booking_needed" in result["midflow_todos"],
            "return_pending" in result["midflow_todos"],
            result["declaration_restricted"], result["declaration_visible"],
            result["restart_consistent"],
        ]
        if not all(checks):
            result["status"] = "failed"
        return result


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
