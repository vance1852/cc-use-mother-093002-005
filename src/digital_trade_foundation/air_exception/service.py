"""航空、分拣、仓储与海关共同使用的异常协同服务。

把主运单、子批次、包装单元、申报版本、仓位租约、航段容量、监管决定和
责任交接串成连续链路，并保证：

- 监管决定只作用于显式覆盖的包装单元，未涉及单元继续流转；
- 拆分/合并前后件数守恒，包装单元在任意时刻只属于一个批次；
- 重复扫描与重复占位不会重复扣减库存或容量；
- 迟到的监管决定只调整尚未完成的路径，已经发生的交接与费用事实保持原样；
- 敏感申报内容只对具备职责的角色开放。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from ..audit import append_event, canonical_json, digest
from ..errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from ..models import Actor, WriteReceipt
from ..service import DomainService
from .schema import AIR_SCHEMA

PARTY_ROLES = frozenset({"carrier", "sorter", "warehouse", "customs"})
LOCATION_BY_ROLE = {"carrier": "carrier", "sorter": "sorting",
                    "warehouse": "warehouse", "customs": "customs"}
LOCATION_TO_ROLE = {"sorting": "sorter", "warehouse": "warehouse",
                    "customs": "customs", "carrier": "carrier"}

BOOKING_RATE_PER_KG = 1.5
REBOOK_FLAT_FEE = 80.0
SLOT_LEASE_FLAT_FEE = 20.0
RETURN_FLAT_FEE = 120.0
STORAGE_RATE_PER_KG_DAY = 0.5
FEE_CURRENCY = "CNY"

RESTRICTIVE_DECISIONS = frozenset({"hold", "inspect", "supplement", "return"})
DECISION_TYPES = RESTRICTIVE_DECISIONS | {"release"}

# 异常席待办在同一时刻内的处理优先级：监管事项优先于运力事项
_TODO_PRIORITY = {"inspection_due": 0, "return_pending": 0, "supplement_due": 1,
                  "rebook_advised": 2, "booking_needed": 2, "handover_due": 3}

_SCAN_ROLES = {"admin", "carrier", "sorter", "warehouse"}
_CARRIER_ROLES = {"admin", "carrier"}
_SORT_ROLES = {"admin", "sorter"}
_WAREHOUSE_ROLES = {"admin", "warehouse"}
_CUSTOMS_ROLES = {"admin", "customs"}
_DECLARANT_ROLES = {"admin", "carrier"}
_BOOK_ROLES = {"admin", "carrier", "desk"}
_RETURN_ROLES = {"admin", "carrier", "desk"}
_SEGMENT_ROLES = {"admin", "carrier", "desk"}
_DECLARATION_READERS = {"admin", "customs"}


def _parse_time(value: Any, field: str) -> datetime:
    """把输入时间解析为带时区的 UTC 时间。"""

    text = str(value).strip()
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError(f"{field} 时间格式无效") from exc
    if moment.tzinfo is None:
        raise ValidationError(f"{field} 必须包含时区")
    return moment.astimezone(timezone.utc)


def _format_time(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class AirExceptionService:
    """在基础服务的权限、幂等、事务与审计边界上实现异常协同链路。"""

    def __init__(self, foundation: DomainService) -> None:
        self.foundation = foundation
        self.database = foundation.database
        self.clock = foundation.clock
        self.database.connection.executescript(AIR_SCHEMA)

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def _now(self) -> datetime:
        return self.clock.now().astimezone(timezone.utc)

    def _now_text(self) -> str:
        return _format_time(self._now())

    def _actor(self, connection, actor_id: str) -> Actor:
        return self.foundation._actor(connection, actor_id)

    def _require(self, actor: Actor, roles: set[str]) -> None:
        if actor.role not in roles:
            raise PermissionDenied("当前角色不能执行该动作")

    def _identifier(self, value: Any, field: str) -> str:
        return self.foundation._identifier(str(value), field)

    def _idempotent(self, connection, *, request_id: str, action: str,
                    payload: dict[str, Any], create) -> WriteReceipt:
        return self.foundation._idempotent(connection, request_id=request_id,
                                           action=action, payload=payload, create=create)

    def _waybill(self, connection, waybill_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM air_waybills WHERE waybill_id=?", (waybill_id,)).fetchone()
        if row is None:
            raise NotFoundError("主运单不存在")
        return row

    def _batch(self, connection, batch_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM air_batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError("批次不存在")
        return row

    def _segment(self, connection, segment_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM air_segments WHERE segment_id=?", (segment_id,)).fetchone()
        if row is None:
            raise NotFoundError("航段不存在")
        return row

    def _batch_totals(self, connection, batch_id: str) -> tuple[int, float]:
        row = connection.execute(
            "SELECT COALESCE(SUM(pieces),0) AS pieces, COALESCE(SUM(weight),0) AS weight "
            "FROM air_packages WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        return int(row["pieces"]), float(row["weight"])

    def _active_lease(self, connection, batch_id: str) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM air_slot_leases WHERE batch_id=? AND status='active'", (batch_id,)
        ).fetchone()

    def _active_booking(self, connection, batch_id: str) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM air_bookings WHERE batch_id=? AND status='active'", (batch_id,)
        ).fetchone()

    def _pending_steps(self, connection, batch_id: str, step_type: str | None = None) -> list[sqlite3.Row]:
        query = "SELECT * FROM air_steps WHERE batch_id=? AND status='pending'"
        parameters: list[Any] = [batch_id]
        if step_type:
            query += " AND step_type=?"
            parameters.append(step_type)
        query += " ORDER BY position"
        return connection.execute(query, parameters).fetchall()

    def _add_step(self, connection, batch_id: str, step_type: str,
                  segment_id: str | None, now: str) -> str:
        position = connection.execute(
            "SELECT COALESCE(MAX(position)+1,0) AS next FROM air_steps WHERE batch_id=?", (batch_id,)
        ).fetchone()["next"]
        step_id = uuid.uuid4().hex
        connection.execute(
            "INSERT INTO air_steps(step_id,batch_id,position,step_type,segment_id,status,created_at) "
            "VALUES(?,?,?,?,?,'pending',?)",
            (step_id, batch_id, position, step_type, segment_id, now),
        )
        return step_id

    def _complete_step(self, connection, step_id: str, now: str) -> None:
        connection.execute(
            "UPDATE air_steps SET status='completed', occurred_at=? WHERE step_id=?", (now, step_id)
        )

    def _cancel_pending_steps(self, connection, batch_id: str, effect: dict[str, Any],
                              step_type: str | None = None) -> None:
        for step in self._pending_steps(connection, batch_id, step_type):
            connection.execute("UPDATE air_steps SET status='cancelled' WHERE step_id=?", (step["step_id"],))
            effect["cancelled_steps"].append(step["step_id"])

    def _create_batch(self, connection, waybill_id: str, kind: str,
                      status: str, location: str, now: str) -> str:
        batch_id = uuid.uuid4().hex
        connection.execute(
            "INSERT INTO air_batches(batch_id,waybill_id,kind,status,location,created_at) VALUES(?,?,?,?,?,?)",
            (batch_id, waybill_id, kind, status, location, now),
        )
        return batch_id

    def _move_packages(self, connection, package_ids: list[str], batch_id: str) -> None:
        for package_id in package_ids:
            connection.execute("UPDATE air_packages SET batch_id=? WHERE package_id=?", (batch_id, package_id))

    def _adjust_booking(self, connection, batch_id: str) -> None:
        """让有效占位始终等于批次当前件数与重量，拆合后容量自动回吐或补足。"""

        booking = self._active_booking(connection, batch_id)
        if booking is None:
            return
        pieces, weight = self._batch_totals(connection, batch_id)
        connection.execute(
            "UPDATE air_bookings SET pieces=?, weight=? WHERE booking_id=?", (pieces, weight, booking["booking_id"])
        )

    def _release_lease(self, connection, batch_id: str, now: str, effect: dict[str, Any]) -> None:
        lease = self._active_lease(connection, batch_id)
        if lease:
            connection.execute(
                "UPDATE air_slot_leases SET status='released', ends_at=? WHERE lease_id=?",
                (now, lease["lease_id"]),
            )
            effect["released_leases"].append(lease["lease_id"])

    def _release_booking(self, connection, batch_id: str, effect: dict[str, Any]) -> None:
        booking = self._active_booking(connection, batch_id)
        if booking:
            connection.execute(
                "UPDATE air_bookings SET status='released' WHERE booking_id=?", (booking["booking_id"],)
            )
            effect["released_bookings"].append(booking["booking_id"])

    def _resume_steps(self, connection, batch_id: str, now: str) -> None:
        """批次恢复流转后按真实业务顺序补齐尚未完成的路径。"""

        if self._active_lease(connection, batch_id) is None:
            self._add_step(connection, batch_id, "store", None, now)
        if self._active_booking(connection, batch_id) is None:
            self._add_step(connection, batch_id, "load", None, now)
        self._add_step(connection, batch_id, "handover", None, now)
        self._add_step(connection, batch_id, "depart", None, now)

    def _standard_steps(self, connection, batch_id: str, now: str) -> None:
        for step_type in ("sort", "store", "load", "handover", "depart"):
            self._add_step(connection, batch_id, step_type, None, now)

    def _record_fee(self, connection, *, waybill_id: str, batch_id: str | None,
                    segment_id: str | None, category: str, amount: float,
                    now: str, note: str) -> str:
        """追加一条不可变的费用事实。"""

        fee_id = uuid.uuid4().hex
        connection.execute(
            "INSERT INTO air_fees(fee_id,waybill_id,batch_id,segment_id,category,amount,currency,incurred_at,note) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (fee_id, waybill_id, batch_id, segment_id, category,
             round(float(amount), 2), FEE_CURRENCY, now, note),
        )
        return fee_id

    def _record_handover(self, connection, *, batch_id: str, from_role: str,
                         to_role: str, now: str, actor_id: str) -> str:
        """追加一条不可变的责任交接事实。"""

        handover_id = uuid.uuid4().hex
        connection.execute(
            "INSERT INTO air_handovers(handover_id,batch_id,from_role,to_role,occurred_at,recorded_by) "
            "VALUES(?,?,?,?,?,?)",
            (handover_id, batch_id, from_role, to_role, now, actor_id),
        )
        return handover_id

    def _custodian(self, connection, batch_id: str, batch: sqlite3.Row | None = None) -> str:
        row = connection.execute(
            "SELECT to_role FROM air_handovers WHERE batch_id=? ORDER BY occurred_at DESC, rowid DESC LIMIT 1",
            (batch_id,),
        ).fetchone()
        if row:
            return row["to_role"]
        batch = batch if batch is not None else self._batch(connection, batch_id)
        return LOCATION_TO_ROLE.get(batch["location"], "carrier")

    def _new_effect(self, late: bool) -> dict[str, Any]:
        return {"late": late, "cancelled_steps": [], "adjusted_batches": [],
                "released_leases": [], "released_bookings": [], "resumed_batches": [],
                "preserved_facts": []}

    # ------------------------------------------------------------------
    # 登记：航段、仓位、主运单
    # ------------------------------------------------------------------

    def register_segment(self, *, request_id: str, actor_id: str, site_id: str, segment_id: str,
                         flight_no: str, origin: str, destination: str, departs_at: str,
                         arrives_at: str, capacity_pieces: int, capacity_weight: float) -> WriteReceipt:
        payload = {"actor_id": actor_id, "site_id": site_id, "segment_id": segment_id,
                   "flight_no": flight_no, "origin": origin, "destination": destination,
                   "departs_at": departs_at, "arrives_at": arrives_at,
                   "capacity_pieces": capacity_pieces, "capacity_weight": capacity_weight}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _SEGMENT_ROLES)
            if connection.execute("SELECT 1 FROM sites WHERE site_id=?", (site_id,)).fetchone() is None:
                raise NotFoundError("场所不存在")
            segment_id = self._identifier(segment_id, "segment_id")
            flight_no = self.foundation._text(flight_no, "flight_no", 40)
            origin = self.foundation._text(origin, "origin", 40)
            destination = self.foundation._text(destination, "destination", 40)
            departs = _parse_time(departs_at, "departs_at")
            arrives = _parse_time(arrives_at, "arrives_at")
            if arrives <= departs:
                raise ValidationError("arrives_at 必须晚于 departs_at")
            if int(capacity_pieces) <= 0 or float(capacity_weight) <= 0:
                raise ValidationError("航段容量必须为正数")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO air_segments(segment_id,site_id,flight_no,origin,destination,departs_at,"
                        "arrives_at,capacity_pieces,capacity_weight,status,delay_minutes,version,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,'scheduled',0,1,?)",
                        (segment_id, site_id, flight_no, origin, destination, _format_time(departs),
                         _format_time(arrives), int(capacity_pieces), float(capacity_weight), now),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ConflictError("航段编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="air.segment.registered",
                             resource_type="segment", resource_id=segment_id,
                             detail={"site_id": site_id, "flight_no": flight_no}, occurred_at=now)
                return "segment", segment_id, {"segment_id": segment_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.register_segment", payload=payload, create=create)

    def register_slot(self, *, request_id: str, actor_id: str, site_id: str, slot_code: str) -> WriteReceipt:
        payload = {"actor_id": actor_id, "site_id": site_id, "slot_code": slot_code}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _WAREHOUSE_ROLES)
            if connection.execute("SELECT 1 FROM sites WHERE site_id=?", (site_id,)).fetchone() is None:
                raise NotFoundError("场所不存在")
            slot_code = self._identifier(slot_code, "slot_code")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO air_slots(site_id,slot_code,created_at) VALUES(?,?,?)",
                        (site_id, slot_code, now),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ConflictError("仓位编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="air.slot.registered",
                             resource_type="slot", resource_id=slot_code,
                             detail={"site_id": site_id}, occurred_at=now)
                return "slot", slot_code, {"slot_code": slot_code}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.register_slot", payload=payload, create=create)

    def register_waybill(self, *, request_id: str, actor_id: str, site_id: str, waybill_id: str,
                         origin: str, destination: str, declared_pieces: int,
                         declared_weight: float, promised_arrival_at: str | None = None) -> WriteReceipt:
        payload = {"actor_id": actor_id, "site_id": site_id, "waybill_id": waybill_id,
                   "origin": origin, "destination": destination, "declared_pieces": declared_pieces,
                   "declared_weight": declared_weight, "promised_arrival_at": promised_arrival_at}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _CARRIER_ROLES)
            if connection.execute("SELECT 1 FROM sites WHERE site_id=?", (site_id,)).fetchone() is None:
                raise NotFoundError("场所不存在")
            waybill_id = self._identifier(waybill_id, "waybill_id")
            origin = self.foundation._text(origin, "origin", 40)
            destination = self.foundation._text(destination, "destination", 40)
            if int(declared_pieces) <= 0 or float(declared_weight) <= 0:
                raise ValidationError("申报件数与重量必须为正数")
            promised = _format_time(_parse_time(promised_arrival_at, "promised_arrival_at")) \
                if promised_arrival_at else None
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO air_waybills(waybill_id,site_id,origin,destination,declared_pieces,"
                        "declared_weight,promised_arrival_at,status,created_by,created_at) "
                        "VALUES(?,?,?,?,?,?,?,'registered',?,?)",
                        (waybill_id, site_id, origin, destination, int(declared_pieces),
                         float(declared_weight), promised, actor_id, now),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ConflictError("主运单编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="air.waybill.registered",
                             resource_type="waybill", resource_id=waybill_id,
                             detail={"site_id": site_id, "declared_pieces": int(declared_pieces)},
                             occurred_at=now)
                return "waybill", waybill_id, {"waybill_id": waybill_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.register_waybill", payload=payload, create=create)

    # ------------------------------------------------------------------
    # 扫描与到港
    # ------------------------------------------------------------------

    def scan_packages(self, *, request_id: str, actor_id: str, waybill_id: str,
                      packages: list[dict[str, Any]]) -> WriteReceipt:
        """扫描登记包装单元；重复扫描自然去重，不会重复计数。"""

        if not isinstance(packages, list) or not packages:
            raise ValidationError("packages 必须是非空数组")
        payload = {"actor_id": actor_id, "waybill_id": waybill_id, "packages": packages}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _SCAN_ROLES)
            waybill = self._waybill(connection, waybill_id)
            if waybill["status"] != "registered":
                raise ConflictError("运单已到港，不能继续扫描登记")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                total = connection.execute(
                    "SELECT COALESCE(SUM(pieces),0) AS pieces FROM air_packages WHERE waybill_id=?",
                    (waybill_id,),
                ).fetchone()["pieces"]
                inserted = 0
                skipped = 0
                for item in packages:
                    package_id = self._identifier(item.get("package_id", ""), "package_id")
                    pieces = int(item.get("pieces", 0))
                    weight = float(item.get("weight", 0))
                    if pieces <= 0 or weight <= 0:
                        raise ValidationError("包装件数与重量必须为正数")
                    existing = connection.execute(
                        "SELECT * FROM air_packages WHERE package_id=?", (package_id,)
                    ).fetchone()
                    if existing:
                        if (existing["waybill_id"] != waybill_id or existing["pieces"] != pieces
                                or abs(existing["weight"] - weight) > 1e-9):
                            raise ConflictError("包裹编号已被不同内容使用")
                        skipped += 1
                        continue
                    if total + pieces > waybill["declared_pieces"]:
                        raise ValidationError("扫描件数超出运单申报件数")
                    connection.execute(
                        "INSERT INTO air_packages(package_id,waybill_id,batch_id,pieces,weight,scanned_at) "
                        "VALUES(?,?,NULL,?,?,?)",
                        (package_id, waybill_id, pieces, weight, now),
                    )
                    total += pieces
                    inserted += 1
                append_event(connection, actor_id=actor_id, action="air.packages.scanned",
                             resource_type="waybill", resource_id=waybill_id,
                             detail={"inserted": inserted, "skipped": skipped}, occurred_at=now)
                return "waybill", waybill_id, {"waybill_id": waybill_id,
                                               "inserted": inserted, "skipped": skipped}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.scan_packages", payload=payload, create=create)

    def arrive(self, *, request_id: str, actor_id: str, waybill_id: str) -> WriteReceipt:
        """到港：全部申报件数扫描齐备后生成到港批次并交接给分拣。"""

        payload = {"actor_id": actor_id, "waybill_id": waybill_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _CARRIER_ROLES)
            waybill = self._waybill(connection, waybill_id)
            if waybill["status"] != "registered":
                raise ConflictError("运单已经完成到港")
            scanned = connection.execute(
                "SELECT COALESCE(SUM(pieces),0) AS pieces FROM air_packages WHERE waybill_id=?",
                (waybill_id,),
            ).fetchone()["pieces"]
            if scanned != waybill["declared_pieces"]:
                raise ValidationError("到港前必须完成全部申报件数的扫描")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                batch_id = self._create_batch(connection, waybill_id, "arrival", "open", "sorting", now)
                connection.execute(
                    "UPDATE air_packages SET batch_id=? WHERE waybill_id=?", (batch_id, waybill_id)
                )
                self._standard_steps(connection, batch_id, now)
                handover_id = self._record_handover(connection, batch_id=batch_id, from_role="carrier",
                                                    to_role="sorter", now=now, actor_id=actor_id)
                connection.execute("UPDATE air_waybills SET status='arrived' WHERE waybill_id=?", (waybill_id,))
                append_event(connection, actor_id=actor_id, action="air.waybill.arrived",
                             resource_type="waybill", resource_id=waybill_id,
                             detail={"batch_id": batch_id, "handover_id": handover_id}, occurred_at=now)
                return "batch", batch_id, {"batch_id": batch_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.arrive", payload=payload, create=create)

    # ------------------------------------------------------------------
    # 拆分与合并（件数守恒）
    # ------------------------------------------------------------------

    def split_batch(self, *, request_id: str, actor_id: str, batch_id: str,
                    package_ids: list[str]) -> WriteReceipt:
        if not isinstance(package_ids, list) or not package_ids:
            raise ValidationError("package_ids 必须是非空数组")
        package_ids = sorted({self._identifier(pid, "package_id") for pid in package_ids})
        payload = {"actor_id": actor_id, "batch_id": batch_id, "package_ids": package_ids}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _SORT_ROLES)
            batch = self._batch(connection, batch_id)
            if batch["status"] != "open":
                raise ConflictError("只有流转中的批次可以拆分")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                remaining = {row["package_id"] for row in connection.execute(
                    "SELECT package_id FROM air_packages WHERE batch_id=?", (batch_id,))}
                chosen = set(package_ids)
                unknown = chosen - remaining
                if unknown:
                    raise ValidationError(f"包裹不在源批次: {sorted(unknown)}")
                if chosen == remaining:
                    raise ValidationError("拆分必须在源批次保留部分包裹")
                new_batch_id = self._create_batch(connection, batch["waybill_id"], "split",
                                                  "open", batch["location"], now)
                self._move_packages(connection, package_ids, new_batch_id)
                for step in self._pending_steps(connection, batch_id):
                    self._add_step(connection, new_batch_id, step["step_type"], step["segment_id"], now)
                self._adjust_booking(connection, batch_id)
                append_event(connection, actor_id=actor_id, action="air.batch.split",
                             resource_type="batch", resource_id=new_batch_id,
                             detail={"source_batch_id": batch_id, "package_count": len(package_ids)},
                             occurred_at=now)
                return "batch", new_batch_id, {"batch_id": new_batch_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.split_batch", payload=payload, create=create)

    def merge_batches(self, *, request_id: str, actor_id: str, target_batch_id: str,
                      source_batch_ids: list[str]) -> WriteReceipt:
        if not isinstance(source_batch_ids, list) or not source_batch_ids:
            raise ValidationError("source_batch_ids 必须是非空数组")
        source_batch_ids = sorted({self._identifier(bid, "batch_id") for bid in source_batch_ids})
        payload = {"actor_id": actor_id, "target_batch_id": target_batch_id,
                   "source_batch_ids": source_batch_ids}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _SORT_ROLES)
            target = self._batch(connection, target_batch_id)
            if target["status"] != "open":
                raise ConflictError("目标批次必须处于流转中")
            if target_batch_id in source_batch_ids:
                raise ValidationError("目标批次不能同时是来源批次")
            sources = [self._batch(connection, bid) for bid in source_batch_ids]
            for source in sources:
                if source["waybill_id"] != target["waybill_id"]:
                    raise ValidationError("只有同一运单的批次可以合并")
                if source["status"] != "open":
                    raise ConflictError("只有流转中的批次可以合并")
                if self._active_lease(connection, source["batch_id"]) or \
                        self._active_booking(connection, source["batch_id"]):
                    raise ConflictError("来源批次仍有有效租约或占位，不能合并")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                moved = 0
                effect = self._new_effect(late=False)
                for source in sources:
                    package_ids = [row["package_id"] for row in connection.execute(
                        "SELECT package_id FROM air_packages WHERE batch_id=?", (source["batch_id"],))]
                    self._move_packages(connection, package_ids, target_batch_id)
                    moved += len(package_ids)
                    self._cancel_pending_steps(connection, source["batch_id"], effect)
                    connection.execute(
                        "UPDATE air_batches SET status='closed', location='merged' WHERE batch_id=?",
                        (source["batch_id"],),
                    )
                self._adjust_booking(connection, target_batch_id)
                append_event(connection, actor_id=actor_id, action="air.batch.merged",
                             resource_type="batch", resource_id=target_batch_id,
                             detail={"source_batch_ids": source_batch_ids, "package_count": moved},
                             occurred_at=now)
                return "batch", target_batch_id, {"batch_id": target_batch_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.merge_batches", payload=payload, create=create)

    # ------------------------------------------------------------------
    # 申报版本（敏感内容按职责开放）
    # ------------------------------------------------------------------

    def submit_declaration(self, *, request_id: str, actor_id: str, waybill_id: str,
                           data: dict[str, Any]) -> WriteReceipt:
        """提交新的申报版本；补件就是在补件要求后提交更高版本。"""

        if not isinstance(data, dict) or not data:
            raise ValidationError("data 必须是非空对象")
        payload = {"actor_id": actor_id, "waybill_id": waybill_id, "data": data}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _DECLARANT_ROLES)
            self._waybill(connection, waybill_id)
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                row = connection.execute(
                    "SELECT COALESCE(MAX(version),0) AS version FROM air_declarations WHERE waybill_id=?",
                    (waybill_id,),
                ).fetchone()
                version = int(row["version"]) + 1
                declaration_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO air_declarations(declaration_id,waybill_id,version,status,payload_json,"
                    "payload_hash,submitted_by,submitted_at) VALUES(?,?,?,'submitted',?,?,?,?)",
                    (declaration_id, waybill_id, version, canonical_json(data),
                     digest(data), actor_id, now),
                )
                append_event(connection, actor_id=actor_id, action="air.declaration.submitted",
                             resource_type="declaration", resource_id=declaration_id,
                             detail={"waybill_id": waybill_id, "version": version,
                                     "payload_hash": digest(data)},
                             occurred_at=now)
                return "declaration", declaration_id, {"declaration_id": declaration_id, "version": version}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.submit_declaration", payload=payload, create=create)

    def list_declarations(self, *, actor_id: str, waybill_id: str) -> list[dict[str, Any]]:
        """敏感申报内容只对海关与管理角色开放，其余角色只能看到元数据。"""

        connection = self.database.connection
        actor = self._actor(connection, actor_id)
        self._waybill(connection, waybill_id)
        allowed = actor.role in _DECLARATION_READERS
        items = []
        for row in connection.execute(
                "SELECT * FROM air_declarations WHERE waybill_id=? ORDER BY version", (waybill_id,)):
            items.append({
                "declaration_id": row["declaration_id"],
                "waybill_id": row["waybill_id"],
                "version": row["version"],
                "status": row["status"],
                "submitted_by": row["submitted_by"],
                "submitted_at": row["submitted_at"],
                "payload": json.loads(row["payload_json"]) if allowed else None,
                "restricted": not allowed,
            })
        return items

    # ------------------------------------------------------------------
    # 监管决定（只覆盖显式单元；迟到决定只调整未完成路径）
    # ------------------------------------------------------------------

    def record_decision(self, *, request_id: str, actor_id: str, waybill_id: str,
                        decision_type: str, package_ids: list[str], effective_at: str,
                        note: str | None = None) -> WriteReceipt:
        if decision_type not in DECISION_TYPES:
            raise ValidationError("decision_type 不在允许范围内")
        if not isinstance(package_ids, list) or not package_ids:
            raise ValidationError("package_ids 必须是非空数组")
        package_ids = sorted({self._identifier(pid, "package_id") for pid in package_ids})
        payload = {"actor_id": actor_id, "waybill_id": waybill_id, "decision_type": decision_type,
                   "package_ids": package_ids, "effective_at": effective_at, "note": note}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _CUSTOMS_ROLES)
            waybill = self._waybill(connection, waybill_id)
            if waybill["status"] != "arrived":
                raise ConflictError("运单尚未到港，监管决定无处作用")
            effective = _parse_time(effective_at, "effective_at")
            issued = self._now()
            late = effective < issued
            now = _format_time(issued)
            packages = {}
            for package_id in package_ids:
                row = connection.execute(
                    "SELECT * FROM air_packages WHERE package_id=?", (package_id,)).fetchone()
                if row is None:
                    raise NotFoundError(f"包裹不存在: {package_id}")
                if row["waybill_id"] != waybill_id:
                    raise ValidationError(f"包裹不属于该运单: {package_id}")
                packages[package_id] = row

            def create() -> tuple[str, str, dict[str, Any]]:
                decision_id = uuid.uuid4().hex
                effect = self._new_effect(late)
                if decision_type in RESTRICTIVE_DECISIONS:
                    self._apply_restriction(connection, waybill_id, decision_type,
                                            packages, effect, now)
                else:
                    self._apply_release(connection, waybill_id, packages, effect, now)
                if decision_type == "supplement":
                    self._flag_latest_declaration(connection, waybill_id, "supplement_requested")
                if decision_type == "release":
                    self._flag_latest_declaration(connection, waybill_id, "cleared")
                effect["preserved_facts"] = self._facts_after(connection, waybill_id, effective)
                connection.execute(
                    "INSERT INTO air_decisions(decision_id,waybill_id,decision_type,effective_at,"
                    "issued_by,issued_at,note,effect_json) VALUES(?,?,?,?,?,?,?,?)",
                    (decision_id, waybill_id, decision_type, _format_time(effective),
                     actor_id, now, note, canonical_json(effect)),
                )
                for package_id in package_ids:
                    connection.execute(
                        "INSERT INTO air_decision_units(decision_id,package_id) VALUES(?,?)",
                        (decision_id, package_id),
                    )
                append_event(connection, actor_id=actor_id, action="air.decision.recorded",
                             resource_type="decision", resource_id=decision_id,
                             detail={"waybill_id": waybill_id, "decision_type": decision_type,
                                     "package_count": len(package_ids), "effective_at": _format_time(effective),
                                     "late": late, "adjusted_batches": effect["adjusted_batches"],
                                     "preserved_facts": len(effect["preserved_facts"])},
                             occurred_at=now)
                return "decision", decision_id, {"decision_id": decision_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.record_decision", payload=payload, create=create)

    def _apply_restriction(self, connection, waybill_id: str, decision_type: str,
                           packages: dict[str, sqlite3.Row], effect: dict[str, Any], now: str) -> None:
        """扣留/查验/补件/退运：只把显式覆盖的包裹移入监管批次。"""

        kind = "return" if decision_type == "return" else "hold"
        step_type = "return" if decision_type == "return" else "inspect"
        groups: dict[str, list[str]] = {}
        for package_id, row in packages.items():
            if row["batch_id"] is None:
                raise ConflictError("包裹尚未进入批次，监管决定无处作用")
            groups.setdefault(row["batch_id"], []).append(package_id)
        for batch_id in sorted(groups):
            covered = sorted(groups[batch_id])
            batch = self._batch(connection, batch_id)
            if batch["status"] == "held":
                if batch["kind"] == kind:
                    continue
                self._cancel_pending_steps(connection, batch_id, effect)
                connection.execute("UPDATE air_batches SET kind=? WHERE batch_id=?", (kind, batch_id))
                self._add_step(connection, batch_id, step_type, None, now)
                effect["adjusted_batches"].append(batch_id)
                continue
            if batch["status"] != "open":
                raise ConflictError(f"批次状态为 {batch['status']}，监管决定无法作用")
            remaining = {row["package_id"] for row in connection.execute(
                "SELECT package_id FROM air_packages WHERE batch_id=?", (batch_id,))}
            if set(covered) == remaining:
                self._release_lease(connection, batch_id, now, effect)
                self._release_booking(connection, batch_id, effect)
                self._cancel_pending_steps(connection, batch_id, effect)
                connection.execute(
                    "UPDATE air_batches SET kind=?, status='held', location='customs' WHERE batch_id=?",
                    (kind, batch_id),
                )
                self._add_step(connection, batch_id, step_type, None, now)
                effect["adjusted_batches"].append(batch_id)
            else:
                hold_batch_id = self._create_batch(connection, waybill_id, kind, "held", "customs", now)
                self._move_packages(connection, covered, hold_batch_id)
                self._adjust_booking(connection, batch_id)
                self._add_step(connection, hold_batch_id, step_type, None, now)
                effect["adjusted_batches"].append(hold_batch_id)

    def _apply_release(self, connection, waybill_id: str, packages: dict[str, sqlite3.Row],
                       effect: dict[str, Any], now: str) -> None:
        """放行：只恢复显式覆盖的包裹，其余包裹维持原状态。"""

        groups: dict[str, list[str]] = {}
        for package_id, row in packages.items():
            if row["batch_id"] is None:
                raise ConflictError("包裹尚未进入批次，无法放行")
            groups.setdefault(row["batch_id"], []).append(package_id)
        for batch_id in sorted(groups):
            covered = sorted(groups[batch_id])
            batch = self._batch(connection, batch_id)
            if batch["status"] == "open":
                continue
            if batch["status"] != "held" or batch["kind"] != "hold":
                raise ConflictError("只有被扣留的批次可以放行")
            remaining = {row["package_id"] for row in connection.execute(
                "SELECT package_id FROM air_packages WHERE batch_id=?", (batch_id,))}
            if set(covered) == remaining:
                self._cancel_pending_steps(connection, batch_id, effect)
                connection.execute(
                    "UPDATE air_batches SET status='open', location='sorting' WHERE batch_id=?", (batch_id,)
                )
                self._resume_steps(connection, batch_id, now)
                effect["resumed_batches"].append(batch_id)
            else:
                resumed_id = self._create_batch(connection, waybill_id, "split", "open", "sorting", now)
                self._move_packages(connection, covered, resumed_id)
                self._resume_steps(connection, resumed_id, now)
                effect["resumed_batches"].append(resumed_id)

    def _flag_latest_declaration(self, connection, waybill_id: str, status: str) -> None:
        row = connection.execute(
            "SELECT declaration_id FROM air_declarations WHERE waybill_id=? ORDER BY version DESC LIMIT 1",
            (waybill_id,),
        ).fetchone()
        if row:
            connection.execute(
                "UPDATE air_declarations SET status=? WHERE declaration_id=?", (status, row["declaration_id"])
            )

    def _facts_after(self, connection, waybill_id: str, effective: datetime) -> list[dict[str, Any]]:
        """列出决定生效时间之后才发生的交接与费用事实；它们保持原样，仅用于提示。"""

        facts: list[dict[str, Any]] = []
        for row in connection.execute(
                "SELECT h.handover_id, h.occurred_at FROM air_handovers h "
                "JOIN air_batches b ON b.batch_id=h.batch_id WHERE b.waybill_id=?", (waybill_id,)):
            if _parse_time(row["occurred_at"], "occurred_at") > effective:
                facts.append({"type": "handover", "id": row["handover_id"],
                              "occurred_at": row["occurred_at"]})
        for row in connection.execute(
                "SELECT fee_id, incurred_at FROM air_fees WHERE waybill_id=?", (waybill_id,)):
            if _parse_time(row["incurred_at"], "incurred_at") > effective:
                facts.append({"type": "fee", "id": row["fee_id"], "occurred_at": row["incurred_at"]})
        facts.sort(key=lambda item: (item["occurred_at"], item["id"]))
        return facts

    def get_decision(self, *, actor_id: str, decision_id: str) -> dict[str, Any]:
        connection = self.database.connection
        self._actor(connection, actor_id)
        row = connection.execute("SELECT * FROM air_decisions WHERE decision_id=?", (decision_id,)).fetchone()
        if row is None:
            raise NotFoundError("监管决定不存在")
        units = [r["package_id"] for r in connection.execute(
            "SELECT package_id FROM air_decision_units WHERE decision_id=? ORDER BY package_id",
            (decision_id,))]
        return {"decision_id": row["decision_id"], "waybill_id": row["waybill_id"],
                "decision_type": row["decision_type"], "effective_at": row["effective_at"],
                "issued_by": row["issued_by"], "issued_at": row["issued_at"], "note": row["note"],
                "package_ids": units, "effect": json.loads(row["effect_json"])}

    # ------------------------------------------------------------------
    # 查验与步骤推进
    # ------------------------------------------------------------------

    def _complete_business_step(self, *, request_id: str, actor_id: str, batch_id: str,
                                step_type: str, roles: set[str], action: str) -> WriteReceipt:
        payload = {"actor_id": actor_id, "batch_id": batch_id, "step_type": step_type}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, roles)
            self._batch(connection, batch_id)
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                steps = self._pending_steps(connection, batch_id, step_type)
                if not steps:
                    raise ConflictError("批次当前没有待完成的该环节")
                self._complete_step(connection, steps[0]["step_id"], now)
                append_event(connection, actor_id=actor_id, action=action,
                             resource_type="batch", resource_id=batch_id,
                             detail={"step_id": steps[0]["step_id"], "step_type": step_type},
                             occurred_at=now)
                return "step", steps[0]["step_id"], {"step_id": steps[0]["step_id"]}

            return self._idempotent(connection, request_id=request_id,
                                    action=action, payload=payload, create=create)

    def complete_sort(self, *, request_id: str, actor_id: str, batch_id: str) -> WriteReceipt:
        return self._complete_business_step(request_id=request_id, actor_id=actor_id, batch_id=batch_id,
                                            step_type="sort", roles=_SORT_ROLES, action="air.sort.completed")

    def complete_inspection(self, *, request_id: str, actor_id: str, batch_id: str) -> WriteReceipt:
        return self._complete_business_step(request_id=request_id, actor_id=actor_id, batch_id=batch_id,
                                            step_type="inspect", roles=_CUSTOMS_ROLES,
                                            action="air.inspection.completed")

    # ------------------------------------------------------------------
    # 仓位租约（并发占位互斥）
    # ------------------------------------------------------------------

    def lease_slot(self, *, request_id: str, actor_id: str, batch_id: str, slot_code: str) -> WriteReceipt:
        payload = {"actor_id": actor_id, "batch_id": batch_id, "slot_code": slot_code}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _WAREHOUSE_ROLES)
            batch = self._batch(connection, batch_id)
            if batch["status"] != "open":
                raise ConflictError("只有流转中的批次可以租用仓位")
            waybill = self._waybill(connection, batch["waybill_id"])
            site_id = waybill["site_id"]
            if connection.execute(
                    "SELECT 1 FROM air_slots WHERE site_id=? AND slot_code=?",
                    (site_id, slot_code)).fetchone() is None:
                raise NotFoundError("仓位不存在")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                lease_id = uuid.uuid4().hex
                try:
                    connection.execute(
                        "INSERT INTO air_slot_leases(lease_id,site_id,slot_code,batch_id,status,starts_at) "
                        "VALUES(?,?,?,?,'active',?)",
                        (lease_id, site_id, slot_code, batch_id, now),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ConflictError("仓位已被占用或批次已有有效租约") from exc
                steps = self._pending_steps(connection, batch_id, "store")
                if steps:
                    self._complete_step(connection, steps[0]["step_id"], now)
                connection.execute(
                    "UPDATE air_batches SET location='warehouse' WHERE batch_id=?", (batch_id,)
                )
                fee_id = self._record_fee(connection, waybill_id=batch["waybill_id"], batch_id=batch_id,
                                          segment_id=None, category="slot_lease",
                                          amount=SLOT_LEASE_FLAT_FEE, now=now,
                                          note=f"仓位 {slot_code}")
                append_event(connection, actor_id=actor_id, action="air.slot.leased",
                             resource_type="lease", resource_id=lease_id,
                             detail={"batch_id": batch_id, "slot_code": slot_code, "fee_id": fee_id},
                             occurred_at=now)
                return "lease", lease_id, {"lease_id": lease_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.lease_slot", payload=payload, create=create)

    def release_slot(self, *, request_id: str, actor_id: str, lease_id: str) -> WriteReceipt:
        payload = {"actor_id": actor_id, "lease_id": lease_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _WAREHOUSE_ROLES)
            row = connection.execute("SELECT * FROM air_slot_leases WHERE lease_id=?", (lease_id,)).fetchone()
            if row is None:
                raise NotFoundError("租约不存在")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                if row["status"] != "active":
                    raise ConflictError("租约已经结束")
                connection.execute(
                    "UPDATE air_slot_leases SET status='released', ends_at=? WHERE lease_id=?",
                    (now, lease_id),
                )
                batch = self._batch(connection, row["batch_id"])
                if batch["location"] == "warehouse":
                    connection.execute(
                        "UPDATE air_batches SET location='sorting' WHERE batch_id=?", (batch["batch_id"],)
                    )
                append_event(connection, actor_id=actor_id, action="air.slot.released",
                             resource_type="lease", resource_id=lease_id,
                             detail={"batch_id": row["batch_id"], "slot_code": row["slot_code"]},
                             occurred_at=now)
                return "lease", lease_id, {"lease_id": lease_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.release_slot", payload=payload, create=create)

    # ------------------------------------------------------------------
    # 航段占位与改签
    # ------------------------------------------------------------------

    def _segment_usage(self, connection, segment_id: str) -> tuple[int, float]:
        row = connection.execute(
            "SELECT COALESCE(SUM(pieces),0) AS pieces, COALESCE(SUM(weight),0) AS weight "
            "FROM air_bookings WHERE segment_id=? AND status='active'",
            (segment_id,),
        ).fetchone()
        return int(row["pieces"]), float(row["weight"])

    def book_segment(self, *, request_id: str, actor_id: str, batch_id: str, segment_id: str) -> WriteReceipt:
        payload = {"actor_id": actor_id, "batch_id": batch_id, "segment_id": segment_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _BOOK_ROLES)
            batch = self._batch(connection, batch_id)
            if batch["status"] != "open":
                raise ConflictError("只有流转中的批次可以占位")
            segment = self._segment(connection, segment_id)
            waybill = self._waybill(connection, batch["waybill_id"])
            if segment["site_id"] != waybill["site_id"]:
                raise ValidationError("航段不属于该枢纽")
            if segment["status"] not in ("scheduled", "delayed"):
                raise ConflictError("航段当前不可占位")
            existing = self._active_booking(connection, batch_id)
            if existing:
                if existing["segment_id"] == segment_id:
                    return WriteReceipt(request_id, "booking", existing["booking_id"], True)
                raise ConflictError("批次已有其他航段占位，请使用改签")
            pieces, weight = self._batch_totals(connection, batch_id)
            used_pieces, used_weight = self._segment_usage(connection, segment_id)
            if used_pieces + pieces > segment["capacity_pieces"] or \
                    used_weight + weight > segment["capacity_weight"] + 1e-9:
                raise ConflictError("航段容量不足")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                steps = self._pending_steps(connection, batch_id, "load")
                if not steps:
                    raise ConflictError("批次当前没有待装载环节")
                booking_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO air_bookings(booking_id,segment_id,batch_id,pieces,weight,status,created_at) "
                    "VALUES(?,?,?,?,?,'active',?)",
                    (booking_id, segment_id, batch_id, pieces, weight, now),
                )
                connection.execute(
                    "UPDATE air_steps SET segment_id=? WHERE step_id=?", (segment_id, steps[0]["step_id"])
                )
                fee_id = self._record_fee(connection, waybill_id=batch["waybill_id"], batch_id=batch_id,
                                          segment_id=segment_id, category="booking",
                                          amount=weight * BOOKING_RATE_PER_KG, now=now,
                                          note=f"航段 {segment_id} 占位")
                append_event(connection, actor_id=actor_id, action="air.segment.booked",
                             resource_type="booking", resource_id=booking_id,
                             detail={"batch_id": batch_id, "segment_id": segment_id,
                                     "pieces": pieces, "weight": weight, "fee_id": fee_id},
                             occurred_at=now)
                return "booking", booking_id, {"booking_id": booking_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.book_segment", payload=payload, create=create)

    def rebook_segment(self, *, request_id: str, actor_id: str, batch_id: str,
                       new_segment_id: str, reason: str | None = None) -> WriteReceipt:
        payload = {"actor_id": actor_id, "batch_id": batch_id,
                   "new_segment_id": new_segment_id, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _BOOK_ROLES)
            batch = self._batch(connection, batch_id)
            if batch["status"] != "open":
                raise ConflictError("只有流转中的批次可以改签")
            booking = self._active_booking(connection, batch_id)
            if booking is None:
                raise ConflictError("批次没有可改签的占位")
            if booking["segment_id"] == new_segment_id:
                raise ValidationError("新航段与当前占位相同")
            old_segment = self._segment(connection, booking["segment_id"])
            segment = self._segment(connection, new_segment_id)
            if segment["site_id"] != old_segment["site_id"]:
                raise ValidationError("新航段不属于该枢纽")
            if segment["status"] not in ("scheduled", "delayed"):
                raise ConflictError("新航段当前不可占位")
            pieces, weight = self._batch_totals(connection, batch_id)
            used_pieces, used_weight = self._segment_usage(connection, new_segment_id)
            if used_pieces + pieces > segment["capacity_pieces"] or \
                    used_weight + weight > segment["capacity_weight"] + 1e-9:
                raise ConflictError("新航段容量不足")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                connection.execute(
                    "UPDATE air_bookings SET status='released' WHERE booking_id=?", (booking["booking_id"],)
                )
                booking_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO air_bookings(booking_id,segment_id,batch_id,pieces,weight,status,created_at) "
                    "VALUES(?,?,?,?,?,'active',?)",
                    (booking_id, new_segment_id, batch_id, pieces, weight, now),
                )
                effect = self._new_effect(late=False)
                self._cancel_pending_steps(connection, batch_id, effect, step_type="load")
                self._add_step(connection, batch_id, "load", new_segment_id, now)
                fee_id = self._record_fee(connection, waybill_id=batch["waybill_id"], batch_id=batch_id,
                                          segment_id=new_segment_id, category="rebooking",
                                          amount=REBOOK_FLAT_FEE, now=now,
                                          note=f"由 {booking['segment_id']} 改签")
                append_event(connection, actor_id=actor_id, action="air.segment.rebooked",
                             resource_type="booking", resource_id=booking_id,
                             detail={"batch_id": batch_id, "from_segment_id": booking["segment_id"],
                                     "to_segment_id": new_segment_id, "reason": reason, "fee_id": fee_id},
                             occurred_at=now)
                return "booking", booking_id, {"booking_id": booking_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.rebook_segment", payload=payload, create=create)

    # ------------------------------------------------------------------
    # 责任交接、航段状态与退运
    # ------------------------------------------------------------------

    def record_handover(self, *, request_id: str, actor_id: str, batch_id: str, to_role: str) -> WriteReceipt:
        payload = {"actor_id": actor_id, "batch_id": batch_id, "to_role": to_role}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            batch = self._batch(connection, batch_id)
            if batch["status"] != "open":
                raise ConflictError("只有流转中的批次可以交接")
            if to_role not in PARTY_ROLES:
                raise ValidationError("to_role 不在允许范围内")
            custodian = self._custodian(connection, batch_id, batch)
            if to_role == custodian:
                raise ValidationError("接收方与当前责任方相同")
            if actor.role not in {custodian, to_role, "desk", "admin"}:
                raise PermissionDenied("只有交接双方或异常席可以登记交接")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                steps = self._pending_steps(connection, batch_id, "handover")
                if not steps:
                    raise ConflictError("批次当前没有待交接环节")
                self._complete_step(connection, steps[0]["step_id"], now)
                handover_id = self._record_handover(connection, batch_id=batch_id, from_role=custodian,
                                                    to_role=to_role, now=now, actor_id=actor_id)
                connection.execute(
                    "UPDATE air_batches SET location=? WHERE batch_id=?",
                    (LOCATION_BY_ROLE[to_role], batch_id),
                )
                append_event(connection, actor_id=actor_id, action="air.handover.recorded",
                             resource_type="handover", resource_id=handover_id,
                             detail={"batch_id": batch_id, "from_role": custodian, "to_role": to_role},
                             occurred_at=now)
                return "handover", handover_id, {"handover_id": handover_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.record_handover", payload=payload, create=create)

    def update_segment_status(self, *, request_id: str, actor_id: str, segment_id: str, status: str,
                              new_departs_at: str | None = None,
                              new_arrives_at: str | None = None) -> WriteReceipt:
        if status not in ("delayed", "cancelled", "departed"):
            raise ValidationError("status 只允许 delayed/cancelled/departed")
        payload = {"actor_id": actor_id, "segment_id": segment_id, "status": status,
                   "new_departs_at": new_departs_at, "new_arrives_at": new_arrives_at}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _SEGMENT_ROLES)
            segment = self._segment(connection, segment_id)
            if segment["status"] in ("cancelled", "departed"):
                raise ConflictError("航段已经终态，不能再次变更")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                if status == "delayed":
                    if not new_departs_at or not new_arrives_at:
                        raise ValidationError("延误必须提供新的起降时间")
                    departs = _parse_time(new_departs_at, "new_departs_at")
                    arrives = _parse_time(new_arrives_at, "new_arrives_at")
                    if arrives <= departs:
                        raise ValidationError("arrives_at 必须晚于 departs_at")
                    delay = max(0, int((departs - _parse_time(segment["departs_at"], "departs_at"))
                                       .total_seconds() // 60))
                    connection.execute(
                        "UPDATE air_segments SET status='delayed', departs_at=?, arrives_at=?, "
                        "delay_minutes=?, version=version+1 WHERE segment_id=?",
                        (_format_time(departs), _format_time(arrives), delay, segment_id),
                    )
                elif status == "cancelled":
                    connection.execute(
                        "UPDATE air_segments SET status='cancelled', version=version+1 WHERE segment_id=?",
                        (segment_id,),
                    )
                    for booking in connection.execute(
                            "SELECT * FROM air_bookings WHERE segment_id=? AND status='active'",
                            (segment_id,)).fetchall():
                        connection.execute(
                            "UPDATE air_bookings SET status='released' WHERE booking_id=?",
                            (booking["booking_id"],),
                        )
                        connection.execute(
                            "UPDATE air_steps SET status='cancelled' WHERE batch_id=? AND status='pending' "
                            "AND step_type='load' AND segment_id=?",
                            (booking["batch_id"], segment_id),
                        )
                        # 航段取消后批次需要重新占位，补上新的待装载环节
                        self._add_step(connection, booking["batch_id"], "load", None, now)
                else:
                    booked = connection.execute(
                        "SELECT * FROM air_bookings WHERE segment_id=? AND status='active'",
                        (segment_id,)).fetchall()
                    blocking = []
                    for booking in booked:
                        if self._pending_steps(connection, booking["batch_id"], "handover"):
                            blocking.append(booking["batch_id"])
                    if blocking:
                        raise ConflictError(f"批次尚未完成交接，不能出港: {sorted(blocking)}")
                    connection.execute(
                        "UPDATE air_segments SET status='departed', version=version+1 WHERE segment_id=?",
                        (segment_id,),
                    )
                    for booking in booked:
                        connection.execute(
                            "UPDATE air_bookings SET status='completed' WHERE booking_id=?",
                            (booking["booking_id"],),
                        )
                        connection.execute(
                            "UPDATE air_batches SET status='departed', location='in_transit' WHERE batch_id=?",
                            (booking["batch_id"],),
                        )
                        for step_type in ("load", "depart"):
                            for step in self._pending_steps(connection, booking["batch_id"], step_type):
                                self._complete_step(connection, step["step_id"], now)
                        for step in self._pending_steps(connection, booking["batch_id"]):
                            connection.execute(
                                "UPDATE air_steps SET status='cancelled' WHERE step_id=?",
                                (step["step_id"],),
                            )
                append_event(connection, actor_id=actor_id, action="air.segment.status",
                             resource_type="segment", resource_id=segment_id,
                             detail={"status": status}, occurred_at=now)
                return "segment", segment_id, {"segment_id": segment_id, "status": status}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.update_segment_status", payload=payload, create=create)

    def execute_return(self, *, request_id: str, actor_id: str, batch_id: str) -> WriteReceipt:
        payload = {"actor_id": actor_id, "batch_id": batch_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, _RETURN_ROLES)
            batch = self._batch(connection, batch_id)
            if batch["kind"] != "return" or batch["status"] != "held":
                raise ConflictError("只有待退运的批次可以执行退运")
            now = self._now_text()

            def create() -> tuple[str, str, dict[str, Any]]:
                steps = self._pending_steps(connection, batch_id, "return")
                if not steps:
                    raise ConflictError("批次当前没有待退运环节")
                self._complete_step(connection, steps[0]["step_id"], now)
                connection.execute(
                    "UPDATE air_batches SET status='returned', location='returned' WHERE batch_id=?",
                    (batch_id,),
                )
                fee_id = self._record_fee(connection, waybill_id=batch["waybill_id"], batch_id=batch_id,
                                          segment_id=None, category="return",
                                          amount=RETURN_FLAT_FEE, now=now, note="退运处理")
                append_event(connection, actor_id=actor_id, action="air.batch.returned",
                             resource_type="batch", resource_id=batch_id,
                             detail={"fee_id": fee_id}, occurred_at=now)
                return "batch", batch_id, {"batch_id": batch_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="air.execute_return", payload=payload, create=create)

    # ------------------------------------------------------------------
    # 异常席看板与链路查询（系统恢复后依旧准确）
    # ------------------------------------------------------------------

    def desk_board(self, *, actor_id: str, site_id: str) -> dict[str, Any]:
        connection = self.database.connection
        self._actor(connection, actor_id)
        if connection.execute("SELECT 1 FROM sites WHERE site_id=?", (site_id,)).fetchone() is None:
            raise NotFoundError("场所不存在")
        waybill_ids = [row["waybill_id"] for row in connection.execute(
            "SELECT waybill_id FROM air_waybills WHERE site_id=?", (site_id,))]

        in_transit = []
        todos: list[dict[str, Any]] = []
        for waybill_id in waybill_ids:
            batches = connection.execute(
                "SELECT * FROM air_batches WHERE waybill_id=? AND status IN ('open','held','departed') "
                "ORDER BY created_at, batch_id", (waybill_id,)).fetchall()
            for batch in batches:
                pieces, weight = self._batch_totals(connection, batch["batch_id"])
                lease = self._active_lease(connection, batch["batch_id"])
                booking = self._active_booking(connection, batch["batch_id"])
                pending = [step["step_type"] for step in self._pending_steps(connection, batch["batch_id"])]
                in_transit.append({
                    "batch_id": batch["batch_id"], "waybill_id": waybill_id, "kind": batch["kind"],
                    "status": batch["status"], "location": batch["location"],
                    "pieces": pieces, "weight": weight,
                    "custodian": self._custodian(connection, batch["batch_id"]),
                    "slot_code": lease["slot_code"] if lease else None,
                    "segment_id": booking["segment_id"] if booking else None,
                    "pending_steps": pending,
                })
                if batch["status"] == "held" and batch["kind"] == "hold" and "inspect" in pending:
                    todos.append({"kind": "inspection_due", "sort_time": batch["created_at"],
                                  "batch_id": batch["batch_id"],
                                  "summary": f"批次 {batch['batch_id']} 等待查验"})
                if batch["status"] == "held" and batch["kind"] == "return":
                    todos.append({"kind": "return_pending", "sort_time": batch["created_at"],
                                  "batch_id": batch["batch_id"],
                                  "summary": f"批次 {batch['batch_id']} 等待退运"})
                if batch["status"] == "open":
                    if booking:
                        segment = self._segment(connection, booking["segment_id"])
                        if segment["status"] == "delayed":
                            todos.append({"kind": "rebook_advised", "sort_time": segment["departs_at"],
                                          "batch_id": batch["batch_id"], "segment_id": segment["segment_id"],
                                          "summary": f"批次 {batch['batch_id']} 所在航段延误，建议改签"})
                        if "handover" in pending:
                            todos.append({"kind": "handover_due", "sort_time": segment["departs_at"],
                                          "batch_id": batch["batch_id"],
                                          "summary": f"批次 {batch['batch_id']} 出港前待交接"})
                    elif "load" in pending:
                        todos.append({"kind": "booking_needed", "sort_time": batch["created_at"],
                                      "batch_id": batch["batch_id"],
                                      "summary": f"批次 {batch['batch_id']} 等待航段占位"})
        for row in connection.execute(
                "SELECT d.declaration_id, d.waybill_id, d.submitted_at FROM air_declarations d "
                "JOIN air_waybills w ON w.waybill_id=d.waybill_id "
                "WHERE w.site_id=? AND d.status='supplement_requested' AND d.version=("
                "SELECT MAX(version) FROM air_declarations WHERE waybill_id=d.waybill_id)", (site_id,)):
            todos.append({"kind": "supplement_due", "sort_time": row["submitted_at"],
                          "declaration_id": row["declaration_id"], "waybill_id": row["waybill_id"],
                          "summary": f"运单 {row['waybill_id']} 的申报等待补件"})
        todos.sort(key=lambda item: (_parse_time(item["sort_time"], "sort_time"),
                                     _TODO_PRIORITY.get(item["kind"], 9), item["kind"]))

        segments = []
        for segment in connection.execute(
                "SELECT * FROM air_segments WHERE site_id=? AND status IN ('scheduled','delayed') "
                "ORDER BY departs_at", (site_id,)):
            used_pieces, used_weight = self._segment_usage(connection, segment["segment_id"])
            segments.append({"segment_id": segment["segment_id"], "flight_no": segment["flight_no"],
                             "status": segment["status"], "departs_at": segment["departs_at"],
                             "booked_pieces": used_pieces, "capacity_pieces": segment["capacity_pieces"],
                             "booked_weight": used_weight, "capacity_weight": segment["capacity_weight"]})
        slots_total = connection.execute(
            "SELECT COUNT(*) AS count FROM air_slots WHERE site_id=?", (site_id,)).fetchone()["count"]
        active_leases = [{"lease_id": row["lease_id"], "slot_code": row["slot_code"],
                          "batch_id": row["batch_id"], "starts_at": row["starts_at"]}
                         for row in connection.execute(
                             "SELECT * FROM air_slot_leases WHERE site_id=? AND status='active' "
                             "ORDER BY slot_code", (site_id,))]
        return {"site_id": site_id, "generated_at": self._now_text(), "in_transit": in_transit,
                "locked_capacity": {"segments": segments,
                                    "slots": {"total": slots_total, "active": active_leases}},
                "todos": todos}

    def waybill_detail(self, *, actor_id: str, waybill_id: str) -> dict[str, Any]:
        connection = self.database.connection
        actor = self._actor(connection, actor_id)
        waybill = self._waybill(connection, waybill_id)
        packages = [{"package_id": row["package_id"], "batch_id": row["batch_id"],
                     "pieces": row["pieces"], "weight": row["weight"]}
                    for row in connection.execute(
                        "SELECT * FROM air_packages WHERE waybill_id=? ORDER BY package_id", (waybill_id,))]
        batches = []
        for batch in connection.execute(
                "SELECT * FROM air_batches WHERE waybill_id=? ORDER BY created_at, batch_id", (waybill_id,)):
            pieces, weight = self._batch_totals(connection, batch["batch_id"])
            steps = [{"step_id": step["step_id"], "step_type": step["step_type"],
                      "segment_id": step["segment_id"], "status": step["status"],
                      "occurred_at": step["occurred_at"]}
                     for step in connection.execute(
                         "SELECT * FROM air_steps WHERE batch_id=? ORDER BY position", (batch["batch_id"],))]
            lease = self._active_lease(connection, batch["batch_id"])
            booking = self._active_booking(connection, batch["batch_id"])
            batches.append({"batch_id": batch["batch_id"], "kind": batch["kind"],
                            "status": batch["status"], "location": batch["location"],
                            "pieces": pieces, "weight": weight, "steps": steps,
                            "active_lease": dict(lease) if lease else None,
                            "active_booking": dict(booking) if booking else None})
        handovers = [dict(row) for row in connection.execute(
            "SELECT h.* FROM air_handovers h JOIN air_batches b ON b.batch_id=h.batch_id "
            "WHERE b.waybill_id=? ORDER BY h.occurred_at, h.rowid", (waybill_id,))]
        decisions = [{"decision_id": row["decision_id"], "decision_type": row["decision_type"],
                      "effective_at": row["effective_at"], "issued_at": row["issued_at"],
                      "package_ids": [u["package_id"] for u in connection.execute(
                          "SELECT package_id FROM air_decision_units WHERE decision_id=? "
                          "ORDER BY package_id", (row["decision_id"],))]}
                     for row in connection.execute(
                         "SELECT * FROM air_decisions WHERE waybill_id=? ORDER BY issued_at, decision_id",
                         (waybill_id,))]
        fees = [dict(row) for row in connection.execute(
            "SELECT * FROM air_fees WHERE waybill_id=? ORDER BY incurred_at, fee_id", (waybill_id,))]
        scanned = sum(item["pieces"] for item in packages)
        assigned = sum(item["pieces"] for item in packages if item["batch_id"])
        declarations = self.list_declarations(actor_id=actor_id, waybill_id=waybill_id)
        return {"waybill": dict(waybill), "packages": packages, "batches": batches,
                "handovers": handovers, "decisions": decisions, "fees": fees,
                "declarations": declarations,
                "conservation": {"declared_pieces": waybill["declared_pieces"],
                                 "scanned_pieces": scanned, "assigned_pieces": assigned,
                                 "conserved": scanned == waybill["declared_pieces"]}}

    # ------------------------------------------------------------------
    # 延误影响与恢复路线（不扩大冻结范围）
    # ------------------------------------------------------------------

    def delay_impact(self, *, actor_id: str, segment_id: str,
                     delay_minutes: int | None = None) -> dict[str, Any]:
        connection = self.database.connection
        self._actor(connection, actor_id)
        segment = self._segment(connection, segment_id)
        delay = int(delay_minutes) if delay_minutes is not None else int(segment["delay_minutes"])
        if delay < 0:
            raise ValidationError("delay_minutes 不能为负数")
        projected_arrival = _parse_time(segment["arrives_at"], "arrives_at") + timedelta(minutes=delay)

        bookings = connection.execute(
            "SELECT * FROM air_bookings WHERE segment_id=? AND status='active'", (segment_id,)).fetchall()
        affected_batches = []
        affected_packages = []
        waybill_ids: list[str] = []
        for booking in bookings:
            batch = self._batch(connection, booking["batch_id"])
            if batch["status"] != "open":
                continue
            pieces, weight = self._batch_totals(connection, batch["batch_id"])
            affected_batches.append({"batch_id": batch["batch_id"], "waybill_id": batch["waybill_id"],
                                     "pieces": pieces, "weight": weight})
            if batch["waybill_id"] not in waybill_ids:
                waybill_ids.append(batch["waybill_id"])
            for row in connection.execute(
                    "SELECT package_id FROM air_packages WHERE batch_id=? ORDER BY package_id",
                    (batch["batch_id"],)):
                affected_packages.append({"package_id": row["package_id"], "batch_id": batch["batch_id"],
                                          "waybill_id": batch["waybill_id"], "held": False})

        frozen_remainder = []
        for waybill_id in waybill_ids:
            for row in connection.execute(
                    "SELECT p.package_id, p.batch_id FROM air_packages p JOIN air_batches b "
                    "ON b.batch_id=p.batch_id WHERE b.waybill_id=? AND b.status='held' "
                    "ORDER BY p.package_id", (waybill_id,)):
                frozen_remainder.append({"package_id": row["package_id"], "batch_id": row["batch_id"],
                                         "waybill_id": waybill_id, "held": True})

        batch_ids = [item["batch_id"] for item in affected_batches]
        recorded_fees = []
        if waybill_ids:
            marks = ",".join("?" for _ in waybill_ids)
            for row in connection.execute(
                    f"SELECT * FROM air_fees WHERE waybill_id IN ({marks}) "
                    "ORDER BY incurred_at, fee_id", waybill_ids):
                if row["segment_id"] == segment_id or row["batch_id"] in batch_ids:
                    recorded_fees.append(dict(row))

        commitments = []
        for waybill_id in waybill_ids:
            waybill = self._waybill(connection, waybill_id)
            if waybill["promised_arrival_at"]:
                promised = _parse_time(waybill["promised_arrival_at"], "promised_arrival_at")
                commitments.append({"waybill_id": waybill_id,
                                    "promised_arrival_at": waybill["promised_arrival_at"],
                                    "projected_arrival_at": _format_time(projected_arrival),
                                    "at_risk": projected_arrival > promised})

        total_weight = sum(item["weight"] for item in affected_batches)
        projected_fees = {"rebooking": round(REBOOK_FLAT_FEE * len(affected_batches), 2),
                          "extra_storage": round(total_weight * STORAGE_RATE_PER_KG_DAY * delay / 1440, 2),
                          "currency": FEE_CURRENCY}

        proposals = []
        unscheduled = []
        considered: list[str] = []
        now = self._now()
        for item in affected_batches:
            candidates = []
            for row in connection.execute(
                    "SELECT * FROM air_segments WHERE site_id=? AND origin=? AND destination=? "
                    "AND status='scheduled' AND segment_id!=? ORDER BY arrives_at",
                    (segment["site_id"], segment["origin"], segment["destination"], segment_id)):
                if _parse_time(row["departs_at"], "departs_at") <= now:
                    continue
                used_pieces, used_weight = self._segment_usage(connection, row["segment_id"])
                if used_pieces + item["pieces"] > row["capacity_pieces"] or \
                        used_weight + item["weight"] > row["capacity_weight"] + 1e-9:
                    continue
                candidates.append(row)
            if not candidates:
                unscheduled.append(item["batch_id"])
                continue
            chosen = candidates[0]
            if chosen["segment_id"] not in considered:
                considered.append(chosen["segment_id"])
            waybill = self._waybill(connection, item["waybill_id"])
            meets = None
            if waybill["promised_arrival_at"]:
                meets = _parse_time(chosen["arrives_at"], "arrives_at") <= \
                    _parse_time(waybill["promised_arrival_at"], "promised_arrival_at")
            proposals.append({"batch_id": item["batch_id"], "from_segment_id": segment_id,
                              "to_segment_id": chosen["segment_id"], "departs_at": chosen["departs_at"],
                              "arrives_at": chosen["arrives_at"], "meets_commitment": meets})

        return {"segment": {"segment_id": segment["segment_id"], "flight_no": segment["flight_no"],
                            "origin": segment["origin"], "destination": segment["destination"],
                            "status": segment["status"], "departs_at": segment["departs_at"],
                            "arrives_at": segment["arrives_at"]},
                "delay_minutes": delay,
                "affected": {"packages": affected_packages, "batches": affected_batches,
                             "segments": [segment_id] + considered,
                             "recorded_fees": recorded_fees, "projected_fees": projected_fees,
                             "commitments": commitments},
                "recovery": {"freeze_scope_expanded": False, "proposals": proposals,
                             "unscheduled_batches": unscheduled,
                             "excluded_held_packages": frozen_remainder}}
