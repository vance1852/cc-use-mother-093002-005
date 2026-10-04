"""航空物流异常协同领域服务。

把主运单、子批次、包装单元、申报版本、仓位租约、航段容量、监管决定和
责任交接串成一条只增不改事实、状态实时推导的连续链路：

* 到港、拆分、合并、查验、补件、放行、改签、交接、退运按真实业务顺序推进；
* 监管决定只作用于显式覆盖的包装，并沿拆/合血缘传播，迟到决定按自身
  生效时间影响尚未完成的路径，已发生的交接与费用永不回写；
* 拆合前后数量按血缘树叶节点守恒；重复扫描与重复请求不会再次扣减
  库存或容量，数据库唯一约束保证同一包装不会同时出现在两个位置或两个航段；
* 敏感申报仅向具备海关职责的操作者开放。
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from .audit import append_event, canonical_json, digest
from .clock import Clock, SystemClock
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .storage import Database

DUTY_AIRLINE = "airline"
DUTY_SORTING = "sorting"
DUTY_WAREHOUSE = "warehouse"
DUTY_CUSTOMS = "customs"
DUTY_EXCEPTION = "exception"
DUTIES = frozenset({DUTY_AIRLINE, DUTY_SORTING, DUTY_WAREHOUSE, DUTY_CUSTOMS, DUTY_EXCEPTION})

DECISION_RELEASE = "release"
DECISION_HOLD = "hold"
DECISION_INSPECT = "inspect"
DECISION_REQUEST_SUPPLEMENT = "request_supplement"
DECISION_RETURN = "return"
DECISION_KINDS = frozenset({
    DECISION_RELEASE, DECISION_HOLD, DECISION_INSPECT,
    DECISION_REQUEST_SUPPLEMENT, DECISION_RETURN,
})
# 阻断后续运输路径的决定种类
BLOCKING_KINDS = (DECISION_HOLD, DECISION_RETURN, DECISION_REQUEST_SUPPLEMENT, DECISION_INSPECT)

# 已经终结、不再参与在途路径的包装阶段
FINAL_STAGES = frozenset({"returned"})
# 已离场或已退运，不允许再拆分/合并
IMMOBILE_STAGES = frozenset({"departed", "returned"})
# 不再产生待办的阶段（已退运或正在航段上）
INACTIVE_STAGES = frozenset({"departed", "returned"})


@dataclass(frozen=True)
class CargoResult:
    """描述一次幂等写操作的稳定结果。"""

    request_id: str
    resource_type: str
    resource_id: str
    replayed: bool
    data: dict[str, Any]


class CargoService:
    """实现航空、分拣、仓储与海关共用的异常协同规则。"""

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------ 基础工具

    def _now(self) -> str:
        return self.clock.now().astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _uuid() -> str:
        return uuid.uuid4().hex

    def _ts(self, value: Any, field: str, *, default_now: bool = False) -> str:
        if value is None:
            if default_now:
                return self._now()
            raise ValidationError(f"{field} 不能为空")
        if not isinstance(value, str) or not value.strip():
            raise ValidationError(f"{field} 必须是 ISO-8601 时间字符串")
        text = value.strip().replace("Z", "+00:00")
        try:
            moment = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValidationError(f"{field} 时间格式无效") from exc
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _positive_qty(self, value: Any, field: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ValidationError(f"{field} 必须是正数")
        return float(value)

    def _nonneg_weight(self, value: Any, field: str) -> float:
        if value is None:
            return 0.0
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
            raise ValidationError(f"{field} 必须是非负数")
        return float(value)

    def _actor(self, connection, actor_id: str):
        if not actor_id:
            raise PermissionDenied("缺少 X-Actor-Id")
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        if not row["active"]:
            raise PermissionDenied("操作者已停用")
        return row

    def _has_duty(self, connection, actor, shipment_id: str, duty: str) -> bool:
        if actor["role"] == "admin":
            return True
        row = connection.execute(
            "SELECT 1 FROM cargo_duties WHERE shipment_id=? AND actor_id=? AND duty=?",
            (shipment_id, actor["actor_id"], duty),
        ).fetchone()
        return row is not None

    def _require_duty(self, connection, actor, shipment_id: str, *duties: str) -> None:
        if actor["role"] == "admin":
            return
        for duty in dict.fromkeys(duties):
            if self._has_duty(connection, actor, shipment_id, duty):
                return
        raise PermissionDenied("当前操作者在该票货物上没有相应职责")

    def _require_customs_read(self, connection, actor, shipment_id: str) -> None:
        # 敏感申报的读取不接受 admin 角色旁路，必须显式具备海关职责
        row = connection.execute(
            "SELECT 1 FROM cargo_duties WHERE shipment_id=? AND actor_id=? AND duty=?",
            (shipment_id, actor["actor_id"], DUTY_CUSTOMS),
        ).fetchone()
        if row is None:
            raise PermissionDenied("敏感申报仅向具备海关职责的人员开放")

    def _mutate(
        self,
        *,
        request_id: str,
        actor_id: str,
        action: str,
        payload: dict[str, Any],
        work: Callable[[Any, Any], tuple[str, str, dict[str, Any]]],
    ) -> CargoResult:
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            payload_hash = digest(payload)
            row = connection.execute(
                "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
            ).fetchone()
            if row:
                if row["action"] != action or row["payload_hash"] != payload_hash:
                    raise ConflictError("request_id 已被不同内容使用")
                return CargoResult(request_id, row["resource_type"], row["resource_id"], True,
                                   json.loads(row["response_json"]))
            resource_type, resource_id, data = work(connection, actor)
            connection.execute(
                "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,"
                "response_json,created_at) VALUES(?,?,?,?,?,?,?)",
                (request_id, action, payload_hash, resource_type, resource_id,
                 canonical_json(data), self._now()),
            )
            return CargoResult(request_id, resource_type, resource_id, False, data)

    def _audit(self, connection, *, actor_id: str, action: str, resource_type: str,
               resource_id: str, detail: dict[str, Any], occurred_at: str | None = None) -> None:
        append_event(connection, actor_id=actor_id, action=action, resource_type=resource_type,
                     resource_id=resource_id, detail=detail, occurred_at=occurred_at or self._now())

    def _shipment(self, connection, shipment_id: str):
        row = connection.execute(
            "SELECT * FROM cargo_shipments WHERE shipment_id=?", (shipment_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("主运单不存在")
        return row

    def _packages(self, connection, shipment_id: str) -> dict[str, Any]:
        rows = connection.execute(
            "SELECT * FROM cargo_packages WHERE shipment_id=? ORDER BY seq, package_id",
            (shipment_id,),
        ).fetchall()
        return {row["package_id"]: row for row in rows}

    def _normalize_package_ids(self, connection, shipment_id: str,
                               package_ids: Iterable[str]) -> list[str]:
        if not isinstance(package_ids, (list, tuple)) or not package_ids:
            raise ValidationError("package_ids 必须是非空数组")
        result: list[str] = []
        seen: set[str] = set()
        for package_id in package_ids:
            if not isinstance(package_id, str) or not package_id.strip():
                raise ValidationError("package_id 不能为空")
            package_id = package_id.strip()
            if package_id in seen:
                continue
            seen.add(package_id)
            row = connection.execute(
                "SELECT 1 FROM cargo_packages WHERE package_id=? AND shipment_id=?",
                (package_id, shipment_id),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"包装 {package_id} 不属于该票货物或不存在")
            result.append(package_id)
        return result

    def _scan_check(self, connection, scan_token: str | None, package_ids: list[str],
                    action: str) -> dict[str, Any] | None:
        """重复扫描直接返回既有结果，绝不再次执行业务扣减。"""

        if not scan_token:
            return None
        row = connection.execute(
            "SELECT * FROM cargo_scans WHERE scan_token=?", (scan_token,)
        ).fetchone()
        if row is not None:
            if row["action"] != action:
                raise ConflictError("扫描令牌已用于其他动作")
            if row["package_id"] not in package_ids:
                raise ConflictError("扫描令牌已用于其他包装")
            return {"replayed_scan": True, "scan_token": scan_token,
                    "result_ref": row["result_ref"]}
        return None

    def _scan_record(self, connection, scan_token: str | None, representative_package: str,
                     action: str, result_ref: str) -> None:
        if scan_token:
            connection.execute(
                "INSERT INTO cargo_scans(scan_token,package_id,action,result_ref,created_at) "
                "VALUES(?,?,?,?,?)",
                (scan_token, representative_package, action, result_ref, self._now()),
            )

    def _record_fee(self, connection, *, shipment_id: str, package_ids: list[str], fee_type: str,
                    amount: float, currency: str, responsible_party: str,
                    ref_type: str | None = None, ref_id: str | None = None,
                    incurred_at: str | None = None) -> str:
        fee_id = self._uuid()
        moment = incurred_at or self._now()
        connection.execute(
            "INSERT INTO cargo_fees(fee_id,shipment_id,package_ids_json,fee_type,amount,currency,"
            "responsible_party,ref_type,ref_id,incurred_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (fee_id, shipment_id, canonical_json(package_ids), fee_type, amount, currency,
             responsible_party, ref_type, ref_id, moment, self._now()),
        )
        return fee_id

    # ------------------------------------------------------------------ 建档与授权

    def grant_duty(self, *, request_id: str, actor_id: str, shipment_id: str,
                   target_actor_id: str, duty: str) -> CargoResult:
        """把承运、分拣、仓储、海关或异常席职责授予某操作者。"""

        payload = {"actor_id": actor_id, "shipment_id": shipment_id,
                   "target_actor_id": target_actor_id, "duty": duty}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            if duty not in DUTIES:
                raise ValidationError("duty 不在允许范围内")
            target = self._actor(connection, target_actor_id)
            if actor["role"] != "admin" and not self._has_duty(connection, actor, shipment_id, DUTY_AIRLINE):
                raise PermissionDenied("只有管理员或承运人可以分配职责")
            try:
                connection.execute(
                    "INSERT INTO cargo_duties(shipment_id,actor_id,duty,granted_by,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (shipment_id, target_actor_id, duty, actor["actor_id"], self._now()),
                )
            except Exception as exc:
                raise ConflictError("该职责已经授予") from exc
            self._audit(connection, actor_id=actor["actor_id"], action="duty.granted",
                        resource_type="shipment", resource_id=shipment_id,
                        detail={"target_actor_id": target_actor_id, "duty": duty,
                                "organization_id": target["organization_id"]})
            data = {"shipment_id": shipment_id, "actor_id": target_actor_id, "duty": duty}
            return "duty", f"{shipment_id}:{target_actor_id}:{duty}", data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="grant_duty",
                            payload=payload, work=work)

    def create_shipment(self, *, request_id: str, actor_id: str, master_waybill: str,
                        origin: str, destination: str, promised_delivery_at: str | None = None,
                        carrier_actor_id: str | None = None) -> CargoResult:
        payload = {"actor_id": actor_id, "master_waybill": master_waybill, "origin": origin,
                   "destination": destination, "promised_delivery_at": promised_delivery_at,
                   "carrier_actor_id": carrier_actor_id or actor_id}

        def work(connection, actor):
            carrier_id = carrier_actor_id or actor["actor_id"]
            self._actor(connection, carrier_id)
            waybill = str(master_waybill).strip()
            if not waybill:
                raise ValidationError("master_waybill 不能为空")
            origin_t = str(origin).strip()
            destination_t = str(destination).strip()
            if not origin_t or not destination_t:
                raise ValidationError("起止港不能为空")
            promised = self._ts(promised_delivery_at, "promised_delivery_at") if promised_delivery_at else None
            shipment_id = self._uuid()
            root_batch_id = self._uuid()
            now = self._now()
            try:
                connection.execute(
                    "INSERT INTO cargo_shipments(shipment_id,master_waybill,carrier_actor_id,origin,"
                    "destination,promised_delivery_at,status,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (shipment_id, waybill, carrier_id, origin_t, destination_t, promised,
                     "open", actor["actor_id"], now),
                )
            except Exception as exc:
                raise ConflictError("主运单号已经存在") from exc
            connection.execute(
                "INSERT INTO cargo_batches(batch_id,shipment_id,parent_batch_id,status,created_by,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (root_batch_id, shipment_id, None, "active", actor["actor_id"], now),
            )
            # 建档承运人天然具备航空与异常协同职责
            connection.execute(
                "INSERT INTO cargo_duties(shipment_id,actor_id,duty,granted_by,created_at) "
                "VALUES(?,?,?,?,?)",
                (shipment_id, carrier_id, DUTY_AIRLINE, actor["actor_id"], now),
            )
            self._audit(connection, actor_id=actor["actor_id"], action="shipment.created",
                        resource_type="shipment", resource_id=shipment_id,
                        detail={"master_waybill": waybill, "origin": origin_t,
                                "destination": destination_t, "promised_delivery_at": promised,
                                "carrier_actor_id": carrier_id, "root_batch_id": root_batch_id})
            data = {"shipment_id": shipment_id, "master_waybill": waybill,
                    "root_batch_id": root_batch_id}
            return "shipment", shipment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="create_shipment",
                            payload=payload, work=work)

    def register_packages(self, *, request_id: str, actor_id: str, shipment_id: str,
                          packages: list[dict[str, Any]], batch_id: str | None = None) -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id,
                   "packages": packages, "batch_id": batch_id}

        def work(connection, actor):
            shipment = self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_AIRLINE, DUTY_SORTING)
            if not isinstance(packages, list) or not packages:
                raise ValidationError("packages 必须是非空数组")
            target_batch = batch_id or connection.execute(
                "SELECT batch_id FROM cargo_batches WHERE shipment_id=? AND parent_batch_id IS NULL",
                (shipment_id,),
            ).fetchone()["batch_id"]
            brow = connection.execute(
                "SELECT * FROM cargo_batches WHERE batch_id=? AND shipment_id=?",
                (target_batch, shipment_id),
            ).fetchone()
            if brow is None or brow["status"] != "active":
                raise ValidationError("目标批次不存在或已关闭")
            created: list[dict[str, Any]] = []
            for item in packages:
                if not isinstance(item, dict):
                    raise ValidationError("packages 中的每一项必须是对象")
                package_id = item.get("package_id") or self._uuid()
                if not isinstance(package_id, str) or not package_id.strip():
                    raise ValidationError("package_id 无效")
                quantity = self._positive_qty(item.get("quantity"), "quantity")
                weight = self._nonneg_weight(item.get("weight", 0.0), "weight")
                seq_row = connection.execute(
                    "SELECT COALESCE(MAX(seq),0)+1 AS next_seq FROM cargo_packages WHERE shipment_id=?",
                    (shipment_id,),
                ).fetchone()
                try:
                    connection.execute(
                        "INSERT INTO cargo_packages(package_id,shipment_id,batch_id,seq,quantity,weight,"
                        "stage) VALUES(?,?,?,?,?,?,'registered')",
                        (package_id, shipment_id, target_batch, seq_row["next_seq"],
                         quantity, weight),
                    )
                except Exception as exc:
                    raise ConflictError(f"包装 {package_id} 已存在") from exc
                created.append({"package_id": package_id, "quantity": quantity, "weight": weight})
            self._audit(connection, actor_id=actor["actor_id"], action="packages.registered",
                        resource_type="shipment", resource_id=shipment_id,
                        detail={"batch_id": target_batch, "count": len(created),
                                "total_quantity": sum(item["quantity"] for item in created),
                                "carrier_actor_id": shipment["carrier_actor_id"]})
            data = {"shipment_id": shipment_id, "batch_id": target_batch, "packages": created}
            return "package_batch", target_batch, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="register_packages",
                            payload=payload, work=work)

    # ------------------------------------------------------------------ 拆分与合并

    def _split_edges(self, connection, shipment_id: str) -> dict[str, set[str]]:
        """返回 子包装 -> 来源父包装 的拆分血缘边（不含合并边）。"""

        edges: dict[str, set[str]] = {}
        for row in connection.execute(
            "SELECT package_id, parent_package_id FROM cargo_packages "
            "WHERE shipment_id=? AND parent_package_id IS NOT NULL",
            (shipment_id,),
        ):
            edges.setdefault(row["package_id"], set()).add(row["parent_package_id"])
        return edges

    def _lineage_edges(self, connection, shipment_id: str) -> dict[str, set[str]]:
        """返回 子包装 -> 来源包装集合 的完整血缘边（拆分与合并统一建模）。"""

        edges = self._split_edges(connection, shipment_id)
        for row in connection.execute(
            "SELECT m.child_package_id, m.source_package_id FROM cargo_merge_sources m "
            "JOIN cargo_packages p ON p.package_id=m.child_package_id WHERE p.shipment_id=?",
            (shipment_id,),
        ):
            edges.setdefault(row["child_package_id"], set()).add(row["source_package_id"])
        return edges

    def _descendants(self, edges: dict[str, set[str]], roots: Iterable[str]) -> set[str]:
        result: set[str] = set()
        stack = list(roots)
        while stack:
            current = stack.pop()
            for child, sources in edges.items():
                if child in result:
                    continue
                if sources & {current} | (sources & result):
                    if child not in result:
                        result.add(child)
                        stack.append(child)
        return result

    def _assert_conservation(self, connection, shipment_id: str) -> None:
        """按血缘连通分量校验数量守恒：初始包装总量等于现存叶节点总量。

        拆分（一对一父）和合并（多对一父）统一用有向无环血缘图表达。
        守恒单位是无向连通分量：合并 R1、R2 -> C 后，三者属于同一分量，
        根节点（R1、R2）数量之和必须等于叶节点（C）数量。
        """

        packages = self._packages(connection, shipment_id)
        edges = self._lineage_edges(connection, shipment_id)
        # 无向邻接表
        adjacency: dict[str, set[str]] = {pid: set() for pid in packages}
        for child, sources in edges.items():
            for source in sources:
                adjacency.setdefault(child, set()).add(source)
                adjacency.setdefault(source, set()).add(child)
        children_of: dict[str, set[str]] = {}
        for child, sources in edges.items():
            for source in sources:
                children_of.setdefault(source, set()).add(child)

        visited: set[str] = set()
        for start in packages:
            if start in visited:
                continue
            component: set[str] = set()
            stack = [start]
            while stack:
                node = stack.pop()
                if node in component:
                    continue
                component.add(node)
                for neighbour in adjacency.get(node, ()):  # 无向洪泛
                    if neighbour not in component:
                        stack.append(neighbour)
            visited |= component
            roots = [node for node in component if not edges.get(node)]
            leaves = [node for node in component if not children_of.get(node)]
            original_total = sum(packages[root]["quantity"] for root in roots)
            leaf_total = sum(packages[leaf]["quantity"] for leaf in leaves)
            if abs(leaf_total - original_total) > 1e-9:
                raise ConflictError(
                    f"拆分/合并数量不守恒：原始合计 {original_total}，现存叶节点合计 {leaf_total}"
                )

    def split_packages(self, *, request_id: str, actor_id: str, shipment_id: str,
                       splits: list[dict[str, Any]], reason: str) -> CargoResult:
        """把若干包装拆入新的子批次；每个来源拆出的子包装数量必须恰好等于原数量。"""

        payload = {"actor_id": actor_id, "shipment_id": shipment_id,
                   "splits": splits, "reason": reason}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_AIRLINE, DUTY_SORTING)
            if not isinstance(splits, list) or not splits:
                raise ValidationError("splits 必须是非空数组")
            reason_t = str(reason).strip()
            if not reason_t:
                raise ValidationError("reason 不能为空")
            packages = self._packages(connection, shipment_id)
            edges = self._lineage_edges(connection, shipment_id)
            children_of: dict[str, set[str]] = {}
            for child, sources in edges.items():
                for source in sources:
                    children_of.setdefault(source, set()).add(child)

            source_rows = []
            all_children: list[dict[str, Any]] = []
            for spec in splits:
                source_id = spec.get("source_package_id")
                source = packages.get(source_id)
                if source is None:
                    raise NotFoundError(f"来源包装 {source_id} 不存在")
                if source_id in children_of:
                    raise ConflictError(f"包装 {source_id} 已拆分过，不能再次作为来源")
                if source["stage"] in IMMOBILE_STAGES:
                    raise ConflictError(f"包装 {source_id} 已离场或退运，不能拆分")
                children_spec = spec.get("children")
                if not isinstance(children_spec, list) or not children_spec:
                    raise ValidationError("每个拆分项必须提供非空 children")
                child_total = 0.0
                for child_spec in children_spec:
                    quantity = self._positive_qty(child_spec.get("quantity"), "child.quantity")
                    weight = self._nonneg_weight(child_spec.get("weight", 0.0), "child.weight")
                    child_total += quantity
                    all_children.append({"source_package_id": source_id,
                                         "package_id": child_spec.get("package_id") or self._uuid(),
                                         "quantity": quantity, "weight": weight})
                if abs(child_total - source["quantity"]) > 1e-9:
                    raise ConflictError(
                        f"包装 {source_id} 拆分后数量 {child_total} 不等于原数量 {source['quantity']}"
                    )
                source_rows.append(source)

            parent_batches = {row["batch_id"] for row in source_rows}
            if len(parent_batches) != 1:
                raise ValidationError("一次拆分只能来自同一个子批次")
            parent_batch_id = next(iter(parent_batches))
            new_batch_id = self._uuid()
            now = self._now()
            connection.execute(
                "INSERT INTO cargo_batches(batch_id,shipment_id,parent_batch_id,status,created_by,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (new_batch_id, shipment_id, parent_batch_id, "active", actor["actor_id"], now),
            )
            for source in source_rows:
                # 已在航段上占位的包装必须先释放/改签，避免拆分后容量悬挂
                active = connection.execute(
                    "SELECT 1 FROM cargo_allocations WHERE package_id=? "
                    "AND state IN ('held','confirmed')",
                    (source["package_id"],),
                ).fetchone()
                if active is not None:
                    raise ConflictError(
                        f"包装 {source['package_id']} 仍有生效航段占位，必须先释放才能拆分")
            for source in source_rows:
                connection.execute(
                    "UPDATE cargo_packages SET stage='split' WHERE package_id=?",
                    (source["package_id"],),
                )
                # 来源包装物理上已被子件取代，移除其库位占用，由子件重新上架
                connection.execute(
                    "DELETE FROM cargo_placements WHERE package_id=?",
                    (source["package_id"],),
                )
            for index, item in enumerate(all_children, start=1):
                seq_row = connection.execute(
                    "SELECT COALESCE(MAX(seq),0)+1 AS next_seq FROM cargo_packages WHERE shipment_id=?",
                    (shipment_id,),
                ).fetchone()
                source = packages[item["source_package_id"]]
                try:
                    connection.execute(
                        "INSERT INTO cargo_packages(package_id,shipment_id,batch_id,seq,quantity,weight,"
                        "stage,current_custodian,parent_package_id,arrived_at,inspected_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (item["package_id"], shipment_id, new_batch_id, seq_row["next_seq"],
                         item["quantity"], item["weight"],
                         "arrived" if source["arrived_at"] else "registered",
                         source["current_custodian"], source["package_id"],
                         source["arrived_at"], source["inspected_at"]),
                    )
                except Exception as exc:
                    raise ConflictError(f"包装 {item['package_id']} 已存在") from exc
                connection.execute(
                    "INSERT INTO cargo_batch_moves(package_id,shipment_id,from_batch_id,to_batch_id,"
                    "reason,moved_at) VALUES(?,?,?,?,?,?)",
                    (item["package_id"], shipment_id, parent_batch_id, new_batch_id,
                     f"split:{reason_t}", now),
                )
            self._assert_conservation(connection, shipment_id)
            self._audit(connection, actor_id=actor["actor_id"], action="packages.split",
                        resource_type="batch", resource_id=new_batch_id,
                        detail={"shipment_id": shipment_id, "parent_batch_id": parent_batch_id,
                                "reason": reason_t, "source_package_ids": [r["package_id"] for r in source_rows],
                                "child_package_ids": [item["package_id"] for item in all_children],
                                "child_total_quantity": sum(item["quantity"] for item in all_children)})
            data = {"shipment_id": shipment_id, "batch_id": new_batch_id,
                    "parent_batch_id": parent_batch_id,
                    "packages": [{"package_id": item["package_id"],
                                  "quantity": item["quantity"],
                                  "source_package_id": item["source_package_id"]}
                                 for item in all_children]}
            return "batch", new_batch_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="split_packages",
                            payload=payload, work=work)

    def merge_packages(self, *, request_id: str, actor_id: str, shipment_id: str,
                       source_package_ids: list[str], reason: str,
                       merged_package_id: str | None = None,
                       weight: float | None = None) -> CargoResult:
        """把多个现存叶包装合并为一个新包装，数量为各来源之和。"""

        payload = {"actor_id": actor_id, "shipment_id": shipment_id,
                   "source_package_ids": source_package_ids, "reason": reason,
                   "merged_package_id": merged_package_id, "weight": weight}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_AIRLINE, DUTY_SORTING)
            ids = self._normalize_package_ids(connection, shipment_id, source_package_ids)
            if len(ids) < 2:
                raise ValidationError("合并至少需要两个来源包装")
            reason_t = str(reason).strip()
            if not reason_t:
                raise ValidationError("reason 不能为空")
            packages = self._packages(connection, shipment_id)
            edges = self._lineage_edges(connection, shipment_id)
            children_of: dict[str, set[str]] = {}
            for child, sources in edges.items():
                for source in sources:
                    children_of.setdefault(source, set()).add(child)
            sources = [packages[pid] for pid in ids]
            for source in sources:
                if source["package_id"] in children_of:
                    raise ConflictError(f"包装 {source['package_id']} 已被拆分，不能参与合并")
                if source["stage"] in IMMOBILE_STAGES or source["stage"] == "split":
                    raise ConflictError(f"包装 {source['package_id']} 当前状态不能参与合并")
                active = connection.execute(
                    "SELECT 1 FROM cargo_allocations WHERE package_id=? "
                    "AND state IN ('held','confirmed')",
                    (source["package_id"],),
                ).fetchone()
                if active is not None:
                    raise ConflictError(
                        f"包装 {source['package_id']} 仍有生效航段占位，必须先释放才能合并")
            total_qty = sum(row["quantity"] for row in sources)
            total_weight = self._nonneg_weight(weight, "weight") if weight is not None \
                else sum(row["weight"] for row in sources)
            custodians = {row["current_custodian"] for row in sources if row["current_custodian"]}
            if len(custodians) > 1:
                raise ConflictError("来源包装分属不同保管方，须先完成责任交接再合并")
            parent_batch_id = sources[0]["batch_id"]
            new_batch_id = self._uuid()
            child_id = merged_package_id or self._uuid()
            now = self._now()
            connection.execute(
                "INSERT INTO cargo_batches(batch_id,shipment_id,parent_batch_id,status,created_by,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (new_batch_id, shipment_id, parent_batch_id, "active", actor["actor_id"], now),
            )
            seq_row = connection.execute(
                "SELECT COALESCE(MAX(seq),0)+1 AS next_seq FROM cargo_packages WHERE shipment_id=?",
                (shipment_id,),
            ).fetchone()
            first_arrived = next((row["arrived_at"] for row in sources if row["arrived_at"]), None)
            try:
                connection.execute(
                    "INSERT INTO cargo_packages(package_id,shipment_id,batch_id,seq,quantity,weight,"
                    "stage,current_custodian,parent_package_id,arrived_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (child_id, shipment_id, new_batch_id, seq_row["next_seq"], total_qty,
                     total_weight, "arrived" if first_arrived else "registered",
                     next(iter(custodians), None), None, first_arrived),
                )
            except Exception as exc:
                raise ConflictError(f"包装 {child_id} 已存在") from exc
            for source in sources:
                connection.execute(
                    "INSERT INTO cargo_merge_sources(child_package_id,source_package_id) VALUES(?,?)",
                    (child_id, source["package_id"]),
                )
                connection.execute(
                    "UPDATE cargo_packages SET stage='merged' WHERE package_id=?",
                    (source["package_id"],),
                )
                # 来源包装已并入新件，移除其库位占用，由新件重新上架
                connection.execute(
                    "DELETE FROM cargo_placements WHERE package_id=?",
                    (source["package_id"],),
                )
                connection.execute(
                    "INSERT INTO cargo_batch_moves(package_id,shipment_id,from_batch_id,to_batch_id,"
                    "reason,moved_at) VALUES(?,?,?,?,?,?)",
                    (child_id, shipment_id, source["batch_id"], new_batch_id,
                     f"merge:{reason_t}", now),
                )
            self._assert_conservation(connection, shipment_id)
            self._audit(connection, actor_id=actor["actor_id"], action="packages.merged",
                        resource_type="package", resource_id=child_id,
                        detail={"shipment_id": shipment_id, "batch_id": new_batch_id,
                                "reason": reason_t, "source_package_ids": ids,
                                "quantity": total_qty, "weight": total_weight})
            data = {"shipment_id": shipment_id, "package_id": child_id,
                    "batch_id": new_batch_id, "quantity": total_qty,
                    "source_package_ids": ids}
            return "package", child_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="merge_packages",
                            payload=payload, work=work)

    # ------------------------------------------------------------------ 到港与查验

    def arrive_packages(self, *, request_id: str, actor_id: str, shipment_id: str,
                        package_ids: list[str], at: str | None = None,
                        scan_token: str | None = None) -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "package_ids": package_ids,
                   "at": at, "scan_token": scan_token}

        def work(connection, actor):
            shipment = self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_AIRLINE, DUTY_SORTING)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            replay = self._scan_check(connection, scan_token, ids, "arrive")
            if replay is not None:
                return "scan", scan_token, replay
            moment = self._ts(at, "at", default_now=True)
            arrived: list[str] = []
            for pid in ids:
                row = connection.execute(
                    "SELECT * FROM cargo_packages WHERE package_id=?", (pid,)
                ).fetchone()
                if row["stage"] in ("registered",):
                    connection.execute(
                        "UPDATE cargo_packages SET stage='arrived', arrived_at=?, "
                        "current_custodian=COALESCE(current_custodian,?) WHERE package_id=?",
                        (moment, shipment["carrier_actor_id"], pid),
                    )
                    if row["current_custodian"] is None:
                        connection.execute(
                            "INSERT INTO cargo_custody_events(package_id,custodian,since_at) "
                            "VALUES(?,?,?)",
                            (pid, shipment["carrier_actor_id"], moment),
                        )
                    arrived.append(pid)
                elif row["arrived_at"]:
                    # 重复扫描同票到港：幂等跳过，不产生任何重复事实
                    continue
                else:
                    raise ConflictError(f"包装 {pid} 当前阶段 {row['stage']} 不能到港")
            self._scan_record(connection, scan_token, ids[0], "arrive",
                              canonical_json({"arrived": arrived}))
            self._audit(connection, actor_id=actor["actor_id"], action="cargo.arrived",
                        resource_type="shipment", resource_id=shipment_id,
                        detail={"package_ids": ids, "newly_arrived": arrived, "at": moment,
                                "scan_token": scan_token}, occurred_at=moment)
            data = {"shipment_id": shipment_id, "package_ids": ids,
                    "newly_arrived": arrived, "at": moment}
            return "arrival", shipment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="arrive_packages",
                            payload=payload, work=work)

    def inspect_packages(self, *, request_id: str, actor_id: str, shipment_id: str,
                         package_ids: list[str], at: str | None = None,
                         scan_token: str | None = None) -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "package_ids": package_ids,
                   "at": at, "scan_token": scan_token}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_CUSTOMS)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            replay = self._scan_check(connection, scan_token, ids, "inspect")
            if replay is not None:
                return "scan", scan_token, replay
            moment = self._ts(at, "at", default_now=True)
            inspected: list[str] = []
            for pid in ids:
                row = connection.execute(
                    "SELECT stage, arrived_at, inspected_at FROM cargo_packages WHERE package_id=?",
                    (pid,),
                ).fetchone()
                if not row["arrived_at"]:
                    raise ConflictError(f"包装 {pid} 尚未到港，不能查验")
                if row["stage"] in IMMOBILE_STAGES:
                    raise ConflictError(f"包装 {pid} 已离场或退运")
                if row["inspected_at"] is None:
                    connection.execute(
                        "UPDATE cargo_packages SET stage='inspected', inspected_at=? WHERE package_id=?",
                        (moment, pid),
                    )
                    inspected.append(pid)
            self._scan_record(connection, scan_token, ids[0], "inspect",
                              canonical_json({"inspected": inspected}))
            self._audit(connection, actor_id=actor["actor_id"], action="cargo.inspected",
                        resource_type="shipment", resource_id=shipment_id,
                        detail={"package_ids": ids, "newly_inspected": inspected, "at": moment,
                                "scan_token": scan_token}, occurred_at=moment)
            data = {"shipment_id": shipment_id, "package_ids": ids,
                    "newly_inspected": inspected, "at": moment}
            return "inspection", shipment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="inspect_packages",
                            payload=payload, work=work)

    # ------------------------------------------------------------------ 申报与监管决定

    def submit_declaration(self, *, request_id: str, actor_id: str, shipment_id: str,
                           package_ids: list[str], payload: dict[str, Any],
                           sensitive: bool = False, effective_at: str | None = None,
                           supersedes_id: str | None = None) -> CargoResult:
        if not isinstance(payload, dict) or not payload:
            raise ValidationError("申报 payload 必须是非空对象")
        body = {"actor_id": actor_id, "shipment_id": shipment_id, "package_ids": package_ids,
                "payload_hash": digest(payload), "sensitive": bool(sensitive),
                "effective_at": effective_at, "supersedes_id": supersedes_id}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_AIRLINE, DUTY_CUSTOMS)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            moment = self._ts(effective_at, "effective_at", default_now=True)
            version_row = connection.execute(
                "SELECT COALESCE(MAX(version_no),0)+1 AS next_version FROM cargo_declarations WHERE shipment_id=?",
                (shipment_id,),
            ).fetchone()
            if supersedes_id:
                old = connection.execute(
                    "SELECT * FROM cargo_declarations WHERE declaration_id=? AND shipment_id=?",
                    (supersedes_id, shipment_id),
                ).fetchone()
                if old is None:
                    raise NotFoundError("被修订的申报版本不存在")
                connection.execute(
                    "UPDATE cargo_declarations SET status='superseded' WHERE declaration_id=?",
                    (supersedes_id,),
                )
            declaration_id = self._uuid()
            connection.execute(
                "INSERT INTO cargo_declarations(declaration_id,shipment_id,version_no,scope_json,"
                "payload_json,payload_hash,sensitive,supersedes_id,status,submitted_by,created_at,effective_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (declaration_id, shipment_id, version_row["next_version"], canonical_json(ids),
                 canonical_json(payload), digest(payload), 1 if sensitive else 0, supersedes_id,
                 "submitted", actor["actor_id"], self._now(), moment),
            )
            self._audit(connection, actor_id=actor["actor_id"], action="declaration.submitted",
                        resource_type="declaration", resource_id=declaration_id,
                        detail={"shipment_id": shipment_id, "version_no": version_row["next_version"],
                                "scope": ids, "sensitive": bool(sensitive),
                                "supersedes_id": supersedes_id, "payload_hash": digest(payload),
                                "effective_at": moment})
            data = {"shipment_id": shipment_id, "declaration_id": declaration_id,
                    "version_no": version_row["next_version"], "scope_package_ids": ids,
                    "sensitive": bool(sensitive)}
            return "declaration", declaration_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="submit_declaration",
                            payload=body, work=work)

    def issue_decision(self, *, request_id: str, actor_id: str, shipment_id: str, kind: str,
                       package_ids: list[str], declaration_id: str | None = None,
                       reason: str | None = None, effective_at: str | None = None) -> CargoResult:
        """登记监管决定。可传入早于当前时间的 effective_at 表示迟到决定。"""

        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "kind": kind,
                   "package_ids": package_ids, "declaration_id": declaration_id,
                   "reason": reason, "effective_at": effective_at}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_customs_read(connection, actor, shipment_id)
            if kind not in DECISION_KINDS:
                raise ValidationError("决定种类无效")
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            if declaration_id:
                drow = connection.execute(
                    "SELECT * FROM cargo_declarations WHERE declaration_id=? AND shipment_id=?",
                    (declaration_id, shipment_id),
                ).fetchone()
                if drow is None:
                    raise NotFoundError("关联申报不存在")
            moment = self._ts(effective_at, "effective_at", default_now=True)
            now = self._now()
            seq_row = connection.execute(
                "SELECT COALESCE(MAX(seq),0)+1 AS next_seq FROM cargo_decisions WHERE shipment_id=?",
                (shipment_id,),
            ).fetchone()
            decision_id = self._uuid()
            connection.execute(
                "INSERT INTO cargo_decisions(decision_id,shipment_id,kind,package_ids_json,"
                "declaration_id,reason,decided_by,effective_at,recorded_at,seq) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (decision_id, shipment_id, kind, canonical_json(ids), declaration_id, reason,
                 actor["actor_id"], moment, now, seq_row["next_seq"]),
            )
            # 补件要求即时挂到申报版本上；放行不回写任何历史交接或费用
            if kind == DECISION_REQUEST_SUPPLEMENT and declaration_id:
                connection.execute(
                    "UPDATE cargo_declarations SET status='supplement_requested' WHERE declaration_id=?",
                    (declaration_id,),
                )
            self._audit(connection, actor_id=actor["actor_id"], action=f"decision.{kind}",
                        resource_type="decision", resource_id=decision_id,
                        detail={"shipment_id": shipment_id, "kind": kind, "package_ids": ids,
                                "declaration_id": declaration_id, "reason": reason,
                                "effective_at": moment, "recorded_at": now,
                                "late": moment < now, "seq": seq_row["next_seq"]})
            data = {"shipment_id": shipment_id, "decision_id": decision_id, "kind": kind,
                    "package_ids": ids, "effective_at": moment, "recorded_at": now,
                    "seq": seq_row["next_seq"], "late": moment < now}
            return "decision", decision_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="issue_decision",
                            payload=payload, work=work)

    def submit_supplement(self, *, request_id: str, actor_id: str, decision_id: str,
                          payload: dict[str, Any], sensitive: bool | None = None) -> CargoResult:
        if not isinstance(payload, dict) or not payload:
            raise ValidationError("补件 payload 必须是非空对象")
        body = {"actor_id": actor_id, "decision_id": decision_id,
                "payload_hash": digest(payload), "sensitive": sensitive}

        def work(connection, actor):
            decision = connection.execute(
                "SELECT * FROM cargo_decisions WHERE decision_id=?", (decision_id,)
            ).fetchone()
            if decision is None:
                raise NotFoundError("监管决定不存在")
            if decision["kind"] != DECISION_REQUEST_SUPPLEMENT:
                raise ValidationError("只能对补件要求提交补件")
            shipment_id = decision["shipment_id"]
            self._require_duty(connection, actor, shipment_id, DUTY_AIRLINE, DUTY_CUSTOMS)
            scope = json.loads(decision["package_ids_json"])
            old_declaration_id = decision["declaration_id"]
            is_sensitive = bool(sensitive) if sensitive is not None else False
            if old_declaration_id:
                old = connection.execute(
                    "SELECT sensitive FROM cargo_declarations WHERE declaration_id=?",
                    (old_declaration_id,),
                ).fetchone()
                if old:
                    is_sensitive = bool(sensitive) if sensitive is not None else bool(old["sensitive"])
                    connection.execute(
                        "UPDATE cargo_declarations SET status='superseded' WHERE declaration_id=?",
                        (old_declaration_id,),
                    )
            version_row = connection.execute(
                "SELECT COALESCE(MAX(version_no),0)+1 AS next_version FROM cargo_declarations WHERE shipment_id=?",
                (shipment_id,),
            ).fetchone()
            new_id = self._uuid()
            now = self._now()
            connection.execute(
                "INSERT INTO cargo_declarations(declaration_id,shipment_id,version_no,scope_json,"
                "payload_json,payload_hash,sensitive,supersedes_id,status,submitted_by,created_at,effective_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (new_id, shipment_id, version_row["next_version"], canonical_json(scope),
                 canonical_json(payload), digest(payload), 1 if is_sensitive else 0,
                 old_declaration_id, "submitted", actor["actor_id"], now, now),
            )
            connection.execute(
                "INSERT INTO cargo_supplements(decision_id,declaration_id,submitted_by,created_at) "
                "VALUES(?,?,?,?)",
                (decision_id, new_id, actor["actor_id"], now),
            )
            self._audit(connection, actor_id=actor["actor_id"], action="declaration.supplemented",
                        resource_type="declaration", resource_id=new_id,
                        detail={"shipment_id": shipment_id, "decision_id": decision_id,
                                "version_no": version_row["next_version"],
                                "supersedes_id": old_declaration_id, "sensitive": is_sensitive,
                                "scope": scope})
            data = {"shipment_id": shipment_id, "declaration_id": new_id,
                    "version_no": version_row["next_version"], "decision_id": decision_id,
                    "scope_package_ids": scope}
            return "declaration", new_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="submit_supplement",
                            payload=body, work=work)

    def clear_inspection(self, *, request_id: str, actor_id: str,
                         decision_id: str) -> CargoResult:
        payload = {"actor_id": actor_id, "decision_id": decision_id}

        def work(connection, actor):
            decision = connection.execute(
                "SELECT * FROM cargo_decisions WHERE decision_id=?", (decision_id,)
            ).fetchone()
            if decision is None:
                raise NotFoundError("监管决定不存在")
            if decision["kind"] != DECISION_INSPECT:
                raise ValidationError("只能核销查验决定")
            shipment_id = decision["shipment_id"]
            self._require_customs_read(connection, actor, shipment_id)
            if connection.execute(
                "SELECT 1 FROM cargo_inspection_clears WHERE decision_id=?", (decision_id,)
            ).fetchone():
                raise ConflictError("该查验决定已经核销")
            now = self._now()
            connection.execute(
                "INSERT INTO cargo_inspection_clears(decision_id,cleared_by,cleared_at) "
                "VALUES(?,?,?)",
                (decision_id, actor["actor_id"], now),
            )
            self._audit(connection, actor_id=actor["actor_id"], action="inspection.cleared",
                        resource_type="decision", resource_id=decision_id,
                        detail={"shipment_id": shipment_id})
            data = {"shipment_id": shipment_id, "decision_id": decision_id, "cleared_at": now}
            return "decision", decision_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="clear_inspection",
                            payload=payload, work=work)

    # ------------------------------------------------------------------ 仓位与库存

    def register_location(self, *, request_id: str, actor_id: str, location_id: str,
                          code: str, capacity_qty: float) -> CargoResult:
        payload = {"actor_id": actor_id, "location_id": location_id, "code": code,
                   "capacity_qty": capacity_qty}

        def work(connection, actor):
            # 仓储资料是全局资源：admin/operator 或在任一运单上承担仓储职责者可登记
            is_warehouse = connection.execute(
                "SELECT 1 FROM cargo_duties WHERE actor_id=? AND duty=? LIMIT 1",
                (actor["actor_id"], DUTY_WAREHOUSE),
            ).fetchone()
            if actor["role"] not in ("admin", "operator") and is_warehouse is None:
                raise PermissionDenied("只有仓储或管理员可以登记仓位")
            capacity = self._positive_qty(capacity_qty, "capacity_qty")
            code_t = str(code).strip()
            lid = str(location_id).strip()
            if not lid or not code_t:
                raise ValidationError("location_id/code 不能为空")
            try:
                connection.execute(
                    "INSERT INTO cargo_locations(location_id,code,capacity_qty,status) "
                    "VALUES(?,?,?,'active')",
                    (lid, code_t, capacity),
                )
            except Exception as exc:
                raise ConflictError("仓位编号或代码已经存在") from exc
            self._audit(connection, actor_id=actor["actor_id"], action="location.registered",
                        resource_type="location", resource_id=lid,
                        detail={"code": code_t, "capacity_qty": capacity})
            data = {"location_id": lid, "code": code_t, "capacity_qty": capacity}
            return "location", lid, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="register_location",
                            payload=payload, work=work)

    def lease_space(self, *, request_id: str, actor_id: str, shipment_id: str,
                    location_id: str, quantity: float, starts_at: str | None = None,
                    ends_at: str | None = None) -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "location_id": location_id,
                   "quantity": quantity, "starts_at": starts_at, "ends_at": ends_at}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_WAREHOUSE, DUTY_AIRLINE)
            location = connection.execute(
                "SELECT * FROM cargo_locations WHERE location_id=?", (location_id,)
            ).fetchone()
            if location is None:
                raise NotFoundError("仓位不存在")
            if location["status"] != "active":
                raise ConflictError("仓位已停用")
            qty = self._positive_qty(quantity, "quantity")
            starts = self._ts(starts_at, "starts_at", default_now=True) if starts_at or starts_at is None else None
            ends = self._ts(ends_at, "ends_at") if ends_at else None
            if ends and starts and ends < starts:
                raise ValidationError("租约结束时间不能早于开始时间")
            lease_id = self._uuid()
            connection.execute(
                "INSERT INTO cargo_leases(lease_id,location_id,shipment_id,quantity,starts_at,"
                "ends_at,status,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (lease_id, location_id, shipment_id, qty, starts, ends, "locked",
                 actor["actor_id"], self._now()),
            )
            self._audit(connection, actor_id=actor["actor_id"], action="lease.locked",
                        resource_type="lease", resource_id=lease_id,
                        detail={"shipment_id": shipment_id, "location_id": location_id,
                                "quantity": qty, "starts_at": starts, "ends_at": ends})
            data = {"lease_id": lease_id, "shipment_id": shipment_id,
                    "location_id": location_id, "quantity": qty,
                    "starts_at": starts, "ends_at": ends, "status": "locked"}
            return "lease", lease_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="lease_space",
                            payload=payload, work=work)

    def place_packages(self, *, request_id: str, actor_id: str, shipment_id: str,
                       package_ids: list[str], location_id: str, lease_id: str | None = None,
                       at: str | None = None, scan_token: str | None = None) -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "package_ids": package_ids,
                   "location_id": location_id, "lease_id": lease_id, "at": at,
                   "scan_token": scan_token}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_WAREHOUSE)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            location = connection.execute(
                "SELECT * FROM cargo_locations WHERE location_id=?", (location_id,)
            ).fetchone()
            if location is None:
                raise NotFoundError("仓位不存在")
            if lease_id:
                lease = connection.execute(
                    "SELECT * FROM cargo_leases WHERE lease_id=? AND shipment_id=? AND location_id=?",
                    (lease_id, shipment_id, location_id),
                ).fetchone()
                if lease is None:
                    raise NotFoundError("租约不存在或不属于该仓位/运单")
                if lease["status"] != "locked":
                    raise ConflictError("租约已释放")
            replay = self._scan_check(connection, scan_token, ids, "place")
            if replay is not None:
                return "scan", scan_token, replay
            moment = self._ts(at, "at", default_now=True)

            placed: list[str] = []
            add_qty = 0.0
            for pid in ids:
                package = connection.execute(
                    "SELECT stage, quantity, arrived_at FROM cargo_packages WHERE package_id=?",
                    (pid,),
                ).fetchone()
                if package["stage"] in FINAL_STAGES:
                    raise ConflictError(f"包装 {pid} 已退运，不能入库")
                if not package["arrived_at"]:
                    raise ConflictError(f"包装 {pid} 尚未到港，不能入库")
                existing = connection.execute(
                    "SELECT location_id FROM cargo_placements WHERE package_id=?", (pid,)
                ).fetchone()
                if existing is not None:
                    if existing["location_id"] == location_id:
                        continue  # 重复上架到同库位：幂等跳过，不重复占用容量
                    raise ConflictError(f"包装 {pid} 已在另一库位，必须先下架")
                add_qty += package["quantity"]
                placed.append(pid)

            used = connection.execute(
                "SELECT COALESCE(SUM(p.quantity),0) AS used FROM cargo_placements pl "
                "JOIN cargo_packages p ON p.package_id=pl.package_id WHERE pl.location_id=?",
                (location_id,),
            ).fetchone()["used"]
            if used + add_qty > location["capacity_qty"] + 1e-9:
                raise ConflictError(
                    f"库位容量不足：已占 {used}，新增 {add_qty}，容量 {location['capacity_qty']}"
                )
            if lease_id:
                lease_used = connection.execute(
                    "SELECT COALESCE(SUM(p.quantity),0) AS used FROM cargo_placements pl "
                    "JOIN cargo_packages p ON p.package_id=pl.package_id WHERE pl.lease_id=?",
                    (lease_id,),
                ).fetchone()["used"]
                lease_qty = connection.execute(
                    "SELECT quantity FROM cargo_leases WHERE lease_id=?", (lease_id,)
                ).fetchone()["quantity"]
                if lease_used + add_qty > lease_qty + 1e-9:
                    raise ConflictError("超出租约预留数量")

            for pid in placed:
                try:
                    connection.execute(
                        "INSERT INTO cargo_placements(package_id,location_id,lease_id,placed_by,placed_at) "
                        "VALUES(?,?,?,?,?)",
                        (pid, location_id, lease_id, actor["actor_id"], moment),
                    )
                except Exception:
                    # 与并发占位相撞：同一包装不能出现在两个位置
                    raise ConflictError(f"包装 {pid} 已被并发放置到其他库位")
            self._scan_record(connection, scan_token, ids[0], "place",
                              canonical_json({"placed": placed, "location_id": location_id}))
            self._audit(connection, actor_id=actor["actor_id"], action="packages.placed",
                        resource_type="location", resource_id=location_id,
                        detail={"shipment_id": shipment_id, "package_ids": placed,
                                "lease_id": lease_id, "at": moment, "scan_token": scan_token},
                        occurred_at=moment)
            data = {"shipment_id": shipment_id, "location_id": location_id,
                    "package_ids": placed, "lease_id": lease_id, "at": moment}
            return "placement", shipment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="place_packages",
                            payload=payload, work=work)

    def pickup_packages(self, *, request_id: str, actor_id: str, shipment_id: str,
                        package_ids: list[str], at: str | None = None) -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "package_ids": package_ids,
                   "at": at}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_WAREHOUSE, DUTY_AIRLINE,
                               DUTY_SORTING)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            moment = self._ts(at, "at", default_now=True)
            removed: list[str] = []
            for pid in ids:
                cur = connection.execute(
                    "DELETE FROM cargo_placements WHERE package_id=? RETURNING location_id",
                    (pid,),
                ).fetchone()
                if cur is not None:
                    removed.append(pid)
            self._audit(connection, actor_id=actor["actor_id"], action="packages.picked_up",
                        resource_type="shipment", resource_id=shipment_id,
                        detail={"package_ids": removed, "at": moment}, occurred_at=moment)
            data = {"shipment_id": shipment_id, "package_ids": removed, "at": moment}
            return "pickup", shipment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="pickup_packages",
                            payload=payload, work=work)

    # ------------------------------------------------------------------ 航段容量与改签

    def register_segment(self, *, request_id: str, actor_id: str, segment_id: str,
                         flight_no: str, origin: str, destination: str, departs_at: str,
                         capacity_qty: float, capacity_weight: float = 0.0) -> CargoResult:
        payload = {"actor_id": actor_id, "segment_id": segment_id, "flight_no": flight_no,
                   "origin": origin, "destination": destination, "departs_at": departs_at,
                   "capacity_qty": capacity_qty, "capacity_weight": capacity_weight}

        def work(connection, actor):
            # 航段是全局资源：admin 或在任一运单上承担航空承运人职责者可登记
            is_airline = connection.execute(
                "SELECT 1 FROM cargo_duties WHERE actor_id=? AND duty=? LIMIT 1",
                (actor["actor_id"], DUTY_AIRLINE),
            ).fetchone()
            if actor["role"] != "admin" and is_airline is None:
                raise PermissionDenied("只有航空承运人或管理员可以登记航段")
            flight = str(flight_no).strip()
            seg_id = str(segment_id).strip()
            origin_t = str(origin).strip()
            destination_t = str(destination).strip()
            if not seg_id or not flight or not origin_t or not destination_t:
                raise ValidationError("航段编号、航班号、起止港不能为空")
            cap_qty = self._positive_qty(capacity_qty, "capacity_qty")
            cap_weight = self._nonneg_weight(capacity_weight, "capacity_weight")
            departs = self._ts(departs_at, "departs_at")
            try:
                connection.execute(
                    "INSERT INTO cargo_segments(segment_id,flight_no,origin,destination,departs_at,"
                    "capacity_qty,capacity_weight,status,created_at) "
                    "VALUES(?,?,?,?,?,?,?,'open',?)",
                    (seg_id, flight, origin_t, destination_t, departs, cap_qty, cap_weight,
                     self._now()),
                )
            except Exception as exc:
                raise ConflictError("航段编号已经存在") from exc
            self._audit(connection, actor_id=actor["actor_id"], action="segment.registered",
                        resource_type="segment", resource_id=seg_id,
                        detail={"flight_no": flight, "origin": origin_t,
                                "destination": destination_t, "departs_at": departs,
                                "capacity_qty": cap_qty, "capacity_weight": cap_weight})
            data = {"segment_id": seg_id, "flight_no": flight, "origin": origin_t,
                    "destination": destination_t, "departs_at": departs,
                    "capacity_qty": cap_qty, "capacity_weight": cap_weight}
            return "segment", seg_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="register_segment",
                            payload=payload, work=work)

    def _segment_load(self, connection, segment_id: str) -> tuple[float, float]:
        row = connection.execute(
            "SELECT COALESCE(SUM(a.quantity),0) AS qty, COALESCE(SUM(p.weight),0) AS weight "
            "FROM cargo_allocations a JOIN cargo_packages p ON p.package_id=a.package_id "
            "WHERE a.segment_id=? AND a.state IN ('held','confirmed')",
            (segment_id,),
        ).fetchone()
        return row["qty"], row["weight"]

    def allocate_segment(self, *, request_id: str, actor_id: str, shipment_id: str,
                         segment_id: str, package_ids: list[str], confirm: bool = False,
                         scan_token: str | None = None) -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "segment_id": segment_id,
                   "package_ids": package_ids, "confirm": confirm, "scan_token": scan_token}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_AIRLINE, DUTY_SORTING)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            segment = connection.execute(
                "SELECT * FROM cargo_segments WHERE segment_id=?", (segment_id,)
            ).fetchone()
            if segment is None:
                raise NotFoundError("航段不存在")
            if segment["status"] != "open":
                raise ConflictError("航段已关闭")
            replay = self._scan_check(connection, scan_token, ids, "allocate")
            if replay is not None:
                return "scan", scan_token, replay

            add_qty = 0.0
            add_weight = 0.0
            for pid in ids:
                package = connection.execute(
                    "SELECT quantity, weight, stage FROM cargo_packages WHERE package_id=?", (pid,)
                ).fetchone()
                if package["stage"] in FINAL_STAGES:
                    raise ConflictError(f"包装 {pid} 已退运，不能占位")
                active = connection.execute(
                    "SELECT segment_id FROM cargo_allocations WHERE package_id=? "
                    "AND state IN ('held','confirmed')",
                    (pid,),
                ).fetchone()
                if active is not None:
                    raise ConflictError(f"包装 {pid} 已在航段 {active['segment_id']} 上占位")
                add_qty += package["quantity"]
                add_weight += package["weight"]

            used_qty, used_weight = self._segment_load(connection, segment_id)
            if used_qty + add_qty > segment["capacity_qty"] + 1e-9:
                raise ConflictError("航段数量容量不足")
            if segment["capacity_weight"] > 0 and used_weight + add_weight > segment["capacity_weight"] + 1e-9:
                raise ConflictError("航段重量容量不足")

            state = "held"
            if confirm:
                blockers = self._departure_blockers(connection, shipment_id, ids, at=self._now())
                if blockers:
                    raise ConflictError("以下包装尚未满足放行条件，不能确认占仓："
                                        + canonical_json(blockers))
                state = "confirmed"

            created: list[str] = []
            now = self._now()
            for pid in ids:
                allocation_id = self._uuid()
                try:
                    connection.execute(
                        "INSERT INTO cargo_allocations(allocation_id,segment_id,package_id,quantity,"
                        "state,request_id,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                        (allocation_id, segment_id, pid,
                         connection.execute("SELECT quantity FROM cargo_packages WHERE package_id=?",
                                            (pid,)).fetchone()["quantity"],
                         state, request_id, actor["actor_id"], now),
                    )
                except Exception:
                    raise ConflictError(f"包装 {pid} 已被并发占位")
                created.append(allocation_id)
            self._scan_record(connection, scan_token, ids[0], "allocate",
                              canonical_json({"segment_id": segment_id, "allocations": created}))
            self._audit(connection, actor_id=actor["actor_id"], action="segment.allocated",
                        resource_type="segment", resource_id=segment_id,
                        detail={"shipment_id": shipment_id, "package_ids": ids, "state": state,
                                "scan_token": scan_token})
            data = {"shipment_id": shipment_id, "segment_id": segment_id,
                    "package_ids": ids, "state": state, "allocation_ids": created}
            return "allocation", segment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="allocate_segment",
                            payload=payload, work=work)

    def confirm_allocation(self, *, request_id: str, actor_id: str, shipment_id: str,
                           segment_id: str, package_ids: list[str]) -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "segment_id": segment_id,
                   "package_ids": package_ids}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_AIRLINE)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            blockers = self._departure_blockers(connection, shipment_id, ids, at=self._now())
            if blockers:
                raise ConflictError("以下包装尚未满足放行条件：" + canonical_json(blockers))
            confirmed: list[str] = []
            for pid in ids:
                row = connection.execute(
                    "SELECT state FROM cargo_allocations WHERE segment_id=? AND package_id=? "
                    "AND state IN ('held','confirmed')",
                    (segment_id, pid),
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"包装 {pid} 在该航段没有生效占位")
                if row["state"] == "held":
                    connection.execute(
                        "UPDATE cargo_allocations SET state='confirmed' "
                        "WHERE segment_id=? AND package_id=? AND state='held'",
                        (segment_id, pid),
                    )
                confirmed.append(pid)
            self._audit(connection, actor_id=actor["actor_id"], action="segment.confirmed",
                        resource_type="segment", resource_id=segment_id,
                        detail={"shipment_id": shipment_id, "package_ids": confirmed})
            data = {"shipment_id": shipment_id, "segment_id": segment_id,
                    "package_ids": confirmed, "state": "confirmed"}
            return "allocation", segment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="confirm_allocation",
                            payload=payload, work=work)

    def release_allocation(self, *, request_id: str, actor_id: str, shipment_id: str,
                           segment_id: str, package_ids: list[str],
                           reason: str = "rebook") -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "segment_id": segment_id,
                   "package_ids": package_ids, "reason": reason}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_AIRLINE, DUTY_SORTING)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            released: list[str] = []
            for pid in ids:
                row = connection.execute(
                    "SELECT state FROM cargo_allocations WHERE segment_id=? AND package_id=? "
                    "AND state IN ('held','confirmed')",
                    (segment_id, pid),
                ).fetchone()
                if row is None:
                    continue
                connection.execute(
                    "UPDATE cargo_allocations SET state='cancelled' WHERE segment_id=? "
                    "AND package_id=? AND state IN ('held','confirmed')",
                    (segment_id, pid),
                )
                released.append(pid)
            self._audit(connection, actor_id=actor["actor_id"], action="segment.release_allocated",
                        resource_type="segment", resource_id=segment_id,
                        detail={"shipment_id": shipment_id, "package_ids": released, "reason": reason})
            data = {"shipment_id": shipment_id, "segment_id": segment_id,
                    "package_ids": released, "state": "cancelled"}
            return "allocation", segment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="release_allocation",
                            payload=payload, work=work)

    def rebook_segment(self, *, request_id: str, actor_id: str, shipment_id: str,
                       package_ids: list[str], from_segment_id: str, to_segment_id: str,
                       at: str | None = None, rebooking_fee: float = 0.0,
                       currency: str = "CNY", fee_party: str | None = None) -> CargoResult:
        """承运人改签：作废旧占位（事实保留）、在新航段重新占位并记录改签费用。"""

        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "package_ids": package_ids,
                   "from_segment_id": from_segment_id, "to_segment_id": to_segment_id,
                   "at": at, "rebooking_fee": rebooking_fee, "currency": currency,
                   "fee_party": fee_party}

        def work(connection, actor):
            shipment = self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_AIRLINE)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            old_seg = connection.execute(
                "SELECT * FROM cargo_segments WHERE segment_id=?", (from_segment_id,)
            ).fetchone()
            new_seg = connection.execute(
                "SELECT * FROM cargo_segments WHERE segment_id=?", (to_segment_id,)
            ).fetchone()
            if old_seg is None or new_seg is None:
                raise NotFoundError("航段不存在")
            if from_segment_id == to_segment_id:
                raise ValidationError("新旧航段不能相同")
            moment = self._ts(at, "at", default_now=True)

            # 全部旧占位校验通过后才动手，避免半成品状态
            old_states: dict[str, str] = {}
            add_qty = 0.0
            add_weight = 0.0
            for pid in ids:
                row = connection.execute(
                    "SELECT state FROM cargo_allocations WHERE segment_id=? AND package_id=?",
                    (from_segment_id, pid),
                ).fetchone()
                if row is None or row["state"] not in ("held", "confirmed"):
                    raise ConflictError(f"包装 {pid} 在原航段没有生效占位，不能改签")
                package = connection.execute(
                    "SELECT quantity, weight, stage FROM cargo_packages WHERE package_id=?", (pid,)
                ).fetchone()
                if package["stage"] in FINAL_STAGES:
                    raise ConflictError(f"包装 {pid} 已退运")
                old_states[pid] = row["state"]
                add_qty += package["quantity"]
                add_weight += package["weight"]
            used_qty, used_weight = self._segment_load(connection, to_segment_id)
            if used_qty + add_qty > new_seg["capacity_qty"] + 1e-9:
                raise ConflictError("改签目标航段数量容量不足")
            if new_seg["capacity_weight"] > 0 and used_weight + add_weight > new_seg["capacity_weight"] + 1e-9:
                raise ConflictError("改签目标航段重量容量不足")

            new_allocations: list[str] = []
            for pid in ids:
                connection.execute(
                    "UPDATE cargo_allocations SET state='rebooked' WHERE segment_id=? "
                    "AND package_id=? AND state IN ('held','confirmed')",
                    (from_segment_id, pid),
                )
                package = connection.execute(
                    "SELECT quantity FROM cargo_packages WHERE package_id=?", (pid,)
                ).fetchone()
                allocation_id = self._uuid()
                # 改签后默认重新占位为 held，等待放行后确认，避免越过海关闸门
                connection.execute(
                    "INSERT INTO cargo_allocations(allocation_id,segment_id,package_id,quantity,"
                    "state,request_id,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (allocation_id, to_segment_id, pid, package["quantity"], "held",
                     request_id, actor["actor_id"], moment),
                )
                new_allocations.append(allocation_id)

            fee_id = None
            fee_amount = self._nonneg_weight(rebooking_fee, "rebooking_fee")
            if fee_amount > 0:
                fee_id = self._record_fee(
                    connection, shipment_id=shipment_id, package_ids=ids,
                    fee_type="rebooking", amount=fee_amount,
                    currency=str(currency).strip() or "CNY",
                    responsible_party=fee_party or shipment["carrier_actor_id"],
                    ref_type="segment", ref_id=to_segment_id, incurred_at=moment,
                )
            self._audit(connection, actor_id=actor["actor_id"], action="segment.rebooked",
                        resource_type="segment", resource_id=to_segment_id,
                        detail={"shipment_id": shipment_id, "package_ids": ids,
                                "from_segment_id": from_segment_id,
                                "to_segment_id": to_segment_id, "at": moment,
                                "rebooking_fee_id": fee_id})
            data = {"shipment_id": shipment_id, "package_ids": ids,
                    "from_segment_id": from_segment_id, "to_segment_id": to_segment_id,
                    "state": "held", "allocation_ids": new_allocations,
                    "rebooking_fee_id": fee_id}
            return "allocation", to_segment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="rebook_segment",
                            payload=payload, work=work)

    def close_segment(self, *, request_id: str, actor_id: str, segment_id: str,
                      at: str | None = None) -> CargoResult:
        """航段截关：未实际离场（未转为 flown）的占位全部失效并释放容量。

        已经通过 depart_packages 记为 flown 的占位不受影响；遗留的 held/confirmed
        占位转为 expired，对应包装在待办中重新进入配舱环节。
        """

        payload = {"actor_id": actor_id, "segment_id": segment_id, "at": at}

        def work(connection, actor):
            segment = connection.execute(
                "SELECT * FROM cargo_segments WHERE segment_id=?", (segment_id,)
            ).fetchone()
            if segment is None:
                raise NotFoundError("航段不存在")
            is_airline = connection.execute(
                "SELECT 1 FROM cargo_duties WHERE actor_id=? AND duty=? LIMIT 1",
                (actor["actor_id"], DUTY_AIRLINE),
            ).fetchone()
            if actor["role"] != "admin" and is_airline is None:
                raise PermissionDenied("只有航空承运人或管理员可以截关航段")
            moment = self._ts(at, "at", default_now=True)
            expired = [row["package_id"] for row in connection.execute(
                "SELECT package_id FROM cargo_allocations WHERE segment_id=? "
                "AND state IN ('held','confirmed')", (segment_id,))]
            connection.execute(
                "UPDATE cargo_allocations SET state='expired' WHERE segment_id=? "
                "AND state IN ('held','confirmed')", (segment_id,))
            connection.execute(
                "UPDATE cargo_segments SET status='closed' WHERE segment_id=?", (segment_id,))
            self._audit(connection, actor_id=actor["actor_id"], action="segment.closed",
                        resource_type="segment", resource_id=segment_id,
                        detail={"expired_package_ids": expired, "at": moment},
                        occurred_at=moment)
            data = {"segment_id": segment_id, "status": "closed",
                    "expired_package_ids": expired, "at": moment}
            return "segment", segment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="close_segment",
                            payload=payload, work=work)

    def depart_packages(self, *, request_id: str, actor_id: str, shipment_id: str,
                        segment_id: str, package_ids: list[str],
                        at: str | None = None) -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "segment_id": segment_id,
                   "package_ids": package_ids, "at": at}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_AIRLINE)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            moment = self._ts(at, "at", default_now=True)
            blockers = self._departure_blockers(connection, shipment_id, ids, at=moment)
            if blockers:
                raise ConflictError("以下包装不满足离场条件：" + canonical_json(blockers))
            departed: list[str] = []
            for pid in ids:
                row = connection.execute(
                    "SELECT state FROM cargo_allocations WHERE segment_id=? AND package_id=?",
                    (segment_id, pid),
                ).fetchone()
                if row is None:
                    raise ConflictError(f"包装 {pid} 未配舱到该航段")
                if row["state"] == "flown":
                    continue  # 重复发运幂等
                if row["state"] != "confirmed":
                    raise ConflictError(f"包装 {pid} 占位状态为 {row['state']}，未确认不能离场")
                connection.execute(
                    "UPDATE cargo_allocations SET state='flown' WHERE segment_id=? AND package_id=?",
                    (segment_id, pid),
                )
                connection.execute(
                    "UPDATE cargo_packages SET stage='departed', departed_at=? WHERE package_id=?",
                    (moment, pid),
                )
                departed.append(pid)
            self._audit(connection, actor_id=actor["actor_id"], action="cargo.departed",
                        resource_type="segment", resource_id=segment_id,
                        detail={"shipment_id": shipment_id, "package_ids": departed, "at": moment},
                        occurred_at=moment)
            data = {"shipment_id": shipment_id, "segment_id": segment_id,
                    "package_ids": departed, "at": moment}
            return "departure", segment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="depart_packages",
                            payload=payload, work=work)

    # ------------------------------------------------------------------ 责任交接与退运

    def handoff(self, *, request_id: str, actor_id: str, shipment_id: str, kind: str,
                from_actor_id: str, to_actor_id: str, package_ids: list[str],
                location: str | None = None, at: str | None = None,
                handling_fee: float = 0.0, currency: str = "CNY") -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "kind": kind,
                   "from_actor_id": from_actor_id, "to_actor_id": to_actor_id,
                   "package_ids": package_ids, "location": location, "at": at,
                   "handling_fee": handling_fee, "currency": currency}

        def work(connection, actor):
            shipment = self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id,
                               DUTY_AIRLINE, DUTY_SORTING, DUTY_WAREHOUSE, DUTY_CUSTOMS)
            kind_t = str(kind).strip()
            if not kind_t:
                raise ValidationError("kind 不能为空")
            from_actor = self._actor(connection, from_actor_id)
            to_actor = self._actor(connection, to_actor_id)
            if actor["role"] != "admin" and actor["actor_id"] != from_actor_id:
                raise PermissionDenied("只能由当前交出方发起交接")
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            moment = self._ts(at, "at", default_now=True)
            for pid in ids:
                row = connection.execute(
                    "SELECT current_custodian, stage FROM cargo_packages WHERE package_id=?",
                    (pid,),
                ).fetchone()
                if row["stage"] in FINAL_STAGES:
                    raise ConflictError(f"包装 {pid} 已退运，不能交接")
                if row["current_custodian"] is None:
                    raise ConflictError(f"包装 {pid} 尚无保管方记录（未到港）")
                if row["current_custodian"] != from_actor_id:
                    raise ConflictError(
                        f"包装 {pid} 当前保管方为 {row['current_custodian']}，与交出方不一致"
                    )
            handoff_id = self._uuid()
            connection.execute(
                "INSERT INTO cargo_handoffs(handoff_id,shipment_id,kind,from_actor,to_actor,"
                "package_ids_json,location,occurred_at,completed_by,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (handoff_id, shipment_id, kind_t, from_actor_id, to_actor_id,
                 canonical_json(ids), location, moment, actor["actor_id"], self._now()),
            )
            for pid in ids:
                connection.execute(
                    "UPDATE cargo_packages SET current_custodian=? WHERE package_id=?",
                    (to_actor_id, pid),
                )
                connection.execute(
                    "INSERT INTO cargo_custody_events(package_id,custodian,handoff_id,since_at) "
                    "VALUES(?,?,?,?)",
                    (pid, to_actor_id, handoff_id, moment),
                )
            fee_id = None
            fee_amount = self._nonneg_weight(handling_fee, "handling_fee")
            if fee_amount > 0:
                fee_id = self._record_fee(
                    connection, shipment_id=shipment_id, package_ids=ids,
                    fee_type=f"handling:{kind_t}", amount=fee_amount,
                    currency=str(currency).strip() or "CNY",
                    responsible_party=shipment["carrier_actor_id"],
                    ref_type="handoff", ref_id=handoff_id, incurred_at=moment,
                )
            self._audit(connection, actor_id=actor["actor_id"], action="custody.handed_over",
                        resource_type="handoff", resource_id=handoff_id,
                        detail={"shipment_id": shipment_id, "kind": kind_t,
                                "from_actor": from_actor_id, "to_actor": to_actor_id,
                                "package_ids": ids, "location": location, "at": moment,
                                "fee_id": fee_id}, occurred_at=moment)
            data = {"handoff_id": handoff_id, "shipment_id": shipment_id, "kind": kind_t,
                    "from_actor": from_actor_id, "to_actor": to_actor_id,
                    "package_ids": ids, "location": location, "at": moment,
                    "handling_fee_id": fee_id}
            return "handoff", handoff_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="handoff",
                            payload=payload, work=work)

    def return_cargo(self, *, request_id: str, actor_id: str, shipment_id: str,
                     package_ids: list[str], to_actor_id: str | None = None,
                     at: str | None = None) -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "package_ids": package_ids,
                   "to_actor_id": to_actor_id, "at": at}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id, DUTY_CUSTOMS, DUTY_AIRLINE)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            moment = self._ts(at, "at", default_now=True)
            decisions = connection.execute(
                "SELECT * FROM cargo_decisions WHERE shipment_id=? AND kind=? ORDER BY seq",
                (shipment_id, DECISION_RETURN),
            ).fetchall()
            effective = [d for d in decisions if d["effective_at"] <= moment]
            for pid in ids:
                row = connection.execute(
                    "SELECT stage, departed_at FROM cargo_packages WHERE package_id=?", (pid,)
                ).fetchone()
                if row["stage"] == "returned":
                    continue
                if row["stage"] == "departed":
                    # 已离场包装不在本枢纽，迟到退运决定不能改写已完成的路径
                    raise ConflictError(f"包装 {pid} 已离场，不能在本枢纽执行退运")
                covered = any(pid in self._decision_scope(connection, shipment_id, d)
                              for d in effective)
                if not covered:
                    raise ConflictError(f"包装 {pid} 没有生效的退运决定，不能退运")
            returned: list[str] = []
            handoff_id = None
            target = to_actor_id
            for pid in ids:
                row = connection.execute(
                    "SELECT stage FROM cargo_packages WHERE package_id=?", (pid,)
                ).fetchone()
                if row["stage"] == "returned":
                    continue
                connection.execute(
                    "UPDATE cargo_allocations SET state='cancelled' WHERE package_id=? "
                    "AND state IN ('held','confirmed')",
                    (pid,),
                )
                # 退运货物离库，释放库位占用；上架事实保留在审计链中
                connection.execute(
                    "DELETE FROM cargo_placements WHERE package_id=?", (pid,))
                connection.execute(
                    "UPDATE cargo_packages SET stage='returned', returned_at=? WHERE package_id=?",
                    (moment, pid),
                )
                returned.append(pid)
            if returned and target:
                target_actor = self._actor(connection, target)
                handoff_id = self._uuid()
                connection.execute(
                    "INSERT INTO cargo_handoffs(handoff_id,shipment_id,kind,from_actor,to_actor,"
                    "package_ids_json,location,occurred_at,completed_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (handoff_id, shipment_id, "return", actor["actor_id"], target,
                     canonical_json(returned), None, moment, actor["actor_id"], self._now()),
                )
                for pid in returned:
                    custodian = connection.execute(
                        "SELECT current_custodian FROM cargo_packages WHERE package_id=?", (pid,)
                    ).fetchone()["current_custodian"]
                    if custodian:
                        connection.execute(
                            "INSERT INTO cargo_custody_events(package_id,custodian,handoff_id,since_at) "
                            "VALUES(?,?,?,?)",
                            (pid, target, handoff_id, moment),
                        )
                    connection.execute(
                        "UPDATE cargo_packages SET current_custodian=? WHERE package_id=?",
                        (target, pid),
                    )
            self._audit(connection, actor_id=actor["actor_id"], action="cargo.returned",
                        resource_type="shipment", resource_id=shipment_id,
                        detail={"package_ids": returned, "to_actor": target, "at": moment,
                                "handoff_id": handoff_id}, occurred_at=moment)
            data = {"shipment_id": shipment_id, "package_ids": returned,
                    "to_actor": target, "at": moment, "handoff_id": handoff_id}
            return "return", shipment_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="return_cargo",
                            payload=payload, work=work)

    def record_fee(self, *, request_id: str, actor_id: str, shipment_id: str,
                   package_ids: list[str], fee_type: str, amount: float,
                   responsible_party: str, currency: str = "CNY",
                   incurred_at: str | None = None) -> CargoResult:
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "package_ids": package_ids,
                   "fee_type": fee_type, "amount": amount, "responsible_party": responsible_party,
                   "currency": currency, "incurred_at": incurred_at}

        def work(connection, actor):
            self._shipment(connection, shipment_id)
            self._require_duty(connection, actor, shipment_id,
                               DUTY_AIRLINE, DUTY_SORTING, DUTY_WAREHOUSE, DUTY_CUSTOMS)
            ids = self._normalize_package_ids(connection, shipment_id, package_ids)
            fee_type_t = str(fee_type).strip()
            if not fee_type_t:
                raise ValidationError("fee_type 不能为空")
            amount_f = self._nonneg_weight(amount, "amount")
            party = self._actor(connection, responsible_party)
            moment = self._ts(incurred_at, "incurred_at", default_now=True)
            fee_id = self._record_fee(
                connection, shipment_id=shipment_id, package_ids=ids, fee_type=fee_type_t,
                amount=amount_f, currency=str(currency).strip() or "CNY",
                responsible_party=party["actor_id"], incurred_at=moment,
            )
            self._audit(connection, actor_id=actor["actor_id"], action="fee.recorded",
                        resource_type="fee", resource_id=fee_id,
                        detail={"shipment_id": shipment_id, "fee_type": fee_type_t,
                                "amount": amount_f, "currency": currency,
                                "responsible_party": party["actor_id"], "incurred_at": moment})
            data = {"fee_id": fee_id, "shipment_id": shipment_id, "fee_type": fee_type_t,
                    "amount": amount_f, "currency": currency,
                    "responsible_party": party["actor_id"], "incurred_at": moment}
            return "fee", fee_id, data

        return self._mutate(request_id=request_id, actor_id=actor_id, action="record_fee",
                            payload=payload, work=work)

    # ------------------------------------------------------------------ 状态推导

    def _decision_scope(self, connection, shipment_id: str, decision) -> set[str]:
        """决定显式覆盖的包装沿拆分血缘扩展到现存叶包装。

        注意只沿拆分边传播：放行 R1 不会自动放行由 R1 与 R2 合并而成的
        包装；合并件各来源的监管状态分别追踪。
        """

        direct = set(json.loads(decision["package_ids_json"]))
        edges = self._split_edges(connection, shipment_id)
        descendants = self._descendants(edges, direct)
        return direct | descendants

    def _root_ancestors(self, edges: dict[str, set[str]], package_id: str) -> set[str]:
        roots: set[str] = set()
        stack = [package_id]
        seen = {package_id}
        while stack:
            node = stack.pop()
            sources = edges.get(node)
            if not sources:
                roots.add(node)
            else:
                for source in sources:
                    if source not in seen:
                        seen.add(source)
                        stack.append(source)
        return roots

    def _effective_decisions(self, connection, shipment_id: str, at: str):
        rows = connection.execute(
            "SELECT * FROM cargo_decisions WHERE shipment_id=? AND effective_at<=? ORDER BY seq",
            (shipment_id, at),
        ).fetchall()
        return rows

    def _package_regulatory_view(self, connection, shipment_id: str, package_id: str,
                                 at: str, edges: dict[str, set[str]]) -> dict[str, Any]:
        """根据生效决定链实时推导单个包装的监管状态。

        覆盖规则：决定只作用于显式覆盖的包装，并沿拆/合血缘向下传播到现存
        叶包装；不向兄弟分支扩散。合并件上不同来源的放行/扣留状态分别追踪，
        任一来源未放行，整件不得离场。
        """

        roots = self._root_ancestors(edges, package_id)
        # 祖先链上的节点（含自身）被决定覆盖时，本包装视为被该决定覆盖
        path = {package_id} | roots
        decisions = self._effective_decisions(connection, shipment_id, at)
        relevant: list[dict[str, Any]] = []
        for decision in decisions:
            scope = self._decision_scope(connection, shipment_id, decision)
            if not (scope & path):
                continue
            relevant.append({"decision_id": decision["decision_id"], "kind": decision["kind"],
                             "effective_at": decision["effective_at"], "seq": decision["seq"],
                             "scope": sorted(scope)})

        # 按血缘来源分别判定放行：直接覆盖现存包装的放行覆盖全部来源，
        # 否则要求每个来源都各自获得放行
        root_states: dict[str, str | None] = {root: None for root in roots}
        last_general_per_root: dict[str, dict[str, Any]] = {}
        for entry in relevant:
            if entry["kind"] not in (DECISION_RELEASE, DECISION_HOLD, DECISION_RETURN):
                continue
            scope_set = set(entry["scope"])
            covers_all = package_id in scope_set
            for root in roots:
                if covers_all or root in scope_set or package_id in scope_set:
                    previous = last_general_per_root.get(root)
                    if previous is None or entry["seq"] > previous["seq"]:
                        last_general_per_root[root] = entry
                        root_states[root] = entry["kind"]
        released = bool(last_general_per_root) and all(
            root_states.get(root) == DECISION_RELEASE for root in roots
        )
        held = any(state == DECISION_HOLD for state in root_states.values())
        return_ordered = any(state == DECISION_RETURN for state in root_states.values())

        # 补件/查验待办按来源根追踪：放行/退运可以终结待办，扣留不能；
        # 已经提交补件或已核销查验也不再计入待办。
        supplement_pending = False
        supplement_decision_id: str | None = None
        for root in roots:
            latest_supp = None
            latest_term = None
            for entry in relevant:
                scope_set = set(entry["scope"])
                covers_all = package_id in scope_set
                touches_root = covers_all or root in scope_set
                if entry["kind"] == DECISION_REQUEST_SUPPLEMENT and touches_root:
                    if latest_supp is None or entry["seq"] > latest_supp["seq"]:
                        latest_supp = entry
                if entry["kind"] in (DECISION_RELEASE, DECISION_RETURN) and touches_root:
                    if latest_term is None or entry["seq"] > latest_term["seq"]:
                        latest_term = entry
            if latest_supp and (latest_term is None or latest_supp["seq"] > latest_term["seq"]):
                submitted = connection.execute(
                    "SELECT 1 FROM cargo_supplements WHERE decision_id=?",
                    (latest_supp["decision_id"],),
                ).fetchone()
                if submitted is None:
                    supplement_pending = True
                    supplement_decision_id = latest_supp["decision_id"]
                    break

        inspection_pending = False
        inspect_pending_id: str | None = None
        for entry in reversed(relevant):
            if entry["kind"] != DECISION_INSPECT:
                continue
            cleared = connection.execute(
                "SELECT 1 FROM cargo_inspection_clears WHERE decision_id=?",
                (entry["decision_id"],),
            ).fetchone()
            if cleared is not None:
                continue
            # 其后若出现直接覆盖现存包装的放行/退运，查验待办消解
            later_terminal = any(
                other["seq"] > entry["seq"]
                and other["kind"] in (DECISION_RELEASE, DECISION_RETURN)
                and package_id in set(other["scope"])
                for other in relevant
            )
            if not later_terminal:
                inspection_pending = True
                inspect_pending_id = entry["decision_id"]
            break
        blocking = []
        for entry in relevant:
            if entry["kind"] == DECISION_RELEASE:
                continue
            if entry["kind"] == DECISION_REQUEST_SUPPLEMENT and \
                    entry["decision_id"] != supplement_decision_id:
                continue
            if entry["kind"] == DECISION_INSPECT and entry["decision_id"] != inspect_pending_id:
                continue
            blocking.append({"decision_id": entry["decision_id"], "kind": entry["kind"],
                             "seq": entry["seq"]})
        return {"decisions": relevant, "released": released, "held": held,
                "return_ordered": return_ordered, "supplement_pending": supplement_pending,
                "supplement_decision_id": supplement_decision_id,
                "inspection_pending": inspection_pending,
                "inspection_decision_id": inspect_pending_id,
                "blocking_decisions": blocking,
                "root_states": root_states}

    def _departure_blockers(self, connection, shipment_id: str, package_ids: list[str],
                            at: str) -> list[dict[str, Any]]:
        packages = self._packages(connection, shipment_id)
        edges = self._lineage_edges(connection, shipment_id)
        blockers: list[dict[str, Any]] = []
        for pid in package_ids:
            row = packages[pid]
            if row["stage"] in FINAL_STAGES:
                blockers.append({"package_id": pid, "reason": "returned"})
                continue
            if not row["arrived_at"]:
                blockers.append({"package_id": pid, "reason": "not_arrived"})
                continue
            view = self._package_regulatory_view(connection, shipment_id, pid, at, edges)
            if view["return_ordered"]:
                blockers.append({"package_id": pid, "reason": "return_ordered",
                                 "decision_id": view["decisions"][-1]["decision_id"] if view["decisions"] else None})
            elif view["held"]:
                latest = next((d for d in reversed(view["decisions"]) if d["kind"] == DECISION_HOLD), None)
                blockers.append({"package_id": pid, "reason": "held",
                                 "decision_id": latest["decision_id"] if latest else None})
            elif view["supplement_pending"]:
                blockers.append({"package_id": pid, "reason": "supplement_requested"})
            elif view["inspection_pending"]:
                blockers.append({"package_id": pid, "reason": "inspection_required"})
            elif not view["released"]:
                blockers.append({"package_id": pid, "reason": "awaiting_release"})
        return blockers

    def _package_view(self, connection, shipment_id: str, row, at: str,
                      edges: dict[str, set[str]]) -> dict[str, Any]:
        pid = row["package_id"]
        children: set[str] = set()
        for child, sources in edges.items():
            if pid in sources:
                children.add(child)
        live = not children and row["stage"] not in FINAL_STAGES and not bool(connection.execute(
            "SELECT 1 FROM cargo_allocations WHERE package_id=? AND state='flown' LIMIT 1",
            (pid,),
        ).fetchone())
        placement = connection.execute(
            "SELECT * FROM cargo_placements WHERE package_id=?", (pid,)
        ).fetchone()
        allocation = connection.execute(
            "SELECT a.*, s.flight_no, s.origin AS seg_origin, s.destination AS seg_destination, "
            "s.departs_at FROM cargo_allocations a JOIN cargo_segments s ON s.segment_id=a.segment_id "
            "WHERE a.package_id=? AND a.state IN ('held','confirmed') ORDER BY a.created_at DESC LIMIT 1",
            (pid,),
        ).fetchone()
        regulatory = self._package_regulatory_view(connection, shipment_id, pid, at, edges)
        history = [
            {"custodian": event["custodian"], "since_at": event["since_at"],
             "handoff_id": event["handoff_id"]}
            for event in connection.execute(
                "SELECT * FROM cargo_custody_events WHERE package_id=? ORDER BY id", (pid,)
            )
        ]
        return {
            "package_id": pid, "batch_id": row["batch_id"], "seq": row["seq"],
            "quantity": row["quantity"], "weight": row["weight"],
            "stage": row["stage"], "live": live,
            "parent_package_id": row["parent_package_id"],
            "current_custodian": row["current_custodian"],
            "arrived_at": row["arrived_at"], "inspected_at": row["inspected_at"],
            "departed_at": row["departed_at"], "returned_at": row["returned_at"],
            "placement": None if placement is None else {
                "location_id": placement["location_id"], "lease_id": placement["lease_id"],
                "placed_at": placement["placed_at"]},
            "allocation": None if allocation is None else {
                "segment_id": allocation["segment_id"], "flight_no": allocation["flight_no"],
                "origin": allocation["seg_origin"], "destination": allocation["seg_destination"],
                "departs_at": allocation["departs_at"], "state": allocation["state"]},
            "regulatory": regulatory,
            "custody_history": history,
        }

    def shipment_status(self, *, actor_id: str, shipment_id: str,
                        include_sensitive: bool = False) -> dict[str, Any]:
        """异常席与各方共用的全链路在途视图（读事务，服务重启后同样准确）。"""

        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            return self._shipment_status(connection, actor, shipment_id,
                                         include_sensitive=include_sensitive)

    def _shipment_status(self, connection, actor, shipment_id: str,
                         include_sensitive: bool = False) -> dict[str, Any]:
        shipment = self._shipment(connection, shipment_id)
        if actor["role"] != "admin":
            row = connection.execute(
                "SELECT 1 FROM cargo_duties WHERE shipment_id=? AND actor_id=?",
                (shipment_id, actor["actor_id"]),
            ).fetchone()
            if row is None:
                raise PermissionDenied("当前操作者与该票货物没有职责关联")
        at = self._now()
        edges = self._lineage_edges(connection, shipment_id)
        packages = self._packages(connection, shipment_id)
        batches = []
        for batch in connection.execute(
            "SELECT * FROM cargo_batches WHERE shipment_id=? ORDER BY created_at, batch_id",
            (shipment_id,),
        ):
            members = [self._package_view(connection, shipment_id, packages[pid], at, edges)
                       for pid in sorted(packages)
                       if packages[pid]["batch_id"] == batch["batch_id"]]
            live_qty = sum(m["quantity"] for m in members if m["live"])
            batches.append({"batch_id": batch["batch_id"],
                            "parent_batch_id": batch["parent_batch_id"],
                            "status": batch["status"], "created_at": batch["created_at"],
                            "packages": members,
                            "live_package_count": sum(1 for m in members if m["live"]),
                            "live_quantity": live_qty})
        declarations = []
        for d in connection.execute(
            "SELECT * FROM cargo_declarations WHERE shipment_id=? ORDER BY version_no",
            (shipment_id,),
        ):
            sensitive = bool(d["sensitive"])
            item = {"declaration_id": d["declaration_id"], "version_no": d["version_no"],
                    "scope": json.loads(d["scope_json"]), "status": d["status"],
                    "sensitive": sensitive, "submitted_by": d["submitted_by"],
                    "created_at": d["created_at"], "effective_at": d["effective_at"],
                    "supersedes_id": d["supersedes_id"], "payload_hash": d["payload_hash"]}
            if not sensitive:
                item["payload"] = json.loads(d["payload_json"])
            elif include_sensitive:
                self._require_customs_read(connection, actor, shipment_id)
                item["payload"] = json.loads(d["payload_json"])
            else:
                item["payload"] = None
            declarations.append(item)
        leases = [dict(row) for row in connection.execute(
            "SELECT lease_id,location_id,quantity,starts_at,ends_at,status FROM cargo_leases "
            "WHERE shipment_id=? ORDER BY created_at", (shipment_id,))]
        allocations = []
        for a in connection.execute(
            "SELECT a.allocation_id,a.segment_id,s.flight_no,a.package_id,a.quantity,a.state,"
            "a.created_at,s.departs_at FROM cargo_allocations a "
            "JOIN cargo_segments s ON s.segment_id=a.segment_id "
            "JOIN cargo_packages p ON p.package_id=a.package_id "
            "WHERE p.shipment_id=? ORDER BY s.departs_at,a.created_at", (shipment_id,)):
            allocations.append(dict(a))
        fees = [dict(row) | {"package_ids": json.loads(row["package_ids_json"])}
                for row in connection.execute(
                    "SELECT fee_id,package_ids_json,fee_type,amount,currency,responsible_party,"
                    "ref_type,ref_id,incurred_at FROM cargo_fees WHERE shipment_id=? "
                    "ORDER BY incurred_at,fee_id", (shipment_id,))]
        for fee in fees:
            fee.pop("package_ids_json", None)
        handoffs = [{"handoff_id": row["handoff_id"], "kind": row["kind"],
                     "from_actor": row["from_actor"], "to_actor": row["to_actor"],
                     "package_ids": json.loads(row["package_ids_json"]),
                     "location": row["location"], "occurred_at": row["occurred_at"]}
                    for row in connection.execute(
                        "SELECT * FROM cargo_handoffs WHERE shipment_id=? ORDER BY occurred_at",
                        (shipment_id,))]
        live_packages = [view for batch in batches for view in batch["packages"] if view["live"]]
        segment_capacity = []
        for seg in connection.execute(
            "SELECT s.segment_id,s.flight_no,s.origin,s.destination,s.departs_at,"
            "s.capacity_qty,s.capacity_weight,s.status, "
            "COALESCE(SUM(CASE WHEN a.state IN ('held','confirmed') THEN a.quantity END),0) AS locked_qty, "
            "COALESCE(SUM(CASE WHEN a.state IN ('held','confirmed') THEN p.weight END),0) AS locked_weight "
            "FROM cargo_segments s "
            "LEFT JOIN cargo_allocations a ON a.segment_id=s.segment_id "
            "LEFT JOIN cargo_packages p ON p.package_id=a.package_id "
            "GROUP BY s.segment_id ORDER BY s.departs_at",
        ):
            segment_capacity.append({
                "segment_id": seg["segment_id"], "flight_no": seg["flight_no"],
                "origin": seg["origin"], "destination": seg["destination"],
                "departs_at": seg["departs_at"], "status": seg["status"],
                "capacity_qty": seg["capacity_qty"], "capacity_weight": seg["capacity_weight"],
                "locked_qty": seg["locked_qty"], "locked_weight": seg["locked_weight"],
                "remaining_qty": seg["capacity_qty"] - seg["locked_qty"]})
        return {
            "shipment_id": shipment_id,
            "master_waybill": shipment["master_waybill"],
            "carrier_actor_id": shipment["carrier_actor_id"],
            "origin": shipment["origin"], "destination": shipment["destination"],
            "promised_delivery_at": shipment["promised_delivery_at"],
            "status": shipment["status"], "as_of": at,
            "live_package_count": len(live_packages),
            "live_quantity": sum(p["quantity"] for p in live_packages),
            "batches": batches, "declarations": declarations,
            "leases": leases, "allocations": allocations,
            "segment_capacity": segment_capacity,
            "fees": fees, "handoffs": handoffs,
            "todo": self._todo(connection, shipment_id, live_packages, at),
        }

    def _todo(self, connection, shipment_id: str, live_packages: list[dict[str, Any]],
              at: str) -> list[dict[str, Any]]:
        """按真实业务顺序给出待办。"""

        todo: list[dict[str, Any]] = []
        seq = 0
        for view in live_packages:
            if view["stage"] in INACTIVE_STAGES:
                continue
            pid = view["package_id"]
            reg = view["regulatory"]
            chain: list[tuple[str, str | None]] = []
            if not view["arrived_at"]:
                chain.append(("arrive", None))
            if reg["return_ordered"]:
                chain.append(("return_cargo", None))
            else:
                if reg["inspection_pending"]:
                    chain.append(("customs_inspection", "查验决定待执行/核销"))
                if reg["supplement_pending"]:
                    chain.append(("submit_supplement", "海关要求补件"))
                if reg["held"]:
                    chain.append(("await_customs_release", "货物被扣留"))
                elif not reg["released"]:
                    chain.append(("await_customs_release", "尚未取得放行"))
                alloc = view["allocation"]
                if reg["released"] and alloc is None:
                    chain.append(("allocate_segment", "需要重新配舱"))
                elif alloc is not None and alloc["state"] == "held" and reg["released"]:
                    chain.append(("confirm_allocation", None))
                if alloc is not None and alloc["state"] == "confirmed":
                    chain.append(("depart", None))
            for action, note in chain:
                seq += 1
                todo.append({"seq": seq, "package_id": pid, "action": action, "note": note,
                             "custodian": view["current_custodian"],
                             "segment_id": view["allocation"]["segment_id"] if view["allocation"] else None})
        todo.sort(key=lambda item: (item["package_id"], item["seq"]))
        for index, item in enumerate(todo, start=1):
            item["seq"] = index
        return todo

    # ------------------------------------------------------------------ 延误牵连与恢复路线

    def delay_impact(self, *, actor_id: str, shipment_id: str, at: str | None = None,
                     reason: str | None = None) -> dict[str, Any]:
        """计算一次延误牵连的包装、航段、费用与承诺，并给出不扩大冻结范围的恢复路线。"""

        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            shipment = self._shipment(connection, shipment_id)
            if actor["role"] != "admin":
                self._require_duty(connection, actor, shipment_id,
                                   DUTY_EXCEPTION, DUTY_AIRLINE, DUTY_CUSTOMS)
            moment = self._ts(at, "at", default_now=True)
            status = self._shipment_status(connection, actor, shipment_id)
            live = [view for batch in status["batches"] for view in batch["packages"] if view["live"]]

            affected_packages: list[dict[str, Any]] = []
            locked_segments: dict[str, dict[str, Any]] = {}
            for view in live:
                reasons: list[str] = []
                reg = view["regulatory"]
                if reg["held"]:
                    reasons.append("held")
                if reg["supplement_pending"]:
                    reasons.append("supplement_requested")
                if reg["inspection_pending"]:
                    reasons.append("inspection_required")
                if reg["return_ordered"]:
                    reasons.append("return_ordered")
                alloc = view["allocation"]
                if alloc and alloc["departs_at"] >= moment:
                    reasons.append("downstream_segment_locked")
                    entry = locked_segments.setdefault(alloc["segment_id"], {
                        "segment_id": alloc["segment_id"], "flight_no": alloc["flight_no"],
                        "departs_at": alloc["departs_at"], "package_ids": [],
                        "quantity": 0.0, "state": alloc["state"]})
                    entry["package_ids"].append(view["package_id"])
                    entry["quantity"] += view["quantity"]
                if reasons:
                    affected_packages.append({"package_id": view["package_id"],
                                              "stage": view["stage"],
                                              "quantity": view["quantity"],
                                              "custodian": view["current_custodian"],
                                              "segment_id": alloc["segment_id"] if alloc else None,
                                              "reasons": reasons})

            fees_by_party: dict[str, float] = {}
            for fee in status["fees"]:
                fees_by_party[fee["responsible_party"]] = \
                    round(fees_by_party.get(fee["responsible_party"], 0.0) + fee["amount"], 2)
            locked_leases = [lease for lease in status["leases"] if lease["status"] == "locked"]

            # 承诺可行性：最晚锁定航段起飞时间与承诺送达时间比较
            latest_depart = None
            for entry in locked_segments.values():
                if latest_depart is None or entry["departs_at"] > latest_depart:
                    latest_depart = entry["departs_at"]
            commitment = {"promised_delivery_at": shipment["promised_delivery_at"],
                          "latest_locked_departure_at": latest_depart,
                          "at_risk": bool(shipment["promised_delivery_at"] and latest_depart
                                          and latest_depart > shipment["promised_delivery_at"])}

            recovery = self._recovery_plan(connection, shipment_id, live, moment)
            return {"shipment_id": shipment_id, "as_of": moment, "reason": reason,
                    "affected_packages": affected_packages,
                    "affected_package_count": len(affected_packages),
                    "locked_segments": sorted(locked_segments.values(),
                                             key=lambda item: item["departs_at"]),
                    "locked_leases": locked_leases,
                    "fees_by_party": fees_by_party,
                    "fees_total": round(sum(fees_by_party.values()), 2),
                    "commitment": commitment,
                    "todo": status["todo"],
                    "recovery_plan": recovery}

    def _recovery_plan(self, connection, shipment_id: str,
                       live: list[dict[str, Any]], at: str) -> list[dict[str, Any]]:
        """恢复路线只处理被覆盖的包装，无争议货物按原路径继续。"""

        plan: list[dict[str, Any]] = []
        released_stranded: list[str] = []
        for view in live:
            reg = view["regulatory"]
            pid = view["package_id"]
            if reg["return_ordered"]:
                plan.append({"package_id": pid, "action": "execute_return",
                             "scope": "covered_only",
                             "note": "按生效退运决定退运该包装，不影响同票其他包装"})
                continue
            if reg["inspection_pending"]:
                plan.append({"package_id": pid, "action": "perform_inspection",
                             "scope": "covered_only"})
            if reg["supplement_pending"]:
                plan.append({"package_id": pid, "action": "submit_supplement",
                             "scope": "covered_only",
                             "note": "补件后等待海关针对该包装的新决定"})
            if reg["held"]:
                plan.append({"package_id": pid, "action": "await_release",
                             "scope": "covered_only",
                             "note": "维持扣留，不冻结同票已放行包装"})
                continue
            if reg["released"] and view["allocation"] is None:
                released_stranded.append(pid)
        for pid in released_stranded:
            candidates = []
            for segment in connection.execute(
                "SELECT s.*, COALESCE(SUM(a.quantity),0) AS used_qty, "
                "COALESCE(SUM(a_p.weight),0) AS used_weight FROM cargo_segments s "
                "LEFT JOIN cargo_allocations a ON a.segment_id=s.segment_id "
                "AND a.state IN ('held','confirmed') "
                "LEFT JOIN cargo_packages a_p ON a_p.package_id=a.package_id "
                "WHERE s.status='open' AND s.departs_at>=? GROUP BY s.segment_id ORDER BY s.departs_at",
                (at,),
            ):
                candidates.append({"segment_id": segment["segment_id"],
                                   "flight_no": segment["flight_no"],
                                   "departs_at": segment["departs_at"],
                                   "remaining_qty": segment["capacity_qty"] - segment["used_qty"]})
            plan.append({"package_id": pid, "action": "rebook", "scope": "covered_only",
                         "note": "已放行但无生效航段，按富余容量改签到最早可行航段；旧占位与"
                                 "已发生费用保持原样，改签产生的费用另行归属",
                         "candidate_segments": candidates[:5]})
        plan.sort(key=lambda item: item["package_id"])
        return plan

    def list_decisions(self, shipment_id: str) -> list[dict[str, Any]]:
        with self.database.transaction() as connection:
            self._shipment(connection, shipment_id)
            result = []
            for row in connection.execute(
                "SELECT * FROM cargo_decisions WHERE shipment_id=? ORDER BY seq", (shipment_id,)
            ):
                result.append({"decision_id": row["decision_id"], "kind": row["kind"],
                               "package_ids": json.loads(row["package_ids_json"]),
                               "declaration_id": row["declaration_id"], "reason": row["reason"],
                               "decided_by": row["decided_by"],
                               "effective_at": row["effective_at"],
                               "recorded_at": row["recorded_at"], "seq": row["seq"],
                               "late": row["effective_at"] < row["recorded_at"]})
            return result

    def list_fees(self, shipment_id: str) -> list[dict[str, Any]]:
        with self.database.transaction() as connection:
            self._shipment(connection, shipment_id)
            return [{"fee_id": row["fee_id"], "fee_type": row["fee_type"],
                     "amount": row["amount"], "currency": row["currency"],
                     "responsible_party": row["responsible_party"],
                     "package_ids": json.loads(row["package_ids_json"]),
                     "ref_type": row["ref_type"], "ref_id": row["ref_id"],
                     "incurred_at": row["incurred_at"]}
                    for row in connection.execute(
                        "SELECT * FROM cargo_fees WHERE shipment_id=? ORDER BY incurred_at",
                        (shipment_id,))]

    def get_declaration(self, *, actor_id: str, declaration_id: str) -> dict[str, Any]:
        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            row = connection.execute(
                "SELECT * FROM cargo_declarations WHERE declaration_id=?", (declaration_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("申报不存在")
            shipment_id = row["shipment_id"]
            data = {"declaration_id": declaration_id, "shipment_id": shipment_id,
                    "version_no": row["version_no"], "status": row["status"],
                    "sensitive": bool(row["sensitive"]),
                    "scope": json.loads(row["scope_json"]),
                    "submitted_by": row["submitted_by"], "created_at": row["created_at"],
                    "effective_at": row["effective_at"], "supersedes_id": row["supersedes_id"]}
            if row["sensitive"]:
                self._require_customs_read(connection, actor, shipment_id)
            elif actor["role"] != "admin":
                duty = connection.execute(
                    "SELECT 1 FROM cargo_duties WHERE shipment_id=? AND actor_id=?",
                    (shipment_id, actor_id),
                ).fetchone()
                if duty is None:
                    raise PermissionDenied("当前操作者与该票货物没有职责关联")
            data["payload"] = json.loads(row["payload_json"])
            return data
