"""提供主体、场所、领域资料和审计查询能力。"""

from __future__ import annotations

import json
import re
import uuid
from typing import Any, Callable

from .audit import append_event, canonical_json, digest, verify_chain
from .clock import Clock, SystemClock
from .domain import is_allowed_category
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .models import Actor, DomainRecord, Site, WriteReceipt
from .storage import Database


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{1,63}$")
ROLES = frozenset({"admin", "operator", "reviewer", "auditor"})


class DomainService:
    """协调权限、幂等、事务和审计规则。"""

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    def _now(self) -> str:
        return self.clock.now().isoformat().replace("+00:00", "Z")

    def _identifier(self, value: str, field: str) -> str:
        value = str(value).strip()
        if not IDENTIFIER.fullmatch(value):
            raise ValidationError(f"{field} 格式无效")
        return value

    def _text(self, value: str, field: str, limit: int = 200) -> str:
        value = str(value).strip()
        if not value or len(value) > limit:
            raise ValidationError(f"{field} 不能为空且不能超过 {limit} 个字符")
        return value

    def _actor(self, connection, actor_id: str) -> Actor:
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        actor = Actor(row["actor_id"], row["display_name"], row["role"], row["organization_id"], bool(row["active"]))
        if not actor.active:
            raise PermissionDenied("操作者已停用")
        return actor

    def _require(self, actor: Actor, *roles: str) -> None:
        if actor.role not in roles:
            raise PermissionDenied("当前角色不能执行该动作")

    def _idempotent(self, connection, *, request_id: str, action: str,
                    payload: dict[str, Any], create: Callable[[], tuple[str, str, dict[str, Any]]]) -> WriteReceipt:
        request_id = self._identifier(request_id, "request_id")
        payload_hash = digest(payload)
        row = connection.execute("SELECT * FROM request_receipts WHERE request_id=?", (request_id,)).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            return WriteReceipt(request_id, row["resource_type"], row["resource_id"], True)
        resource_type, resource_id, response = create()
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,response_json,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, resource_type, resource_id, canonical_json(response), self._now()),
        )
        return WriteReceipt(request_id, resource_type, resource_id, False)

    def register_organization(self, *, request_id: str, actor_id: str,
                              organization_id: str, name: str) -> WriteReceipt:
        payload = {"actor_id": actor_id, "organization_id": organization_id, "name": name}
        with self.database.transaction(immediate=True) as connection:
            existing_actors = connection.execute("SELECT COUNT(*) AS count FROM actors").fetchone()["count"]
            if existing_actors:
                actor = self._actor(connection, actor_id)
                self._require(actor, "admin")
            elif actor_id != "bootstrap":
                raise PermissionDenied("首次建档必须使用 bootstrap")
            organization_id = self._identifier(organization_id, "organization_id")
            name = self._text(name, "name")

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO organizations(organization_id,name,created_at) VALUES(?,?,?)",
                        (organization_id, name, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("组织编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="organization.registered",
                             resource_type="organization", resource_id=organization_id,
                             detail={"name": name}, occurred_at=self._now())
                return "organization", organization_id, {"organization_id": organization_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="register_organization", payload=payload, create=create)

    def register_actor(self, *, request_id: str, actor_id: str, new_actor_id: str,
                       display_name: str, role: str, organization_id: str) -> WriteReceipt:
        payload = {"actor_id": actor_id, "new_actor_id": new_actor_id, "display_name": display_name,
                   "role": role, "organization_id": organization_id}
        with self.database.transaction(immediate=True) as connection:
            count = connection.execute("SELECT COUNT(*) AS count FROM actors").fetchone()["count"]
            if count:
                actor = self._actor(connection, actor_id)
                self._require(actor, "admin")
            elif actor_id != "bootstrap":
                raise PermissionDenied("首位管理员必须由 bootstrap 创建")
            new_actor_id = self._identifier(new_actor_id, "new_actor_id")
            display_name = self._text(display_name, "display_name")
            if role not in ROLES:
                raise ValidationError("role 不在允许范围内")
            if connection.execute("SELECT 1 FROM organizations WHERE organization_id=?", (organization_id,)).fetchone() is None:
                raise NotFoundError("组织不存在")

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO actors(actor_id,display_name,role,organization_id,active,created_at) VALUES(?,?,?,?,1,?)",
                        (new_actor_id, display_name, role, organization_id, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("操作者编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="actor.registered",
                             resource_type="actor", resource_id=new_actor_id,
                             detail={"display_name": display_name, "role": role, "organization_id": organization_id},
                             occurred_at=self._now())
                return "actor", new_actor_id, {"actor_id": new_actor_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="register_actor", payload=payload, create=create)

    def register_site(self, *, request_id: str, actor_id: str, site_id: str,
                      organization_id: str, name: str, timezone_name: str) -> WriteReceipt:
        payload = {"actor_id": actor_id, "site_id": site_id, "organization_id": organization_id,
                   "name": name, "timezone_name": timezone_name}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            if actor.organization_id != organization_id and actor.role != "admin":
                raise PermissionDenied("不能为其他组织登记场所")
            site_id = self._identifier(site_id, "site_id")
            name = self._text(name, "name")
            timezone_name = self._text(timezone_name, "timezone_name", 80)

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO sites(site_id,organization_id,name,timezone_name,version,created_at) VALUES(?,?,?,?,1,?)",
                        (site_id, organization_id, name, timezone_name, self._now()),
                    )
                except Exception as exc:
                    raise ConflictError("场所编号已经存在或组织无效") from exc
                append_event(connection, actor_id=actor_id, action="site.registered",
                             resource_type="site", resource_id=site_id,
                             detail={"organization_id": organization_id, "name": name, "timezone_name": timezone_name},
                             occurred_at=self._now())
                return "site", site_id, {"site_id": site_id, "version": 1}

            return self._idempotent(connection, request_id=request_id,
                                    action="register_site", payload=payload, create=create)

    def record_domain_data(self, *, request_id: str, actor_id: str, site_id: str,
                           category: str, external_key: str, data: dict[str, Any]) -> WriteReceipt:
        if not isinstance(data, dict) or not data:
            raise ValidationError("data 必须是非空对象")
        payload = {"actor_id": actor_id, "site_id": site_id, "category": category,
                   "external_key": external_key, "data": data}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator", "reviewer")
            site = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
            if site is None:
                raise NotFoundError("场所不存在")
            if actor.organization_id != site["organization_id"] and actor.role != "admin":
                raise PermissionDenied("不能写入其他组织的场所")
            if not is_allowed_category(category):
                raise ValidationError("资料类别不属于当前项目")
            external_key = self._identifier(external_key, "external_key")
            data_hash = digest(data)

            def create() -> tuple[str, str, dict[str, Any]]:
                existing = connection.execute(
                    "SELECT * FROM domain_records WHERE site_id=? AND category=? AND external_key=?",
                    (site_id, category, external_key),
                ).fetchone()
                if existing:
                    if existing["payload_hash"] != data_hash:
                        raise ConflictError("同一业务键已经登记不同内容")
                    return "domain_record", existing["record_id"], {"record_id": existing["record_id"]}
                record_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO domain_records(record_id,site_id,category,external_key,payload_json,payload_hash,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (record_id, site_id, category, external_key, canonical_json(data), data_hash, actor_id, self._now()),
                )
                append_event(connection, actor_id=actor_id, action="domain_data.recorded",
                             resource_type="domain_record", resource_id=record_id,
                             detail={"site_id": site_id, "category": category, "external_key": external_key,
                                     "payload_hash": data_hash}, occurred_at=self._now())
                return "domain_record", record_id, {"record_id": record_id}

            return self._idempotent(connection, request_id=request_id,
                                    action="record_domain_data", payload=payload, create=create)

    def get_site(self, site_id: str) -> Site:
        row = self.database.connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if row is None:
            raise NotFoundError("场所不存在")
        return Site(row["site_id"], row["organization_id"], row["name"], row["timezone_name"], row["version"])

    def list_domain_data(self, site_id: str, category: str | None = None) -> list[DomainRecord]:
        parameters: list[Any] = [site_id]
        query = "SELECT * FROM domain_records WHERE site_id=?"
        if category:
            query += " AND category=?"
            parameters.append(category)
        query += " ORDER BY created_at, record_id"
        records = []
        for row in self.database.connection.execute(query, parameters):
            records.append(DomainRecord(row["record_id"], row["site_id"], row["category"],
                                        row["external_key"], json.loads(row["payload_json"]),
                                        row["created_by"], row["created_at"]))
        return records

    def audit_events(self, after_sequence: int = 0) -> list[dict[str, Any]]:
        rows = self.database.connection.execute(
            "SELECT * FROM audit_events WHERE sequence>? ORDER BY sequence", (after_sequence,)
        ).fetchall()
        return [{"sequence": row["sequence"], "event_id": row["event_id"], "actor_id": row["actor_id"],
                 "action": row["action"], "resource_type": row["resource_type"],
                 "resource_id": row["resource_id"], "detail": json.loads(row["detail_json"]),
                 "previous_hash": row["previous_hash"], "event_hash": row["event_hash"],
                 "occurred_at": row["occurred_at"]} for row in rows]

    def verify_audit(self) -> tuple[bool, int]:
        return verify_chain(self.database.connection)
