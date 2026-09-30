"""实现可离线校验的哈希串联审计日志。"""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any


GENESIS_HASH = "0" * 64


def canonical_json(value: Any) -> str:
    """生成稳定的紧凑 JSON 文本。"""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    """计算业务载荷的稳定摘要。"""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def append_event(connection, *, actor_id: str, action: str, resource_type: str,
                 resource_id: str, detail: dict[str, Any], occurred_at: str) -> dict[str, Any]:
    """追加一个审计事件并返回可序列化结果。"""

    row = connection.execute(
        "SELECT event_hash FROM audit_events ORDER BY sequence DESC LIMIT 1"
    ).fetchone()
    previous_hash = row["event_hash"] if row else GENESIS_HASH
    event_id = uuid.uuid4().hex
    material = {
        "event_id": event_id,
        "actor_id": actor_id,
        "action": action,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "detail": detail,
        "previous_hash": previous_hash,
        "occurred_at": occurred_at,
    }
    event_hash = digest(material)
    connection.execute(
        "INSERT INTO audit_events(event_id,actor_id,action,resource_type,resource_id,detail_json,"
        "previous_hash,event_hash,occurred_at) VALUES(?,?,?,?,?,?,?,?,?)",
        (event_id, actor_id, action, resource_type, resource_id, canonical_json(detail),
         previous_hash, event_hash, occurred_at),
    )
    return {**material, "event_hash": event_hash}


def verify_chain(connection) -> tuple[bool, int]:
    """逐条验证审计哈希链。"""

    previous_hash = GENESIS_HASH
    count = 0
    for row in connection.execute("SELECT * FROM audit_events ORDER BY sequence"):
        detail = json.loads(row["detail_json"])
        material = {
            "event_id": row["event_id"], "actor_id": row["actor_id"],
            "action": row["action"], "resource_type": row["resource_type"],
            "resource_id": row["resource_id"], "detail": detail,
            "previous_hash": row["previous_hash"], "occurred_at": row["occurred_at"],
        }
        if row["previous_hash"] != previous_hash or digest(material) != row["event_hash"]:
            return False, count
        previous_hash = row["event_hash"]
        count += 1
    return True, count
