"""封装 SQLite 连接、建表和事务边界。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS organizations (
    organization_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actors (
    actor_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sites (
    site_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    name TEXT NOT NULL,
    timezone_name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS domain_records (
    record_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    category TEXT NOT NULL,
    external_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL,
    UNIQUE(site_id, category, external_key)
);
CREATE TABLE IF NOT EXISTS request_receipts (
    request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL
);
-- 航空物流异常协同领域表
CREATE TABLE IF NOT EXISTS cargo_shipments (
    shipment_id TEXT PRIMARY KEY,
    master_waybill TEXT NOT NULL UNIQUE,
    carrier_actor_id TEXT NOT NULL,
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    promised_delivery_at TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cargo_batches (
    batch_id TEXT PRIMARY KEY,
    shipment_id TEXT NOT NULL REFERENCES cargo_shipments(shipment_id),
    parent_batch_id TEXT REFERENCES cargo_batches(batch_id),
    status TEXT NOT NULL DEFAULT 'active',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    closed_at TEXT
);
CREATE TABLE IF NOT EXISTS cargo_packages (
    package_id TEXT PRIMARY KEY,
    shipment_id TEXT NOT NULL REFERENCES cargo_shipments(shipment_id),
    batch_id TEXT NOT NULL REFERENCES cargo_batches(batch_id),
    seq INTEGER NOT NULL,
    quantity REAL NOT NULL CHECK(quantity > 0),
    weight REAL NOT NULL CHECK(weight >= 0),
    stage TEXT NOT NULL DEFAULT 'registered',
    current_custodian TEXT,
    parent_package_id TEXT REFERENCES cargo_packages(package_id),
    arrived_at TEXT,
    inspected_at TEXT,
    handed_over_at TEXT,
    departed_at TEXT,
    returned_at TEXT,
    UNIQUE(shipment_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_cargo_packages_shipment ON cargo_packages(shipment_id);
CREATE TABLE IF NOT EXISTS cargo_merge_sources (
    child_package_id TEXT NOT NULL,
    source_package_id TEXT NOT NULL,
    PRIMARY KEY (child_package_id, source_package_id)
);
CREATE TABLE IF NOT EXISTS cargo_batch_moves (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id TEXT NOT NULL,
    shipment_id TEXT NOT NULL,
    from_batch_id TEXT,
    to_batch_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    moved_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cargo_declarations (
    declaration_id TEXT PRIMARY KEY,
    shipment_id TEXT NOT NULL REFERENCES cargo_shipments(shipment_id),
    version_no INTEGER NOT NULL CHECK(version_no >= 1),
    scope_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    sensitive INTEGER NOT NULL CHECK(sensitive IN (0, 1)),
    supersedes_id TEXT,
    status TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    effective_at TEXT NOT NULL,
    UNIQUE(shipment_id, version_no)
);
CREATE TABLE IF NOT EXISTS cargo_decisions (
    decision_id TEXT PRIMARY KEY,
    shipment_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    package_ids_json TEXT NOT NULL,
    declaration_id TEXT,
    reason TEXT,
    decided_by TEXT NOT NULL,
    effective_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    seq INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cargo_decisions_shipment ON cargo_decisions(shipment_id);
CREATE TABLE IF NOT EXISTS cargo_supplements (
    decision_id TEXT PRIMARY KEY,
    declaration_id TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cargo_inspection_clears (
    decision_id TEXT PRIMARY KEY,
    cleared_by TEXT NOT NULL,
    cleared_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cargo_duties (
    shipment_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    duty TEXT NOT NULL,
    granted_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (shipment_id, actor_id, duty)
);
CREATE TABLE IF NOT EXISTS cargo_locations (
    location_id TEXT PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    capacity_qty REAL NOT NULL CHECK(capacity_qty >= 0),
    status TEXT NOT NULL DEFAULT 'active'
);
CREATE TABLE IF NOT EXISTS cargo_leases (
    lease_id TEXT PRIMARY KEY,
    location_id TEXT NOT NULL REFERENCES cargo_locations(location_id),
    shipment_id TEXT NOT NULL,
    quantity REAL NOT NULL CHECK(quantity > 0),
    starts_at TEXT,
    ends_at TEXT,
    status TEXT NOT NULL DEFAULT 'locked',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cargo_placements (
    package_id TEXT PRIMARY KEY,
    location_id TEXT NOT NULL,
    lease_id TEXT,
    placed_by TEXT NOT NULL,
    placed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cargo_scans (
    scan_token TEXT PRIMARY KEY,
    package_id TEXT NOT NULL,
    action TEXT NOT NULL,
    result_ref TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cargo_segments (
    segment_id TEXT PRIMARY KEY,
    flight_no TEXT NOT NULL,
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    departs_at TEXT NOT NULL,
    capacity_qty REAL NOT NULL CHECK(capacity_qty > 0),
    capacity_weight REAL NOT NULL CHECK(capacity_weight >= 0),
    status TEXT NOT NULL DEFAULT 'open',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cargo_allocations (
    allocation_id TEXT PRIMARY KEY,
    segment_id TEXT NOT NULL REFERENCES cargo_segments(segment_id),
    package_id TEXT NOT NULL,
    quantity REAL NOT NULL CHECK(quantity > 0),
    state TEXT NOT NULL DEFAULT 'held',
    request_id TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cargo_alloc_segment ON cargo_allocations(segment_id, state);
-- 同一航段上同一包装只能有一个生效占位
CREATE UNIQUE INDEX IF NOT EXISTS cargo_alloc_segment_package_uq
    ON cargo_allocations(segment_id, package_id) WHERE state IN ('held', 'confirmed');
-- 同一包装在全航段网络中只能有一个生效占位，改签必须先作废旧占位
CREATE UNIQUE INDEX IF NOT EXISTS cargo_alloc_package_active_uq
    ON cargo_allocations(package_id) WHERE state IN ('held', 'confirmed');
CREATE TABLE IF NOT EXISTS cargo_handoffs (
    handoff_id TEXT PRIMARY KEY,
    shipment_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    from_actor TEXT NOT NULL,
    to_actor TEXT NOT NULL,
    package_ids_json TEXT NOT NULL,
    location TEXT,
    occurred_at TEXT NOT NULL,
    completed_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cargo_custody_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id TEXT NOT NULL,
    custodian TEXT NOT NULL,
    handoff_id TEXT,
    since_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cargo_custody_package ON cargo_custody_events(package_id, id);
CREATE TABLE IF NOT EXISTS cargo_fees (
    fee_id TEXT PRIMARY KEY,
    shipment_id TEXT NOT NULL,
    package_ids_json TEXT NOT NULL,
    fee_type TEXT NOT NULL,
    amount REAL NOT NULL CHECK(amount >= 0),
    currency TEXT NOT NULL,
    responsible_party TEXT NOT NULL,
    ref_type TEXT,
    ref_id TEXT,
    incurred_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cargo_fees_shipment ON cargo_fees(shipment_id);
"""


class Database:
    """管理 SQLite 数据库并为服务提供短事务。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """在异常时回滚，在成功时提交。"""

        self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield self.connection
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def close(self) -> None:
        """关闭底层连接。"""

        self.connection.close()
