"""航空枢纽异常协同服务的 SQLite 表结构。

链路顺序：主运单 → 扫描包装 → 到港批次 → 拆分/合并 → 仓位租约/航段占位 →
申报版本 → 监管决定（只覆盖显式单元）→ 查验/补件/放行/改签/交接/退运。
所有写操作复用基础服务的幂等回执与哈希串联审计。
"""

AIR_SCHEMA = """
CREATE TABLE IF NOT EXISTS air_segments (
    segment_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    flight_no TEXT NOT NULL,
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    departs_at TEXT NOT NULL,
    arrives_at TEXT NOT NULL,
    capacity_pieces INTEGER NOT NULL CHECK(capacity_pieces > 0),
    capacity_weight REAL NOT NULL CHECK(capacity_weight > 0),
    status TEXT NOT NULL CHECK(status IN ('scheduled','delayed','cancelled','departed')),
    delay_minutes INTEGER NOT NULL DEFAULT 0 CHECK(delay_minutes >= 0),
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS air_slots (
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    slot_code TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(site_id, slot_code)
);
CREATE TABLE IF NOT EXISTS air_waybills (
    waybill_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    declared_pieces INTEGER NOT NULL CHECK(declared_pieces > 0),
    declared_weight REAL NOT NULL CHECK(declared_weight > 0),
    promised_arrival_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('registered','arrived','closed')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS air_batches (
    batch_id TEXT PRIMARY KEY,
    waybill_id TEXT NOT NULL REFERENCES air_waybills(waybill_id),
    kind TEXT NOT NULL CHECK(kind IN ('arrival','split','merge','hold','return')),
    status TEXT NOT NULL CHECK(status IN ('open','held','departed','returned','closed')),
    location TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS air_packages (
    package_id TEXT PRIMARY KEY,
    waybill_id TEXT NOT NULL REFERENCES air_waybills(waybill_id),
    batch_id TEXT REFERENCES air_batches(batch_id),
    pieces INTEGER NOT NULL CHECK(pieces > 0),
    weight REAL NOT NULL CHECK(weight > 0),
    scanned_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS air_steps (
    step_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES air_batches(batch_id),
    position INTEGER NOT NULL CHECK(position >= 0),
    step_type TEXT NOT NULL CHECK(step_type IN ('sort','store','inspect','load','handover','depart','return')),
    segment_id TEXT,
    status TEXT NOT NULL CHECK(status IN ('pending','completed','cancelled')),
    created_at TEXT NOT NULL,
    occurred_at TEXT
);
CREATE TABLE IF NOT EXISTS air_slot_leases (
    lease_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    slot_code TEXT NOT NULL,
    batch_id TEXT NOT NULL REFERENCES air_batches(batch_id),
    status TEXT NOT NULL CHECK(status IN ('active','released')),
    starts_at TEXT NOT NULL,
    ends_at TEXT,
    FOREIGN KEY(site_id, slot_code) REFERENCES air_slots(site_id, slot_code)
);
CREATE UNIQUE INDEX IF NOT EXISTS air_slot_active ON air_slot_leases(site_id, slot_code) WHERE status='active';
CREATE UNIQUE INDEX IF NOT EXISTS air_batch_active_lease ON air_slot_leases(batch_id) WHERE status='active';
CREATE TABLE IF NOT EXISTS air_bookings (
    booking_id TEXT PRIMARY KEY,
    segment_id TEXT NOT NULL REFERENCES air_segments(segment_id),
    batch_id TEXT NOT NULL REFERENCES air_batches(batch_id),
    pieces INTEGER NOT NULL CHECK(pieces >= 0),
    weight REAL NOT NULL CHECK(weight >= 0),
    status TEXT NOT NULL CHECK(status IN ('active','released','completed')),
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS air_batch_active_booking ON air_bookings(batch_id) WHERE status='active';
CREATE INDEX IF NOT EXISTS air_segment_active_bookings ON air_bookings(segment_id) WHERE status='active';
CREATE TABLE IF NOT EXISTS air_declarations (
    declaration_id TEXT PRIMARY KEY,
    waybill_id TEXT NOT NULL REFERENCES air_waybills(waybill_id),
    version INTEGER NOT NULL CHECK(version >= 1),
    status TEXT NOT NULL CHECK(status IN ('submitted','supplement_requested','cleared')),
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    UNIQUE(waybill_id, version)
);
CREATE TABLE IF NOT EXISTS air_decisions (
    decision_id TEXT PRIMARY KEY,
    waybill_id TEXT NOT NULL REFERENCES air_waybills(waybill_id),
    decision_type TEXT NOT NULL CHECK(decision_type IN ('hold','inspect','supplement','release','return')),
    effective_at TEXT NOT NULL,
    issued_by TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    note TEXT,
    effect_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS air_decision_units (
    decision_id TEXT NOT NULL REFERENCES air_decisions(decision_id),
    package_id TEXT NOT NULL REFERENCES air_packages(package_id),
    PRIMARY KEY(decision_id, package_id)
);
CREATE TABLE IF NOT EXISTS air_handovers (
    handover_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES air_batches(batch_id),
    from_role TEXT NOT NULL,
    to_role TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    recorded_by TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS air_fees (
    fee_id TEXT PRIMARY KEY,
    waybill_id TEXT NOT NULL REFERENCES air_waybills(waybill_id),
    batch_id TEXT,
    segment_id TEXT,
    category TEXT NOT NULL CHECK(category IN ('booking','rebooking','slot_lease','return')),
    amount REAL NOT NULL CHECK(amount >= 0),
    currency TEXT NOT NULL,
    incurred_at TEXT NOT NULL,
    note TEXT
);
"""
