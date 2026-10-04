"""提供不依赖第三方框架的 HTTP/JSON 边界。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import DomainError, ValidationError
from .service import DomainService
from .cargo import CargoService
from .storage import Database


def _result(result) -> dict[str, Any]:
    return {"request_id": result.request_id, "resource_type": result.resource_type,
            "resource_id": result.resource_id, "replayed": result.replayed,
            "data": result.data}


def cargo_route(cargo: CargoService, method: str, path: str, body: dict[str, Any],
                query: dict[str, str], actor_id: str) -> tuple[int, dict[str, Any]] | None:
    """处理航空异常协同路由；不属于该域时返回 None。"""

    parts = [segment for segment in path.split("/") if segment]
    if not parts or parts[0] != "cargo":
        return None

    def call(name: str, **kwargs):
        receipt = getattr(cargo, name)(actor_id=actor_id, **kwargs)
        return 200 if receipt.replayed else 201, _result(receipt)

    try:
        if method == "POST":
            if path == "/cargo/shipments":
                return call("create_shipment", **body)
            if path == "/cargo/duties":
                return call("grant_duty", **body)
            if path == "/cargo/packages":
                return call("register_packages", **body)
            if path == "/cargo/packages/split":
                return call("split_packages", **body)
            if path == "/cargo/packages/merge":
                return call("merge_packages", **body)
            if path == "/cargo/arrivals":
                return call("arrive_packages", **body)
            if path == "/cargo/inspections":
                return call("inspect_packages", **body)
            if path == "/cargo/inspections/clear":
                return call("clear_inspection", **body)
            if path == "/cargo/declarations":
                return call("submit_declaration", **body)
            if path == "/cargo/supplements":
                return call("submit_supplement", **body)
            if path == "/cargo/decisions":
                return call("issue_decision", **body)
            if path == "/cargo/locations":
                return call("register_location", **body)
            if path == "/cargo/leases":
                return call("lease_space", **body)
            if path == "/cargo/placements":
                return call("place_packages", **body)
            if path == "/cargo/pickups":
                return call("pickup_packages", **body)
            if path == "/cargo/segments":
                return call("register_segment", **body)
            if path == "/cargo/allocations":
                return call("allocate_segment", **body)
            if path == "/cargo/allocations/confirm":
                return call("confirm_allocation", **body)
            if path == "/cargo/allocations/release":
                return call("release_allocation", **body)
            if path == "/cargo/segments/close":
                return call("close_segment", **body)
            if path == "/cargo/rebookings":
                return call("rebook_segment", **body)
            if path == "/cargo/departures":
                return call("depart_packages", **body)
            if path == "/cargo/handoffs":
                return call("handoff", **body)
            if path == "/cargo/returns":
                return call("return_cargo", **body)
            if path == "/cargo/fees":
                return call("record_fee", **body)
        if method == "GET":
            if len(parts) == 4 and parts[1] == "shipments" and parts[3] == "status":
                return 200, cargo.shipment_status(
                    shipment_id=parts[2],
                    include_sensitive=query.get("include_sensitive", ["false"])[0].lower() == "true",
                    actor_id=actor_id)
            if len(parts) == 4 and parts[1] == "shipments" and parts[3] == "delay-impact":
                return 200, cargo.delay_impact(shipment_id=parts[2],
                                               at=query.get("at", [None])[0],
                                               reason=query.get("reason", [None])[0],
                                               actor_id=actor_id)
            if path == "/cargo/decisions":
                shipment_id = query.get("shipment_id", [""])[0]
                if not shipment_id:
                    raise ValidationError("shipment_id 不能为空")
                return 200, {"items": cargo.list_decisions(shipment_id)}
            if path == "/cargo/fees":
                shipment_id = query.get("shipment_id", [""])[0]
                if not shipment_id:
                    raise ValidationError("shipment_id 不能为空")
                return 200, {"items": cargo.list_fees(shipment_id)}
            if len(parts) == 3 and parts[1] == "declarations":
                return 200, cargo.get_declaration(declaration_id=parts[2], actor_id=actor_id)
        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


def route(service: DomainService, method: str, path: str, body: dict[str, Any] | None,
          headers: dict[str, str] | None = None, cargo: CargoService | None = None
          ) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到领域服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    actor_id = headers.get("X-Actor-Id", "")
    query = parse_qs(parsed.query)
    if cargo is not None and parsed.path.startswith("/cargo"):
        cargo_result = cargo_route(cargo, method, parsed.path, body, query, actor_id)
        if cargo_result is not None:
            return cargo_result
    try:
        if method == "GET" and parsed.path == "/health":
            valid, count = service.verify_audit()
            return 200, {"status": "ok", "audit_valid": valid, "audit_events": count}
        if method == "POST" and parsed.path == "/organizations":
            receipt = service.register_organization(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/actors":
            receipt = service.register_actor(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/sites":
            receipt = service.register_site(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/domain-records":
            receipt = service.record_domain_data(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "GET" and parsed.path == "/domain-records":
            site_id = query.get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            category = query.get("category", [None])[0]
            return 200, {"items": [item.__dict__ for item in service.list_domain_data(site_id, category)]}
        if method == "GET" and parsed.path == "/audit-events":
            after = int(query.get("after_sequence", ["0"])[0])
            return 200, {"items": service.audit_events(after)}
        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为路由调用。"""

    service: DomainService
    cargo: CargoService

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        status, payload = route(self.service, self.command, self.path, body,
                                {"X-Actor-Id": self.headers.get("X-Actor-Id", "")},
                                cargo=self.cargo)
        self._write(status, payload)

    def _write(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    """启动本地 HTTP 服务。"""

    parser = argparse.ArgumentParser(description="启动技能赛训协作基础服务")
    parser.add_argument("--database", default="service.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    database = Database(args.database)
    Handler.service = DomainService(database)
    Handler.cargo = CargoService(database)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
