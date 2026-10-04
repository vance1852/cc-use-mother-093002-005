"""航空枢纽异常协同服务的 HTTP/JSON 边界，复用基础服务的无框架路由。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from ..api import route as foundation_route
from ..errors import DomainError, ValidationError
from ..service import DomainService
from ..storage import Database
from .service import AirExceptionService


def _receipt_response(receipt) -> tuple[int, dict[str, Any]]:
    return (200 if receipt.replayed else 201), receipt.__dict__


def _dispatch(service: AirExceptionService, method: str, path: str, query: dict[str, list[str]],
              body: dict[str, Any], actor_id: str) -> tuple[int, dict[str, Any]] | None:
    parts = [part for part in path.split("/") if part][1:]  # 去掉开头的 air
    if method == "POST" and parts == ["segments"]:
        return _receipt_response(service.register_segment(actor_id=actor_id, **body))
    if method == "POST" and len(parts) == 3 and parts[0] == "segments" and parts[2] == "status":
        return _receipt_response(service.update_segment_status(actor_id=actor_id, segment_id=parts[1], **body))
    if method == "POST" and parts == ["slots"]:
        return _receipt_response(service.register_slot(actor_id=actor_id, **body))
    if method == "POST" and parts == ["waybills"]:
        return _receipt_response(service.register_waybill(actor_id=actor_id, **body))
    if method == "POST" and len(parts) == 3 and parts[0] == "waybills" and parts[2] == "packages":
        return _receipt_response(service.scan_packages(actor_id=actor_id, waybill_id=parts[1], **body))
    if method == "POST" and len(parts) == 3 and parts[0] == "waybills" and parts[2] == "arrive":
        return _receipt_response(service.arrive(actor_id=actor_id, waybill_id=parts[1], **body))
    if method == "POST" and len(parts) == 3 and parts[0] == "waybills" and parts[2] == "declarations":
        return _receipt_response(service.submit_declaration(actor_id=actor_id, waybill_id=parts[1], **body))
    if method == "GET" and len(parts) == 3 and parts[0] == "waybills" and parts[2] == "declarations":
        return 200, {"items": service.list_declarations(actor_id=actor_id, waybill_id=parts[1])}
    if method == "GET" and len(parts) == 2 and parts[0] == "waybills":
        return 200, service.waybill_detail(actor_id=actor_id, waybill_id=parts[1])
    if method == "POST" and len(parts) == 3 and parts[0] == "batches" and parts[2] == "split":
        return _receipt_response(service.split_batch(actor_id=actor_id, batch_id=parts[1], **body))
    if method == "POST" and parts == ["batches", "merge"]:
        return _receipt_response(service.merge_batches(actor_id=actor_id, **body))
    if method == "POST" and len(parts) == 3 and parts[0] == "batches" and parts[2] == "handover":
        return _receipt_response(service.record_handover(actor_id=actor_id, batch_id=parts[1], **body))
    if method == "POST" and len(parts) == 3 and parts[0] == "batches" and parts[2] == "rebook":
        return _receipt_response(service.rebook_segment(actor_id=actor_id, batch_id=parts[1], **body))
    if method == "POST" and len(parts) == 3 and parts[0] == "batches" and parts[2] == "complete-sort":
        return _receipt_response(service.complete_sort(actor_id=actor_id, batch_id=parts[1], **body))
    if method == "POST" and len(parts) == 3 and parts[0] == "batches" and parts[2] == "complete-inspection":
        return _receipt_response(service.complete_inspection(actor_id=actor_id, batch_id=parts[1], **body))
    if method == "POST" and len(parts) == 3 and parts[0] == "batches" and parts[2] == "execute-return":
        return _receipt_response(service.execute_return(actor_id=actor_id, batch_id=parts[1], **body))
    if method == "POST" and parts == ["decisions"]:
        return _receipt_response(service.record_decision(actor_id=actor_id, **body))
    if method == "GET" and len(parts) == 2 and parts[0] == "decisions":
        return 200, service.get_decision(actor_id=actor_id, decision_id=parts[1])
    if method == "POST" and parts == ["leases"]:
        return _receipt_response(service.lease_slot(actor_id=actor_id, **body))
    if method == "POST" and len(parts) == 3 and parts[0] == "leases" and parts[2] == "release":
        return _receipt_response(service.release_slot(actor_id=actor_id, lease_id=parts[1], **body))
    if method == "POST" and parts == ["bookings"]:
        return _receipt_response(service.book_segment(actor_id=actor_id, **body))
    if method == "GET" and parts == ["desk-board"]:
        site_id = query.get("site_id", [""])[0]
        if not site_id:
            raise ValidationError("site_id 不能为空")
        return 200, service.desk_board(actor_id=actor_id, site_id=site_id)
    if method == "GET" and parts == ["delay-impact"]:
        segment_id = query.get("segment_id", [""])[0]
        if not segment_id:
            raise ValidationError("segment_id 不能为空")
        raw_delay = query.get("delay_minutes", [None])[0]
        delay = int(raw_delay) if raw_delay is not None else None
        return 200, service.delay_impact(actor_id=actor_id, segment_id=segment_id, delay_minutes=delay)
    return None


def route(foundation: DomainService, service: AirExceptionService, method: str, path: str,
          body: dict[str, Any] | None, headers: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    """先匹配航空异常协同路由，未命中时回落到基础服务路由。"""

    headers = headers or {}
    parsed = urlparse(path)
    if parsed.path == "/air" or parsed.path.startswith("/air/"):
        actor_id = headers.get("X-Actor-Id", "")
        if not actor_id:
            return 403, {"error": "permission_denied", "message": "缺少操作者标识"}
        try:
            result = _dispatch(service, method, parsed.path, parse_qs(parsed.query), body or {}, actor_id)
        except DomainError as exc:
            return exc.status, {"error": exc.code, "message": str(exc)}
        except (TypeError, ValueError) as exc:
            return 400, {"error": "invalid_request", "message": str(exc)}
        if result is None:
            return 404, {"error": "route_not_found", "message": "接口不存在"}
        return result
    return foundation_route(foundation, method, path, body, headers)


class AirHandler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为协同服务路由调用。"""

    foundation: DomainService
    service: AirExceptionService

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        status, payload = route(self.foundation, self.service, self.command, self.path, body,
                                {"X-Actor-Id": self.headers.get("X-Actor-Id", "")})
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
    """启动航空枢纽异常协同 HTTP 服务。"""

    parser = argparse.ArgumentParser(description="启动航空枢纽异常协同服务")
    parser.add_argument("--database", default="air_exception.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    args = parser.parse_args()
    database = Database(args.database)
    foundation = DomainService(database)
    AirHandler.foundation = foundation
    AirHandler.service = AirExceptionService(foundation)
    server = ThreadingHTTPServer((args.host, args.port), AirHandler)
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
