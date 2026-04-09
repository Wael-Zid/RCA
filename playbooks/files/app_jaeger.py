import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ---------- Config ----------
JAEGER_URL = os.getenv("JAEGER_URL", "http://localhost:16686").rstrip("/")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "10"))

app = FastAPI(
    title="Jaeger MCP Bridge",
    version="1.0.0",
    description="Read-only bridge for Jaeger trace queries used by AI/LLM investigations.",
)

# ---------- Logging JSON ----Q------
logger = logging.getLogger("jaeger_mcp")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra_data") and isinstance(record.extra_data, dict):
            payload.update(record.extra_data)
        return json.dumps(payload, ensure_ascii=False)


handler.setFormatter(JsonFormatter())
logger.handlers.clear()
logger.addHandler(handler)

# ---------- Models ----------


class FindTracesRequest(BaseModel):
    service: str = Field(..., min_length=1, description="Jaeger service name")
    operation: Optional[str] = Field(None, description="Optional operation name")
    start: str = Field(..., description="RFC3339 or unix timestamp in microseconds")
    end: str = Field(..., description="RFC3339 or unix timestamp in microseconds")
    limit: int = Field(20, ge=1, le=100, description="Max number of traces to return")
    tags: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional tags filter, e.g. {'http.status_code':'500'}",
    )


# ---------- Helpers ----------


def log_query(
    kind: str,
    success: bool,
    response_time_ms: float,
    status_code: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    logger.info(
        f"{kind} request completed",
        extra={
            "extra_data": {
                "query_type": kind,
                "response_time_ms": round(response_time_ms, 2),
                "success": success,
                "status_code": status_code,
                **(details or {}),
            }
        },
    )


def call_jaeger(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{JAEGER_URL}{path}"
    started = time.perf_counter()

    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        elapsed_ms = (time.perf_counter() - started) * 1000

        log_query(
            kind=path,
            success=response.ok,
            response_time_ms=elapsed_ms,
            status_code=response.status_code,
            details={"params": params or {}},
        )

        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        elapsed_ms = (time.perf_counter() - started) * 1000
        log_query(
            kind=path,
            success=False,
            response_time_ms=elapsed_ms,
            status_code=None,
            details={"params": params or {}},
        )
        raise HTTPException(status_code=502, detail=f"Jaeger unreachable or request failed: {str(e)}")


def check_jaeger_reachable() -> bool:
    # Health pragmatique: on teste une route de lecture simple
    try:
        response = requests.get(f"{JAEGER_URL}/api/services", timeout=REQUEST_TIMEOUT)
        return response.ok
    except requests.RequestException:
        return False


def normalize_tags(tags: Optional[Dict[str, Any]]) -> Optional[str]:
    if not tags:
        return None
    return json.dumps(tags, separators=(",", ":"), ensure_ascii=False)


def parse_time_to_iso(value: Optional[int]) -> Optional[str]:
    """
    Jaeger renvoie souvent startTime en microsecondes.
    """
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc).isoformat()
    except Exception:
        return None


def duration_us_to_ms(value: Optional[int]) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(value / 1000.0, 3)
    except Exception:
        return None


def extract_status_code(span: Dict[str, Any]) -> Optional[Any]:
    tags = span.get("tags", []) or []
    for tag in tags:
        key = tag.get("key")
        if key in {"http.status_code", "rpc.grpc.status_code", "otel.status_code"}:
            return tag.get("value")
    return None


def build_parent_map(spans: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    parent_map: Dict[str, Optional[str]] = {}
    for span in spans:
        refs = span.get("references", []) or []
        parent_id = None
        for ref in refs:
            if ref.get("refType") == "CHILD_OF":
                parent_id = ref.get("spanID")
                break
        parent_map[span.get("spanID")] = parent_id
    return parent_map


def simplify_trace(trace: Dict[str, Any]) -> Dict[str, Any]:
    spans = trace.get("spans", []) or []
    processes = trace.get("processes", {}) or {}
    parent_map = build_parent_map(spans)

    simplified_spans = []
    for span in spans:
        process_id = span.get("processID")
        process = processes.get(process_id, {}) if process_id else {}
        service_name = process.get("serviceName")

        simplified_spans.append(
            {
                "trace_id": trace.get("traceID"),
                "span_id": span.get("spanID"),
                "operation": span.get("operationName"),
                "service": service_name,
                "start_time": parse_time_to_iso(span.get("startTime")),
                "duration_ms": duration_us_to_ms(span.get("duration")),
                "status_code": extract_status_code(span),
                "parent_span_id": parent_map.get(span.get("spanID")),
                "references": span.get("references", []),
                "tags": span.get("tags", []),
                "logs": span.get("logs", []),
            }
        )

    return {
        "trace_id": trace.get("traceID"),
        "span_count": len(simplified_spans),
        "processes": processes,
        "spans": simplified_spans,
        "warnings": trace.get("warnings", None),
    }


# ---------- Routes ----------

@app.get("/health")
def health() -> Dict[str, Any]:
    reachable = check_jaeger_reachable()
    return {
        "status": "healthy" if reachable else "degraded",
        "jaeger_url": JAEGER_URL,
        "jaeger_reachable": reachable,
    }


@app.get("/get_services")
def get_services() -> Dict[str, Any]:
    data = call_jaeger("/api/services")
    services = data.get("data", []) or []
    return {
        "count": len(services),
        "services": services,
    }


@app.get("/get_operations")
def get_operations(service: str) -> Dict[str, Any]:
    if not service:
        raise HTTPException(status_code=400, detail="service is required")

    data = call_jaeger("/api/operations", params={"service": service})
    operations = data.get("data", []) or []

    normalized = []
    for op in operations:
        if isinstance(op, dict):
            normalized.append(
                {
                    "name": op.get("name"),
                    "span_kind": op.get("spanKind"),
                }
            )
        else:
            normalized.append({"name": str(op), "span_kind": None})

    return {
        "service": service,
        "count": len(normalized),
        "operations": normalized,
    }


@app.post("/find_traces")
def find_traces(body: FindTracesRequest) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "service": body.service,
        "start": body.start,
        "end": body.end,
        "limit": body.limit,
    }

    if body.operation:
        params["operation"] = body.operation

    tags_json = normalize_tags(body.tags)
    if tags_json:
        params["tags"] = tags_json

    data = call_jaeger("/api/traces", params=params)
    traces = data.get("data", []) or []

    summarized = []
    for trace in traces:
        trace_id = trace.get("traceID")
        spans = trace.get("spans", []) or []
        processes = trace.get("processes", {}) or {}

        services = sorted(
            {
                proc.get("serviceName")
                for proc in processes.values()
                if isinstance(proc, dict) and proc.get("serviceName")
            }
        )

        root_start = min((s.get("startTime") for s in spans if s.get("startTime") is not None), default=None)
        total_duration = sum((s.get("duration", 0) for s in spans if s.get("duration") is not None))

        summarized.append(
            {
                "trace_id": trace_id,
                "traceID": trace_id,  # utile pour cross-reference Loki
                "span_count": len(spans),
                "services": services,
                "start_time": parse_time_to_iso(root_start),
                "approx_total_duration_ms": duration_us_to_ms(total_duration),
            }
        )

    return {
        "count": len(summarized),
        "traces": summarized,
    }


@app.get("/get_trace")
def get_trace(trace_id: str) -> Dict[str, Any]:
    if not trace_id:
        raise HTTPException(status_code=400, detail="trace_id is required")

    data = call_jaeger(f"/api/traces/{trace_id}")
    traces = data.get("data", []) or []

    if not traces:
        raise HTTPException(status_code=404, detail=f"Trace not found: {trace_id}")

    trace = traces[0]
    return simplify_trace(trace)