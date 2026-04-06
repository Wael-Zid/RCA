import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException

PROMETHEUS_MCP_URL = os.getenv("PROMETHEUS_MCP_URL", "http://127.0.0.1:8091").rstrip("/")
LOKI_MCP_URL = os.getenv("LOKI_MCP_URL", "http://127.0.0.1:8092").rstrip("/")
JAEGER_MCP_URL = os.getenv("JAEGER_MCP_URL", "http://127.0.0.1:8093").rstrip("/")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15"))

app = FastAPI(
    title="RCA Cross-Pillar Orchestrator",
    version="1.0.0",
    description="Correlates metrics, logs and traces for automated RCA."
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_rfc3339(dt: datetime) -> str:
    return dt.isoformat()


def to_unix_ns(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1_000_000_000))


def to_unix_us(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1_000_000))


def post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"POST failed on {url}: {e}")


def get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"GET failed on {url}: {e}")


def extract_trace_ids(logs: List[Dict[str, Any]]) -> List[str]:
    pattern = re.compile(r'(?:trace[_-]?id|traceID)[=: ]([a-fA-F0-9]+)')
    found = []
    for item in logs:
        line = item.get("line", "")
        match = pattern.search(line)
        if match:
            found.append(match.group(1))
    return list(dict.fromkeys(found))


def find_failing_service(trace: Dict[str, Any]) -> Optional[str]:
    spans = trace.get("spans", [])
    failing = None
    max_duration = -1

    for span in spans:
        status_code = span.get("status_code")
        duration_ms = span.get("duration_ms") or 0
        service = span.get("service")

        if status_code in [500, "500", "ERROR", "Error", "error"]:
            if duration_ms > max_duration and service:
                max_duration = duration_ms
                failing = service

    if failing:
        return failing

    for span in spans:
        duration_ms = span.get("duration_ms") or 0
        service = span.get("service")
        if duration_ms > max_duration and service:
            max_duration = duration_ms
            failing = service

    return failing


def summarize_metric_vector(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return data.get("result", [])


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "healthy",
        "prometheus_mcp_url": PROMETHEUS_MCP_URL,
        "loki_mcp_url": LOKI_MCP_URL,
        "jaeger_mcp_url": JAEGER_MCP_URL,
    }


@app.post("/run_rca")
def run_rca() -> Dict[str, Any]:
    end_dt = now_utc()
    start_dt = end_dt - timedelta(minutes=15)

    start_rfc3339 = to_rfc3339(start_dt)
    end_rfc3339 = to_rfc3339(end_dt)

    start_ns = to_unix_ns(start_dt)
    end_ns = to_unix_ns(end_dt)

    start_us = to_unix_us(start_dt)
    end_us = to_unix_us(end_dt)

    # 1) Metrics
    latency_metrics = post_json(
        f"{PROMETHEUS_MCP_URL}/query_range",
        {
            "promql": 'rate(http_server_request_duration_seconds_sum[5m])',
            "start": start_rfc3339,
            "end": end_rfc3339,
            "step": "60s",
        },
    )

    error_metrics = post_json(
        f"{PROMETHEUS_MCP_URL}/query_instant",
        {
            "promql": 'sum(rate(http_requests_total{status=~"5.."}[5m])) by (service)'
        },
    )

    # 2) Logs
    logs_data = post_json(
        f"{LOKI_MCP_URL}/query_logs",
        {
            "logql": '{job=~".+"} |= "error"',
            "start": start_ns,
            "end": end_ns,
            "limit": 50,
        },
    )
    logs = logs_data.get("logs", [])

    # 3) Extract trace ids
    trace_ids = extract_trace_ids(logs)

    trace_data = None
    failing_service = None

    # 4) Trace-log correlation
    if trace_ids:
        trace_data = get_json(
            f"{JAEGER_MCP_URL}/get_trace",
            params={"trace_id": trace_ids[0]},
        )
        failing_service = find_failing_service(trace_data)

    # 5) Trace-metric correlation
    service_metrics = None
    if failing_service:
        service_metrics = post_json(
            f"{PROMETHEUS_MCP_URL}/query_instant",
            {
                "promql": f'sum(rate(http_requests_total{{service="{failing_service}",status=~"5.."}}[5m]))'
            },
        )

    # 6) Build RCA
    root_cause = "Aucune cause racine déterminée avec certitude."
    confidence = "medium"

    if failing_service and trace_data and logs:
        root_cause = f"Le service {failing_service} apparaît comme le service le plus probable en erreur, confirmé par les logs et les spans Jaeger."
        confidence = "high"
    elif logs:
        root_cause = "Des erreurs applicatives ont été détectées dans Loki, mais la corrélation de trace reste partielle."

    return {
        "time_window": {
            "start": start_rfc3339,
            "end": end_rfc3339,
        },
        "root_cause": root_cause,
        "confidence": confidence,
        "evidence": {
            "metrics": {
                "latency": summarize_metric_vector(latency_metrics),
                "errors": summarize_metric_vector(error_metrics),
                "service_check": summarize_metric_vector(service_metrics) if service_metrics else [],
            },
            "logs": logs[:10],
            "traces": {
                "trace_ids": trace_ids,
                "trace": trace_data if trace_data else {},
            },
        },
    }