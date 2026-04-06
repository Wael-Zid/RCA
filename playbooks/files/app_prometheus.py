import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090").rstrip("/")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "10"))

app = FastAPI(
    title="Prometheus MCP Bridge",
    version="1.0.0",
    description="Read-only bridge for Prometheus queries used by AI/LLM investigations."
)


# ---------- Logging JSON ----------
logger = logging.getLogger("prom_mcp")
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
class InstantQueryRequest(BaseModel):
    promql: str = Field(..., min_length=1, description="PromQL instant query")


class RangeQueryRequest(BaseModel):
    promql: str = Field(..., min_length=1, description="PromQL range query")
    start: str = Field(..., description="RFC3339 or unix timestamp")
    end: str = Field(..., description="RFC3339 or unix timestamp")
    step: str = Field(..., description="Query resolution step width, e.g. 30s, 1m")


# ---------- Helpers ----------
def log_query(kind: str, promql: Optional[str], response_time_ms: float, success: bool, status_code: Optional[int] = None) -> None:
    logger.info(
        f"{kind} request completed",
        extra={
            "extra_data": {
                "query_type": kind,
                "promql": promql,
                "response_time_ms": round(response_time_ms, 2),
                "success": success,
                "status_code": status_code,
            }
        },
    )


def call_prometheus(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{PROMETHEUS_URL}{path}"
    started = time.perf_counter()
    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        elapsed_ms = (time.perf_counter() - started) * 1000

        promql = None
        if params and "query" in params:
            promql = params["query"]

        log_query(
            kind=path,
            promql=promql,
            response_time_ms=elapsed_ms,
            success=response.ok,
            status_code=response.status_code,
        )

        response.raise_for_status()
        data = response.json()

        if data.get("status") != "success":
            raise HTTPException(status_code=502, detail={"message": "Prometheus returned non-success status", "payload": data})

        return data

    except requests.RequestException as e:
        elapsed_ms = (time.perf_counter() - started) * 1000
        promql = params.get("query") if params else None
        log_query(
            kind=path,
            promql=promql,
            response_time_ms=elapsed_ms,
            success=False,
            status_code=None,
        )
        raise HTTPException(status_code=502, detail=f"Prometheus unreachable or request failed: {str(e)}")


def check_prometheus_reachable() -> bool:
    try:
        response = requests.get(f"{PROMETHEUS_URL}/-/healthy", timeout=REQUEST_TIMEOUT)
        return response.ok
    except requests.RequestException:
        return False


# ---------- Routes ----------
@app.get("/health")
def health() -> Dict[str, Any]:
    reachable = check_prometheus_reachable()
    return {
        "status": "healthy" if reachable else "degraded",
        "prometheus_url": PROMETHEUS_URL,
        "prometheus_reachable": reachable,
    }


@app.post("/query_instant")
def query_instant(body: InstantQueryRequest) -> Dict[str, Any]:
    data = call_prometheus(
        "/api/v1/query",
        params={"query": body.promql},
    )
    result_type = data.get("data", {}).get("resultType")
    if result_type != "vector":
        # On renvoie quand même les données, mais on signale le type réel
        return {
            "warning": f"Expected instant vector, got {result_type}",
            "resultType": result_type,
            "result": data.get("data", {}).get("result", []),
        }

    return {
        "resultType": result_type,
        "result": data.get("data", {}).get("result", []),
    }


@app.post("/query_range")
def query_range(body: RangeQueryRequest) -> Dict[str, Any]:
    data = call_prometheus(
        "/api/v1/query_range",
        params={
            "query": body.promql,
            "start": body.start,
            "end": body.end,
            "step": body.step,
        },
    )
    result_type = data.get("data", {}).get("resultType")
    if result_type != "matrix":
        return {
            "warning": f"Expected time series matrix, got {result_type}",
            "resultType": result_type,
            "result": data.get("data", {}).get("result", []),
        }

    return {
        "resultType": result_type,
        "result": data.get("data", {}).get("result", []),
    }


@app.get("/get_alerts")
def get_alerts() -> Dict[str, Any]:
    data = call_prometheus("/api/v1/alerts")
    alerts = data.get("data", {}).get("alerts", [])

    firing_alerts = [
        {
            "labels": alert.get("labels", {}),
            "annotations": alert.get("annotations", {}),
            "state": alert.get("state"),
            "activeAt": alert.get("activeAt"),
            "value": alert.get("value"),
        }
        for alert in alerts
        if alert.get("state") == "firing"
    ]

    return {
        "count": len(firing_alerts),
        "alerts": firing_alerts,
    }


@app.get("/get_targets")
def get_targets() -> Dict[str, Any]:
    data = call_prometheus("/api/v1/targets")
    active_targets = data.get("data", {}).get("activeTargets", [])

    formatted = [
        {
            "scrapeUrl": target.get("scrapeUrl"),
            "health": target.get("health"),
            "lastError": target.get("lastError"),
            "labels": target.get("labels", {}),
            "discoveredLabels": target.get("discoveredLabels", {}),
            "lastScrape": target.get("lastScrape"),
        }
        for target in active_targets
    ]

    return {
        "count": len(formatted),
        "targets": formatted,
    }