import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

import requests
import redis
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

# ---------- Config ----------
LOKI_URL = os.getenv("LOKI_URL", "http://localhost:3100").rstrip("/")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "10"))
MAX_LOG_LINES = 50

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
RATE_LIMIT = 10  # req/sec

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI(title="Loki MCP PRO")

# ---------- Logging ----------
logger = logging.getLogger("loki_mcp")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()


class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": record.getMessage(),
            **getattr(record, "extra_data", {})
        })


handler.setFormatter(JsonFormatter())
logger.handlers.clear()
logger.addHandler(handler)

# ---------- Models ----------


class QueryLogsRequest(BaseModel):
    logql: str
    start: str
    end: str
    limit: Optional[int] = 50
    cursor: Optional[str] = None
    severity: Optional[str] = None


# ---------- Helpers ----------

def to_iso(ts_ns: str) -> str:
    return datetime.fromtimestamp(int(ts_ns) / 1e9, tz=timezone.utc).isoformat()


def cache_key(params: Dict[str, Any]) -> str:
    return "loki:" + json.dumps(params, sort_keys=True)


def rate_limit_check(ip: str):
    key = f"rate:{ip}"
    count = redis_client.incr(key)
    if count == 1:
        redis_client.expire(key, 1)
    if count > RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Too many requests")


def call_loki(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    key = cache_key(params)

    # 🔥 CACHE
    cached = redis_client.get(key)
    if cached:
        return json.loads(cached)

    start_time = time.perf_counter()
    url = f"{LOKI_URL}{path}"

    try:
        res = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        elapsed = (time.perf_counter() - start_time) * 1000

        logger.info("query",
                    extra={"extra_data": {
                        "query": params.get("query"),
                        "time_ms": round(elapsed, 2),
                        "success": res.ok
                    }})

        res.raise_for_status()
        data = res.json()

        redis_client.setex(key, 30, json.dumps(data))  # cache 30s

        return data

    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=str(e))


def check_loki():
    try:
        return requests.get(f"{LOKI_URL}/ready", timeout=REQUEST_TIMEOUT).ok
    except:
        return False


# ---------- Routes ----------

@app.middleware("http")
async def limiter(request: Request, call_next):
    ip = request.client.host
    rate_limit_check(ip)
    return await call_next(request)


@app.get("/health")
def health():
    return {
        "status": "healthy" if check_loki() else "degraded"
    }


@app.post("/query_logs")
def query_logs(body: QueryLogsRequest):
    limit = min(body.limit or MAX_LOG_LINES, MAX_LOG_LINES)

    logql = body.logql

    # 🔥 SEVERITY FILTER
    if body.severity:
        logql += f' |= "{body.severity.upper()}"'

    # 🔥 CURSOR pagination
    start = body.cursor if body.cursor else body.start

    data = call_loki("/loki/api/v1/query_range", {
        "query": logql,
        "start": start,
        "end": body.end,
        "limit": limit,
        "direction": "BACKWARD"
    })

    result = data.get("data", {}).get("result", [])

    logs: List[Dict[str, Any]] = []
    last_ts = None

    for stream in result:
        for ts, line in stream.get("values", []):
            logs.append({
                "timestamp": to_iso(ts),
                "line": line
            })
            last_ts = ts
            if len(logs) >= MAX_LOG_LINES:
                break
        if len(logs) >= MAX_LOG_LINES:
            break

    return {
        "count": len(logs),
        "logs": logs,
        "next_cursor": last_ts  # 🔥 pagination
    }


@app.get("/get_label_values")
def get_label_values(label: str):
    data = call_loki(f"/loki/api/v1/label/{label}/values", {})
    return {
        "values": data.get("data", [])
    }


@app.post("/get_log_volume")
def get_log_volume(body: QueryLogsRequest):
    query = f'count_over_time({body.logql}[1m])'

    data = call_loki("/loki/api/v1/query_range", {
        "query": query,
        "start": body.start,
        "end": body.end,
        "step": "60"
    })

    series = []
    for stream in data.get("data", {}).get("result", []):
        for ts, val in stream.get("values", []):
            series.append({
                "timestamp": to_iso(ts),
                "count": float(val)
            })

    return {"series": series}