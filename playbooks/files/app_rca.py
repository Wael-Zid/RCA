import json
import os
import re
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException

PROMETHEUS_MCP_URL = os.getenv("PROMETHEUS_MCP_URL", "http://127.0.0.1:8091").rstrip("/")
LOKI_MCP_URL = os.getenv("LOKI_MCP_URL", "http://127.0.0.1:8092").rstrip("/")
JAEGER_MCP_URL = os.getenv("JAEGER_MCP_URL", "http://127.0.0.1:8093").rstrip("/")

RCA_DB_PATH = os.getenv("RCA_HISTORY_DB_PATH", "/data/rca_history.db")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))

# LLM
LLM_URL = os.getenv("LLM_URL", "http://127.0.0.1:11434/api/generate").rstrip("/")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3")
LLM_ENABLED = os.getenv("LLM_ENABLED", "true").lower() == "true"

# Drain3 optionnel
DRAIN3_URL = os.getenv("DRAIN3_URL", "").rstrip("/")
DRAIN3_ENABLED = os.getenv("DRAIN3_ENABLED", "false").lower() == "true"

app = FastAPI(
    title="RCA Cross-Pillar Orchestrator",
    version="2.0.0",
    description="Correlates metrics, logs, traces and LLM reasoning for automated RCA."
)


# ---------- DB ----------
def init_db() -> None:
    db_dir = Path(RCA_DB_PATH).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(RCA_DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS rca_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            alert_name TEXT,
            root_cause TEXT,
            confidence TEXT,
            metrics_json TEXT,
            logs_json TEXT,
            traces_json TEXT,
            raw_json TEXT
        )
    """)

    conn.commit()
    conn.close()


@app.on_event("startup")
def startup() -> None:
    init_db()


# ---------- Time helpers ----------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_rfc3339(dt: datetime) -> str:
    return dt.isoformat()


def to_unix_ns(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1_000_000_000))


def to_unix_us(dt: datetime) -> str:
    return str(int(dt.timestamp() * 1_000_000))


def get_time_window(minutes: int = 15) -> Dict[str, str]:
    end_dt = now_utc()
    start_dt = end_dt - timedelta(minutes=minutes)

    return {
        "start_rfc3339": to_rfc3339(start_dt),
        "end_rfc3339": to_rfc3339(end_dt),
        "start_ns": to_unix_ns(start_dt),
        "end_ns": to_unix_ns(end_dt),
        "start_us": to_unix_us(start_dt),
        "end_us": to_unix_us(end_dt),
    }


# ---------- HTTP helpers ----------
def post_json(url: str, payload: Dict[str, Any], timeout: Optional[float] = None) -> Dict[str, Any]:
    try:
        r = requests.post(url, json=payload, timeout=timeout or REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"POST failed on {url}: {e}")


def get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None) -> Dict[str, Any]:
    try:
        r = requests.get(url, params=params, timeout=timeout or REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"GET failed on {url}: {e}")


# ---------- RCA helpers ----------
def extract_trace_ids(logs: List[Dict[str, Any]]) -> List[str]:
    pattern = re.compile(r"(?:trace[_-]?id|traceID)[=: ]([a-fA-F0-9]+)")
    found: List[str] = []

    for item in logs:
        line = item.get("line", "")
        match = pattern.search(line)
        if match:
            found.append(match.group(1))

    return list(dict.fromkeys(found))


def find_failing_service(trace: Dict[str, Any]) -> Optional[str]:
    spans = trace.get("spans", [])
    failing = None
    max_duration = -1.0

    for span in spans:
        status_code = span.get("status_code")
        duration_ms = float(span.get("duration_ms") or 0)
        service = span.get("service")

        if status_code in [500, "500", "ERROR", "Error", "error"] and service:
            if duration_ms > max_duration:
                max_duration = duration_ms
                failing = service

    if failing:
        return failing

    for span in spans:
        duration_ms = float(span.get("duration_ms") or 0)
        service = span.get("service")
        if service and duration_ms > max_duration:
            max_duration = duration_ms
            failing = service

    return failing


def summarize_metric_vector(data: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not data:
        return []
    return data.get("result", [])


def save_rca(alert_name: str, result: Dict[str, Any]) -> int:
    conn = sqlite3.connect(RCA_DB_PATH)
    cur = conn.cursor()

    evidence = result.get("evidence", {})

    cur.execute("""
        INSERT INTO rca_decisions (
            timestamp, alert_name, root_cause, confidence,
            metrics_json, logs_json, traces_json, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now(timezone.utc).isoformat(),
        alert_name,
        result.get("root_cause"),
        result.get("confidence"),
        json.dumps(evidence.get("metrics", {}), ensure_ascii=False),
        json.dumps(evidence.get("logs", {}), ensure_ascii=False),
        json.dumps(evidence.get("traces", {}), ensure_ascii=False),
        json.dumps(result, ensure_ascii=False),
    ))

    conn.commit()
    rca_id = cur.lastrowid
    conn.close()
    return rca_id


def fetch_decisions(limit: int = 50, alert_name: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(RCA_DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    safe_limit = max(1, min(limit, 500))

    if alert_name:
        cur.execute("""
            SELECT *
            FROM rca_decisions
            WHERE alert_name = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (alert_name, safe_limit))
    else:
        cur.execute("""
            SELECT *
            FROM rca_decisions
            ORDER BY timestamp DESC
            LIMIT ?
        """, (safe_limit,))

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


# ---------- Log anomaly / Drain3 ----------
def detect_anomalous_logs_fallback(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    keywords = [
        "timeout",
        "exception",
        "connection refused",
        "db failed",
        "database error",
        "error",
        "failed",
        "unavailable",
        "latency",
    ]
    anomalies: List[Dict[str, Any]] = []

    for log in logs:
        line = (log.get("line") or "").lower()
        if any(keyword in line for keyword in keywords):
            anomalies.append(log)

    return anomalies


def detect_anomalous_logs(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if DRAIN3_ENABLED and DRAIN3_URL:
        try:
            return post_json(
                f"{DRAIN3_URL}/detect_anomalies",
                {"logs": logs},
                timeout=15,
            ).get("anomalies", [])
        except Exception:
            pass

    return detect_anomalous_logs_fallback(logs)


# ---------- LLM ----------
def build_llm_prompt(
    time_window: Dict[str, str],
    metrics: Dict[str, Any],
    logs: List[Dict[str, Any]],
    trace_data: Dict[str, Any],
    failing_service: Optional[str],
    anomalous_logs: List[Dict[str, Any]],
) -> str:
    return f"""
You are an RCA assistant for an enterprise DevOps observability system.

Mandatory instruction:
Always check all three pillars before verdict: metrics, logs, and traces.
Use the same time window for correlation.
If a trace_id is found in logs, use Jaeger trace data.
If a failing service is identified in traces, validate it with Prometheus metrics.
If anomalous logs are detected, compare them with coinciding metric anomalies.

Return your answer in strict JSON with this schema:
{{
  "root_cause": "string",
  "confidence": "low|medium|high",
  "verdict": "ESCALATE|DISMISS|INVESTIGATE",
  "evidence_summary": {{
    "metrics": ["..."],
    "logs": ["..."],
    "traces": ["..."]
  }}
}}

Time window:
{json.dumps(time_window, ensure_ascii=False)}

Failing service candidate:
{json.dumps(failing_service, ensure_ascii=False)}

Metrics:
{json.dumps(metrics, ensure_ascii=False)}

Logs:
{json.dumps(logs[:10], ensure_ascii=False)}

Anomalous logs:
{json.dumps(anomalous_logs[:10], ensure_ascii=False)}

Trace:
{json.dumps(trace_data, ensure_ascii=False)}
""".strip()


def call_llm(prompt: str) -> Dict[str, Any]:
    if not LLM_ENABLED:
        return {
            "enabled": False,
            "raw_text": "",
            "parsed": None,
        }

    payload = {
        "model": LLM_MODEL,
        "prompt": prompt,
        "stream": False,
    }

    try:
        r = requests.post(LLM_URL, json=payload, timeout=45)
        r.raise_for_status()
        data = r.json()
        text = (data.get("response") or "").strip()

        parsed = None
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None

        return {
            "enabled": True,
            "raw_text": text,
            "parsed": parsed,
        }
    except requests.RequestException as e:
        return {
            "enabled": True,
            "raw_text": f"LLM call failed: {e}",
            "parsed": None,
        }


# ---------- Routes ----------
@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "healthy",
        "prometheus_mcp_url": PROMETHEUS_MCP_URL,
        "loki_mcp_url": LOKI_MCP_URL,
        "jaeger_mcp_url": JAEGER_MCP_URL,
        "rca_history_db_path": RCA_DB_PATH,
        "llm_enabled": LLM_ENABLED,
        "drain3_enabled": DRAIN3_ENABLED,
    }


@app.get("/decisions")
def get_decisions(limit: int = 50, alert_name: Optional[str] = None) -> Dict[str, Any]:
    rows = fetch_decisions(limit=limit, alert_name=alert_name)
    return {
        "count": len(rows),
        "decisions": rows,
    }


@app.post("/run_rca")
def run_rca() -> Dict[str, Any]:
    time_window = get_time_window(minutes=15)

    latency_metrics = post_json(
        f"{PROMETHEUS_MCP_URL}/query_range",
        {
            "promql": 'rate(http_server_request_duration_seconds_sum[5m])',
            "start": time_window["start_rfc3339"],
            "end": time_window["end_rfc3339"],
            "step": "60s",
        },
    )

    error_metrics = post_json(
        f"{PROMETHEUS_MCP_URL}/query_instant",
        {
            "promql": 'sum(rate(http_requests_total{status=~"5.."}[5m])) by (service)'
        },
    )

    logs_data = post_json(
        f"{LOKI_MCP_URL}/query_logs",
        {
            "logql": '{job=~".+"} |= "error"',
            "start": time_window["start_ns"],
            "end": time_window["end_ns"],
            "limit": 50,
        },
    )
    logs = logs_data.get("logs", [])

    trace_ids = extract_trace_ids(logs)
    trace_data: Dict[str, Any] = {}
    failing_service: Optional[str] = None

    if trace_ids:
        trace_data = get_json(
            f"{JAEGER_MCP_URL}/get_trace",
            params={"trace_id": trace_ids[0]},
        )
        failing_service = find_failing_service(trace_data)

    service_metrics: Optional[Dict[str, Any]] = None
    if failing_service:
        service_metrics = post_json(
            f"{PROMETHEUS_MCP_URL}/query_instant",
            {
                "promql": f'sum(rate(http_requests_total{{service="{failing_service}",status=~"5.."}}[5m]))'
            },
        )

    anomalous_logs = detect_anomalous_logs(logs)
    log_metric_checks: Optional[Dict[str, Any]] = None
    if anomalous_logs:
        log_metric_checks = post_json(
            f"{PROMETHEUS_MCP_URL}/query_instant",
            {
                "promql": 'sum(rate(http_requests_total{status=~"5.."}[5m])) by (service)'
            },
        )

    evidence_metrics = {
        "latency": summarize_metric_vector(latency_metrics),
        "errors": summarize_metric_vector(error_metrics),
        "service_check": summarize_metric_vector(service_metrics),
        "log_metric_check": summarize_metric_vector(log_metric_checks),
    }

    evidence_logs = {
        "raw_logs": logs[:10],
        "anomalous_logs": anomalous_logs[:10],
    }

    evidence_traces = {
        "trace_ids": trace_ids,
        "trace": trace_data if trace_data else {},
    }

    root_cause = "Aucune cause racine déterminée avec certitude."
    confidence = "medium"
    verdict = "INVESTIGATE"

    if failing_service and trace_data and logs:
        root_cause = (
            f"Le service {failing_service} apparaît comme le service le plus probable "
            f"en erreur, confirmé par les logs et les spans Jaeger."
        )
        confidence = "high"
        verdict = "ESCALATE"
    elif anomalous_logs:
        root_cause = "Des logs anormaux ont été détectés et corrélés à des anomalies métriques."
        confidence = "medium"
        verdict = "INVESTIGATE"
    elif logs:
        root_cause = "Des erreurs applicatives ont été détectées dans Loki, mais la corrélation de trace reste partielle."
        confidence = "medium"
        verdict = "INVESTIGATE"

    llm_prompt = build_llm_prompt(
        time_window=time_window,
        metrics=evidence_metrics,
        logs=logs,
        trace_data=trace_data if trace_data else {},
        failing_service=failing_service,
        anomalous_logs=anomalous_logs,
    )
    llm_result = call_llm(llm_prompt)

    if llm_result.get("parsed"):
        parsed = llm_result["parsed"]
        root_cause = parsed.get("root_cause", root_cause)
        confidence = parsed.get("confidence", confidence)
        verdict = parsed.get("verdict", verdict)

    result = {
        "time_window": {
            "start": time_window["start_rfc3339"],
            "end": time_window["end_rfc3339"],
        },
        "root_cause": root_cause,
        "confidence": confidence,
        "verdict": verdict,
        "llm_prompt_instruction": "Always check all three pillars before verdict.",
        "llm_analysis": llm_result,
        "evidence": {
            "metrics": evidence_metrics,
            "logs": evidence_logs,
            "traces": evidence_traces,
        },
    }

    rca_id = save_rca("generic_alert", result)
    result["rca_id"] = rca_id

    return result