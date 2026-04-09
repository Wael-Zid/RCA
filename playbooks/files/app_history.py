import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException

RCA_DB_PATH = os.getenv("RCA_HISTORY_DB_PATH", "/data/rca_history.db")

app = FastAPI(
    title="RCA History MCP",
    version="1.0.0",
    description="Read-only history bridge for past RCA decisions."
)


def query_db(sql: str, params=()) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(RCA_DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "db_path": RCA_DB_PATH,
    }

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(limit: int = 50):
    ...

    
@app.get("/get_recent_rcas")
def get_recent_rcas(hours: int = 24):
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    rows = query_db("""
        SELECT id, timestamp, alert_name, root_cause, confidence
        FROM rca_decisions
        WHERE timestamp >= ?
        ORDER BY timestamp DESC
    """, (since,))

    return {
        "count": len(rows),
        "rcas": rows,
    }


@app.get("/search_rcas")
def search_rcas(alert_name: str, days: int = 7):
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    rows = query_db("""
        SELECT id, timestamp, alert_name, root_cause, confidence
        FROM rca_decisions
        WHERE alert_name = ?
          AND timestamp >= ?
        ORDER BY timestamp DESC
    """, (alert_name, since))

    return {
        "count": len(rows),
        "rcas": rows,
    }


@app.get("/get_rca_detail")
def get_rca_detail(rca_id: int):
    rows = query_db("""
        SELECT *
        FROM rca_decisions
        WHERE id = ?
    """, (rca_id,))

    if not rows:
        raise HTTPException(status_code=404, detail="RCA not found")

    return rows[0]


@app.get("/get_alert_frequency")
def get_alert_frequency(alert_name: str, days: int = 7):
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    rows = query_db("""
        SELECT COUNT(*) AS frequency
        FROM rca_decisions
        WHERE alert_name = ?
          AND timestamp >= ?
    """, (alert_name, since))

    frequency = rows[0]["frequency"] if rows else 0

    return {
        "alert_name": alert_name,
        "days": days,
        "frequency": frequency,
    }