import html
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

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


@app.get("/get_recent_rcas")
def get_recent_rcas(hours: int = 24):
    safe_hours = max(1, min(hours, 24 * 30))
    since = (datetime.now(timezone.utc) - timedelta(hours=safe_hours)).isoformat()

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
    safe_days = max(1, min(days, 365))
    since = (datetime.now(timezone.utc) - timedelta(days=safe_days)).isoformat()

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
    safe_days = max(1, min(days, 365))
    since = (datetime.now(timezone.utc) - timedelta(days=safe_days)).isoformat()

    rows = query_db("""
        SELECT COUNT(*) AS frequency
        FROM rca_decisions
        WHERE alert_name = ?
          AND timestamp >= ?
    """, (alert_name, since))

    frequency = rows[0]["frequency"] if rows else 0

    return {
        "alert_name": alert_name,
        "days": safe_days,
        "frequency": frequency,
    }


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(limit: int = 50):
    safe_limit = max(1, min(limit, 200))

    rows = query_db("""
        SELECT id, timestamp, alert_name, root_cause, confidence
        FROM rca_decisions
        ORDER BY timestamp DESC
        LIMIT ?
    """, (safe_limit,))

    table_rows = ""
    for row in rows:
        confidence = html.escape(str(row.get("confidence") or ""))
        if confidence == "high":
            color = "#16a34a"
        elif confidence == "medium":
            color = "#d97706"
        else:
            color = "#dc2626"

        table_rows += f"""
        <tr>
            <td>{row.get("id")}</td>
            <td>{html.escape(str(row.get("timestamp") or ""))}</td>
            <td>{html.escape(str(row.get("alert_name") or ""))}</td>
            <td>{html.escape(str(row.get("root_cause") or ""))}</td>
            <td><span style="color:{color};font-weight:600;">{confidence}</span></td>
        </tr>
        """

    if not table_rows:
        table_rows = """
        <tr>
            <td colspan="5" style="text-align:center;color:#64748b;">Aucune décision RCA enregistrée.</td>
        </tr>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>RCA Decision Dashboard</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background: #f8fafc;
                color: #0f172a;
                margin: 0;
                padding: 32px;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
            }}
            .card {{
                background: white;
                border-radius: 16px;
                box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
                padding: 24px;
            }}
            h1 {{
                margin-top: 0;
                font-size: 28px;
            }}
            .subtitle {{
                color: #475569;
                margin-bottom: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                overflow: hidden;
            }}
            th, td {{
                text-align: left;
                padding: 14px 12px;
                border-bottom: 1px solid #e2e8f0;
                vertical-align: top;
            }}
            th {{
                background: #f1f5f9;
                font-weight: 700;
            }}
            tr:hover {{
                background: #f8fafc;
            }}
            .meta {{
                margin-bottom: 16px;
                color: #64748b;
            }}
            .badge {{
                display: inline-block;
                padding: 6px 10px;
                background: #e2e8f0;
                border-radius: 999px;
                font-size: 12px;
                font-weight: 600;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="card">
                <h1>RCA Decision Dashboard</h1>
                <div class="subtitle">Historique des décisions RCA enregistrées</div>
                <div class="meta">
                    <span class="badge">Dernières décisions affichées : {safe_limit}</span>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Timestamp</th>
                            <th>Alert Name</th>
                            <th>Root Cause</th>
                            <th>Confidence</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)