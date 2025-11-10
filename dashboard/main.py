

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
import sqlite3
import os
import json
from fastapi.responses import HTMLResponse


app = FastAPI(title="Queue monitor")



BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "queue.db")

templates = Jinja2Templates(directory="static")



def get_conn():
    if not os.path.exists(DB_PATH):
        return None
    
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    conn = get_conn()
    if conn is None:
        return templates.TemplateResponse("index.html", {
            "request": request,
            "jobs": [],
            "dlq": [],
            "status": {"total": 0, "running": 0, "failed": 0, "dlq": 0}
        })

    cur = conn.cursor()
    # job stats
    cur.execute("SELECT state, COUNT(*) AS count FROM jobs GROUP BY state")
    stats = {row["state"]: row["count"] for row in cur.fetchall()}

    cur.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 10")
    jobs = [dict(row) for row in cur.fetchall()]

    cur.execute("SELECT * FROM dlq ORDER BY payload DESC LIMIT 10")
    dlq = [dict(row) for row in cur.fetchall()]

    conn.close()

    totals = {
        "total": sum(stats.values()) if stats else 0,
        "running": stats.get("running", 0),
        "failed": stats.get("failed", 0),
        "dlq": len(dlq),
    }

    return templates.TemplateResponse("index.html", {
        "request": request,
        "jobs": jobs,
        "dlq": dlq,
        "status": totals
    })


@app.get("/api/jobs")
def api_jobs():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 20")
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return {"jobs": data}


@app.get("/api/dlq")
def api_dlq():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM dlq ORDER BY failed_at DESC LIMIT 20")
    data = [dict(row) for row in cur.fetchall()]
    conn.close()
    return {"dlq": data}

    




