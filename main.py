import asyncio
from contextlib import asynccontextmanager
import hashlib
import ipaddress
import json
import sqlite3
import time
import urllib.parse
import uuid
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
import httpx
from pydantic import BaseModel, Field

DB_FILE = "requests.db"
task_queue: asyncio.Queue = asyncio.Queue()

# --- 1. Database Layer (SQLite WAL Mode for Storm Concurrency) ---

def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=30.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,
                result TEXT,
                callback_url TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_requests_mode ON requests(mode);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_requests_created_at ON requests(created_at);")

def db_execute(query: str, params: tuple = (), fetchone: bool = False, fetchall: bool = False):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        if fetchone:
            row = cur.fetchone()
            return dict(row) if row else None
        if fetchall:
            return [dict(r) for r in cur.fetchall()]
        conn.commit()

# --- 2. Shared Deterministic Work ---

def deterministic_work(payload: dict) -> dict:
    """CPU-bound task shared across sync and async execution paths.
    Always produces identical output for identical input.
    """
    serialized = json.dumps(payload, sort_keys=True).encode("utf-8")
    curr = serialized
    for _ in range(40_000):
        curr = hashlib.sha256(curr).digest()
    return {
        "input_keys": sorted(list(payload.keys())),
        "fingerprint": curr.hex(),
        "iterations": 40_000,
    }

# --- 3. Callback Dispatcher & Worker ---

def validate_callback_url(url: str, allow_local: bool = True) -> bool:
    """Protects against SSRF and invalid schemes."""
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        if not parsed.hostname:
            return False
        
        # Production SSRF safeguard (set allow_local=False in cloud deployments)
        if not allow_local:
            ip = ipaddress.ip_address(parsed.hostname)
            if ip.is_private or ip.is_loopback or ip.is_link_local:
                return False
        return True
    except Exception:
        return False

async def deliver_callback(callback_url: str, payload: dict) -> bool:
    """Dispatches webhook with exponential backoff and timeout."""
    if not validate_callback_url(callback_url, allow_local=True):
        return False

    async with httpx.AsyncClient(timeout=4.0) as client:
        for attempt in range(3):
            try:
                res = await client.post(callback_url, json=payload)
                if 200 <= res.status_code < 300:
                    return True
            except (httpx.RequestError, httpx.TimeoutException):
                pass
            await asyncio.sleep(0.5 * (2 ** attempt))
    return False

async def async_worker():
    """Background consumer taking jobs from the in-memory queue."""
    while True:
        req_id = await task_queue.get()
        try:
            req = db_execute("SELECT payload, callback_url FROM requests WHERE id = ?", (req_id,), fetchone=True)
            if not req:
                continue

            payload = json.loads(req["payload"])
            
            # Run CPU work in thread pool to avoid blocking the event loop
            result = await asyncio.to_thread(deterministic_work, payload)

            db_execute(
                "UPDATE requests SET status = ?, result = ?, updated_at = ? WHERE id = ?",
                ("completed", json.dumps(result), time.time(), req_id)
            )

            # Send callback
            callback_success = await deliver_callback(
                req["callback_url"],
                {"request_id": req_id, "status": "completed", "result": result}
            )

            if not callback_success:
                db_execute(
                    "UPDATE requests SET status = ?, updated_at = ? WHERE id = ?",
                    ("callback_failed", time.time(), req_id)
                )

        except Exception as e:
            db_execute(
                "UPDATE requests SET status = ?, result = ?, updated_at = ? WHERE id = ?",
                ("error", json.dumps({"error": str(e)}), time.time(), req_id)
            )
        finally:
            task_queue.task_done()

# --- 4. Application Lifespan & API Surface ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # 5 concurrent workers pulling from queue
    workers = [asyncio.create_task(async_worker()) for _ in range(5)]
    yield
    for w in workers:
        w.cancel()

app = FastAPI(title="Storm Engine: Sync vs Async", lifespan=lifespan)

class SyncRequest(BaseModel):
    data: dict = Field(..., example={"item_id": 101, "batch": "A"})

class AsyncRequest(BaseModel):
    data: dict = Field(..., example={"item_id": 101, "batch": "A"})
    callback_url: str = Field(..., example="http://127.0.0.1:8001/callback")

@app.post("/sync")
async def handle_sync(req: SyncRequest):
    req_id = str(uuid.uuid4())
    now = time.time()
    db_execute(
        "INSERT INTO requests (id, mode, payload, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        (req_id, "sync", json.dumps(req.data), "processing", now, now)
    )

    result = await asyncio.to_thread(deterministic_work, req.data)

    db_execute(
        "UPDATE requests SET status = ?, result = ?, updated_at = ? WHERE id = ?",
        ("completed", json.dumps(result), time.time(), req_id)
    )
    return {"request_id": req_id, "status": "completed", "result": result}

@app.post("/async")
async def handle_async(req: AsyncRequest):
    if not validate_callback_url(req.callback_url, allow_local=True):
        raise HTTPException(status_code=400, detail="Invalid callback_url scheme or host")

    req_id = str(uuid.uuid4())
    now = time.time()
    db_execute(
        "INSERT INTO requests (id, mode, payload, status, callback_url, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (req_id, "async", json.dumps(req.data), "queued", req.callback_url, now, now)
    )

    await task_queue.put(req_id)
    return {"request_id": req_id, "status": "queued"}

@app.get("/requests")
def list_requests(mode: Optional[str] = Query(None, regex="^(sync|async)$")):
    if mode:
        rows = db_execute("SELECT * FROM requests WHERE mode = ? ORDER BY created_at DESC LIMIT 50", (mode,), fetchall=True)
    else:
        rows = db_execute("SELECT * FROM requests ORDER BY created_at DESC LIMIT 50", fetchall=True)
    
    for r in rows:
        if r.get("payload"):
            r["payload"] = json.loads(r["payload"])
        if r.get("result"):
            r["result"] = json.loads(r["result"])
    return rows

@app.get("/requests/{req_id}")
def get_request(req_id: str):
    row = db_execute("SELECT * FROM requests WHERE id = ?", (req_id,), fetchone=True)
    if not row:
        raise HTTPException(status_code=404, detail="Request not found")
    if row.get("payload"):
        row["payload"] = json.loads(row["payload"])
    if row.get("result"):
        row["result"] = json.loads(row["result"])
    return row

@app.get("/healthz")
def healthz():
    return {
        "status": "healthy",
        "pending_queue_size": task_queue.qsize()
    }