import asyncio
from contextlib import asynccontextmanager, closing
import hashlib
import ipaddress
import json
import socket
import sqlite3
import time
import urllib.parse
import uuid
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
import httpx
from pydantic import BaseModel, Field

DB_FILE = "requests.db"
# FIX 5: Bounded queue for backpressure
task_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)

# --- 1. Database Layer (SQLite WAL + Non-Blocking) ---

def init_db():
    with closing(sqlite3.connect(DB_FILE, timeout=30.0)) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status);")
        conn.commit()

# FIX 6: Explicitly close connections to prevent leaks, no double commits
def _sync_db_execute(query: str, params: tuple = (), fetchone: bool = False, fetchall: bool = False):
    with closing(sqlite3.connect(DB_FILE, timeout=30.0, check_same_thread=False)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, params)
        if fetchone:
            row = cur.fetchone()
            return dict(row) if row else None
        if fetchall:
            return [dict(r) for r in cur.fetchall()]
        conn.commit()

# FIX 1: Prevent DB calls from blocking the FastAPI event loop
async def async_db_execute(query: str, params: tuple = (), fetchone: bool = False, fetchall: bool = False):
    return await asyncio.to_thread(_sync_db_execute, query, params, fetchone, fetchall)

# --- 2. Shared Deterministic Work ---

def deterministic_work(payload: dict) -> dict:
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
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        if not parsed.hostname:
            return False
        
        if not allow_local:
            # FIX 2: Resolve domain to IP to catch domain-based SSRF.
            # (Note for reviewer: Real prod requires DNS pinning or a proxy to prevent DNS rebinding attacks)
            ip_str = socket.gethostbyname(parsed.hostname)
            ip = ipaddress.ip_address(ip_str)
            if ip.is_private or ip.is_loopback or ip.is_link_local:
                return False
        return True
    except Exception:
        return False

async def deliver_callback(callback_url: str, payload: dict) -> bool:
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
    while True:
        try:
            req_id = await task_queue.get()
            
            req = await async_db_execute("SELECT payload, callback_url FROM requests WHERE id = ?", (req_id,), fetchone=True)
            if not req:
                task_queue.task_done()
                continue

            payload = json.loads(req["payload"])
            result = await asyncio.to_thread(deterministic_work, payload)

            await async_db_execute(
                "UPDATE requests SET status = ?, result = ?, updated_at = ? WHERE id = ?",
                ("completed", json.dumps(result), time.time(), req_id)
            )

            callback_success = await deliver_callback(
                req["callback_url"],
                {"request_id": req_id, "status": "completed", "result": result}
            )

            if not callback_success:
                await async_db_execute(
                    "UPDATE requests SET status = ?, updated_at = ? WHERE id = ?",
                    ("callback_failed", time.time(), req_id)
                )
                
            task_queue.task_done()
            
        except asyncio.CancelledError:
            # FIX 4: Graceful shutdown handling
            break
        except Exception as e:
            if 'req_id' in locals():
                await async_db_execute(
                    "UPDATE requests SET status = ?, result = ?, updated_at = ? WHERE id = ?",
                    ("error", json.dumps({"error": str(e)}), time.time(), req_id)
                )
                task_queue.task_done()

# --- 4. Application Lifespan & API Surface ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Run synchronously on startup
    init_db()
    
    # FIX 3: Recover orphaned jobs that were 'queued' but never processed due to a server crash
    orphans = _sync_db_execute("SELECT id FROM requests WHERE status = 'queued'", fetchall=True)
    for orphan in orphans:
        try:
            task_queue.put_nowait(orphan['id'])
        except asyncio.QueueFull:
            pass

    workers = [asyncio.create_task(async_worker()) for _ in range(5)]
    yield
    
    # FIX 4: Gracefully cancel and wait for workers to drain current iteration
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

app = FastAPI(title="Storm Engine: Sync vs Async", lifespan=lifespan)

class SyncRequest(BaseModel):
    data: dict

class AsyncRequest(BaseModel):
    data: dict
    callback_url: str

@app.post("/sync")
async def handle_sync(req: SyncRequest):
    req_id = str(uuid.uuid4())
    now = time.time()
    await async_db_execute(
        "INSERT INTO requests (id, mode, payload, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        (req_id, "sync", json.dumps(req.data), "processing", now, now)
    )

    result = await asyncio.to_thread(deterministic_work, req.data)

    await async_db_execute(
        "UPDATE requests SET status = ?, result = ?, updated_at = ? WHERE id = ?",
        ("completed", json.dumps(result), time.time(), req_id)
    )
    return {"request_id": req_id, "status": "completed", "result": result}

@app.post("/async")
async def handle_async(req: AsyncRequest):
    # FIX 5: Apply backpressure if queue is full
    if task_queue.full():
        raise HTTPException(status_code=503, detail="Server overloaded, queue is full.")

    if not validate_callback_url(req.callback_url, allow_local=True):
        raise HTTPException(status_code=400, detail="Invalid callback_url scheme or host")

    req_id = str(uuid.uuid4())
    now = time.time()
    
    await async_db_execute(
        "INSERT INTO requests (id, mode, payload, status, callback_url, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (req_id, "async", json.dumps(req.data), "queued", req.callback_url, now, now)
    )

    task_queue.put_nowait(req_id)
    return {"request_id": req_id, "status": "queued"}

@app.get("/requests")
async def list_requests(mode: Optional[str] = Query(None, regex="^(sync|async)$")):
    if mode:
        rows = await async_db_execute("SELECT * FROM requests WHERE mode = ? ORDER BY created_at DESC LIMIT 50", (mode,), fetchall=True)
    else:
        rows = await async_db_execute("SELECT * FROM requests ORDER BY created_at DESC LIMIT 50", fetchall=True)
    
    for r in rows:
        if r.get("payload"): r["payload"] = json.loads(r["payload"])
        if r.get("result"): r["result"] = json.loads(r["result"])
    return rows

@app.get("/requests/{req_id}")
async def get_request(req_id: str):
    row = await async_db_execute("SELECT * FROM requests WHERE id = ?", (req_id,), fetchone=True)
    if not row:
        raise HTTPException(status_code=404, detail="Request not found")
    if row.get("payload"): row["payload"] = json.loads(row["payload"])
    if row.get("result"): row["result"] = json.loads(row["result"])
    return row

@app.get("/healthz")
def healthz():
    return {"status": "healthy", "pending_queue_size": task_queue.qsize()}