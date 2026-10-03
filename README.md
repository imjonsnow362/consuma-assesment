# Sync API vs Async API (Callback) Under Request Storms

A high-throughput Python backend demonstrating two distinct interaction styles (inline synchronous execution vs. decoupled asynchronous callback delivery) sharing identical deterministic logic.

## Key Design Decisions & Tradeoffs

1. **In-Memory Queue vs. Distributed Broker:**
   - *Decision:* Implemented `asyncio.Queue` paired with 5 dedicated asynchronous worker tasks directly inside FastAPI's lifespan loop.
   - *Tradeoff:* Avoids heavy external infrastructure overhead (Redis/RabbitMQ/Celery) while guaranteeing zero network hops for task queuing. For horizontal multi-instance scaling, swapping `asyncio.Queue` for Redis Streams or SQS is straightforward.
2. **Concurrency & Event Loop Health:**
   - *Decision:* Heavy CPU work (`deterministic_work` with 40,000 SHA-256 iterations) is executed inside worker threads via `asyncio.to_thread()`.
   - *Tradeoff:* Prevents CPU-bound computation from blocking FastAPI's async event loop, preserving sub-millisecond response times for incoming request storms.
3. **Database Architecture:**
   - *Decision:* SQLite3 with `PRAGMA journal_mode=WAL` (Write-Ahead Logging) and `PRAGMA synchronous=NORMAL`.
   - *Tradeoff:* WAL allows simultaneous concurrent readers alongside active writes, preventing database lock contention under burst traffic without requiring a PostgreSQL container.
4. **Resiliency & SSRF Safeguards:**
   - *Decision:* Outgoing callbacks use `httpx.AsyncClient` with a 4-second timeout, 3-step exponential backoff, and URL scheme validation.
   - *Tradeoff:* If a callback endpoint is unreachable or lagging, retries occur asynchronously without stalling the queue worker or leaking connections.

---

## Running Locally

### 1. Setup
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt