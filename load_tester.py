import asyncio
import time
import httpx
from aiohttp import web

API_BASE = "http://127.0.0.1:8000"
RECEIVER_PORT = 8001
CALLBACK_URL = f"http://127.0.0.1:{RECEIVER_PORT}/callback"

NUM_REQUESTS = 100
CONCURRENCY = 20

# Tracking state
dispatched_async = {}
async_callback_latencies = []
sync_latencies = []

async def callback_handler(request):
    data = await request.json()
    req_id = data.get("request_id")
    if req_id in dispatched_async:
        elapsed = time.time() - dispatched_async[req_id]
        async_callback_latencies.append(elapsed)
    return web.Response(text="OK")

async def start_receiver():
    app = web.Application()
    app.router.add_post("/callback", callback_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", RECEIVER_PORT)
    await site.start()
    return runner

def calculate_percentiles(latencies):
    if not latencies:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "min": 0.0, "max": 0.0}
    sorted_l = sorted(latencies)
    n = len(sorted_l)
    return {
        "min": sorted_l[0],
        "max": sorted_l[-1],
        "p50": sorted_l[int(n * 0.50)],
        "p95": sorted_l[int(min(n - 1, n * 0.95))],
        "p99": sorted_l[int(min(n - 1, n * 0.99))],
    }

async def fire_sync_requests():
    print(f"\n[1/2] Firing {NUM_REQUESTS} SYNC requests (Concurrency: {CONCURRENCY})...")
    sem = asyncio.Semaphore(CONCURRENCY)

    async def send_sync(idx: int, client: httpx.AsyncClient):
        async with sem:
            start = time.time()
            try:
                res = await client.post(f"{API_BASE}/sync", json={"data": {"job": idx}})
                if res.status_code == 200:
                    sync_latencies.append(time.time() - start)
            except Exception as e:
                print(f"Sync request {idx} failed: {e}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        await asyncio.gather(*(send_sync(i, client) for i in range(NUM_REQUESTS)))

async def fire_async_requests():
    print(f"\n[2/2] Firing {NUM_REQUESTS} ASYNC requests (Concurrency: {CONCURRENCY})...")
    sem = asyncio.Semaphore(CONCURRENCY)
    ack_count = 0

    async def send_async(idx: int, client: httpx.AsyncClient):
        nonlocal ack_count
        async with sem:
            try:
                dispatch_time = time.time()
                res = await client.post(f"{API_BASE}/async", json={
                    "data": {"job": idx},
                    "callback_url": CALLBACK_URL
                })
                if res.status_code == 200:
                    req_id = res.json()["request_id"]
                    dispatched_async[req_id] = dispatch_time
                    ack_count += 1
            except Exception as e:
                print(f"Async request {idx} failed: {e}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        await asyncio.gather(*(send_async(i, client) for i in range(NUM_REQUESTS)))
    
    print(f"-> All {ack_count} async requests acknowledged instantly.")
    print("-> Waiting for background workers to execute work and deliver webhooks...")
    
    # Wait until all callbacks land or timeout expires
    wait_start = time.time()
    while len(async_callback_latencies) < ack_count and (time.time() - wait_start) < 25.0:
        await asyncio.sleep(0.5)

def print_report():
    sync_stats = calculate_percentiles(sync_latencies)
    async_stats = calculate_percentiles(async_callback_latencies)

    print("\n" + "=" * 55)
    print("             LOAD TEST RUN SUMMARY")
    print("=" * 55)
    print(f"{'Metric':<25} | {'Sync API':<12} | {'Async API (Callback)':<12}")
    print("-" * 55)
    print(f"{'Total Dispatched':<25} | {NUM_REQUESTS:<12} | {NUM_REQUESTS:<12}")
    print(f"{'Total Completed':<25} | {len(sync_latencies):<12} | {len(async_callback_latencies):<12}")
    print(f"{'Min Latency':<25} | {sync_stats['min']:.4f}s     | {async_stats['min']:.4f}s")
    print(f"{'p50 (Median)':<25} | {sync_stats['p50']:.4f}s     | {async_stats['p50']:.4f}s")
    print(f"{'p95':<25} | {sync_stats['p95']:.4f}s     | {async_stats['p95']:.4f}s")
    print(f"{'p99':<25} | {sync_stats['p99']:.4f}s     | {async_stats['p99']:.4f}s")
    print(f"{'Max Latency':<25} | {sync_stats['max']:.4f}s     | {async_stats['max']:.4f}s")
    print("=" * 55)

async def main():
    receiver = await start_receiver()
    try:
        await fire_sync_requests()
        await fire_async_requests()
        print_report()
    finally:
        await receiver.cleanup()

if __name__ == "__main__":
    asyncio.run(main())