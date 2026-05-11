"""
Throttling Comparison Load Test
================================
Blasts N concurrent requests at two different paths and compares:

  PATH 1 — Kafka (buffered):
    POST /orders on order-service (port 8001)
    → order goes to Kafka → inventory-service reserves → order-service confirms
    → email-service consumes at EMAIL_RATE_LIMIT_PER_SECOND → Resend API
    HTTP caller gets instant 200. Email delivery is steady, controlled, no 429s.

  PATH 2 — Direct (unbuffered):
    POST /direct/send-email on email-service (port 8003)
    → Resend API is called synchronously, inside the HTTP request.
    Under load: latencies spike, 429s appear, emails are lost (no retry).

Usage
-----
  # Make sure services are running:
  docker compose up

  # Run default test (50 concurrent, both paths):
  python scripts/load_test.py

  # Custom: 200 concurrent, Kafka path only:
  python scripts/load_test.py --concurrency 200 --path kafka

  # Extreme: 500 concurrent, direct path (watch it break):
  python scripts/load_test.py --concurrency 500 --path direct

Environment
-----------
  ORDER_SERVICE_URL   default http://localhost:8001
  EMAIL_SERVICE_URL   default http://localhost:8003
  TEST_EMAIL          recipient address for direct emails (required for direct path)
"""

import argparse
import asyncio
import os
import statistics
import time

import httpx

ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://localhost:8001")
EMAIL_SERVICE_URL = os.getenv("EMAIL_SERVICE_URL", "http://localhost:8003")
TEST_EMAIL = os.getenv("TEST_EMAIL", "test@example.com")

# Sample products seeded in inventory (seed with /inventory/adjust first)
PRODUCTS = ["product-1", "product-2", "product-3"]


# ---------------------------------------------------------------------------
# Kafka path — fire-and-forget orders
# ---------------------------------------------------------------------------

async def send_order(client: httpx.AsyncClient, index: int) -> dict:
    product_id = PRODUCTS[index % len(PRODUCTS)]
    t0 = time.monotonic()
    try:
        resp = await client.post(
            f"{ORDER_SERVICE_URL}/orders",
            json={
                "customer_email": TEST_EMAIL,
                "product_id": product_id,
                "quantity": 1,
            },
            timeout=10.0,
        )
        elapsed = time.monotonic() - t0
        return {
            "index": index,
            "status": resp.status_code,
            "elapsed_ms": round(elapsed * 1000, 1),
            "ok": resp.status_code == 200,
        }
    except Exception as exc:
        return {
            "index": index,
            "status": 0,
            "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
            "ok": False,
            "error": str(exc),
        }


# ---------------------------------------------------------------------------
# Direct path — call email-service directly (no Kafka)
# ---------------------------------------------------------------------------

async def send_direct_email(client: httpx.AsyncClient, index: int) -> dict:
    t0 = time.monotonic()
    try:
        resp = await client.post(
            f"{EMAIL_SERVICE_URL}/direct/send-email",
            json={
                "to": TEST_EMAIL,
                "order_id": f"direct-order-{index:04d}",
                "product_id": PRODUCTS[index % len(PRODUCTS)],
                "quantity": 1,
                "confirmed": True,
            },
            timeout=15.0,
        )
        elapsed = time.monotonic() - t0
        return {
            "index": index,
            "status": resp.status_code,
            "elapsed_ms": round(elapsed * 1000, 1),
            "ok": resp.status_code == 200,
        }
    except Exception as exc:
        return {
            "index": index,
            "status": 0,
            "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
            "ok": False,
            "error": str(exc),
        }


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

async def run_test(path: str, concurrency: int) -> list[dict]:
    print(f"\n{'='*60}")
    print(f"  Path       : {path.upper()}")
    print(f"  Concurrency: {concurrency} simultaneous requests")
    print(f"{'='*60}")

    async with httpx.AsyncClient() as client:
        t_start = time.monotonic()

        if path == "kafka":
            tasks = [send_order(client, i) for i in range(concurrency)]
        else:
            tasks = [send_direct_email(client, i) for i in range(concurrency)]

        results = await asyncio.gather(*tasks)
        total_elapsed = time.monotonic() - t_start

    return _print_stats(results, total_elapsed)


def _print_stats(results: list[dict], total_elapsed: float) -> list[dict]:
    ok = [r for r in results if r["ok"]]
    failed = [r for r in results if not r["ok"]]
    latencies = [r["elapsed_ms"] for r in results]

    status_counts: dict[int, int] = {}
    for r in results:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1

    print(f"\n  Results ({len(results)} requests in {total_elapsed:.2f}s):")
    print(f"  ✅ Success : {len(ok)}")
    print(f"  ❌ Failed  : {len(failed)}")
    print(f"\n  Latency (ms):")
    print(f"    min  : {min(latencies):.1f}")
    print(f"    p50  : {statistics.median(latencies):.1f}")
    print(f"    p95  : {sorted(latencies)[int(len(latencies)*0.95)]:.1f}")
    print(f"    max  : {max(latencies):.1f}")
    print(f"\n  HTTP status breakdown:")
    for code, count in sorted(status_counts.items()):
        tag = "✅" if code == 200 else "❌"
        print(f"    {tag} {code} : {count}x")

    if failed:
        print(f"\n  First 3 failures:")
        for r in failed[:3]:
            print(f"    {r}")

    return results


# ---------------------------------------------------------------------------
# Seed inventory (so orders don't all get rejected immediately)
# ---------------------------------------------------------------------------

async def seed_inventory(quantity_per_product: int = 10000):
    print(f"\n⚙  Seeding inventory ({quantity_per_product} units per product)...")
    # The adjust endpoint DECREMENTS stock, so we need hincrby directly.
    # Instead, let's use a large positive adjust workaround:
    # We'll call the API but it decrements, so we set a huge stock via redis
    # Actually the /inventory/adjust endpoint decrements — seed via Redis directly
    # or just note it here. For simplicity we print a reminder.
    print("  ⚠  Remember to seed inventory before running the Kafka path:")
    print("     The inventory-service starts with 0 stock.")
    print("     Use Redis CLI or a script to set initial stock, e.g.:")
    for p in PRODUCTS:
        print(f"       docker exec kafka-learning-redis redis-cli hset inventory {p} {quantity_per_product}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--concurrency", type=int, default=50, help="Number of simultaneous requests (default: 50)")
    parser.add_argument("--path", choices=["kafka", "direct", "both"], default="both", help="Which path to test (default: both)")
    parser.add_argument("--seed", action="store_true", help="Print inventory seed commands and exit")
    return parser.parse_args()


async def main():
    args = parse_args()

    if args.seed:
        await seed_inventory()
        return

    print("\n🚀 Kafka Throttling vs Direct API — Load Test")
    print(f"   TEST_EMAIL        : {TEST_EMAIL}")
    print(f"   ORDER_SERVICE_URL : {ORDER_SERVICE_URL}")
    print(f"   EMAIL_SERVICE_URL : {EMAIL_SERVICE_URL}")

    if args.path in ("kafka", "both"):
        await run_test("kafka", args.concurrency)
        print("\n  ⏳ NOTE: Kafka emails arrive AFTER this script finishes.")
        print("  Watch email-service logs: docker compose logs -f email-service")
        print("  Poll stats: curl http://localhost:8003/health")

    if args.path in ("direct", "both"):
        await run_test("direct", args.concurrency)


if __name__ == "__main__":
    asyncio.run(main())
