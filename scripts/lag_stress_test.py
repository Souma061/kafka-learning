"""
Kafka Consumer Lag Stress Test
================================
Seeds inventory and blasts many orders at Kafka so the email-service
rate limiter can't keep up → visible lag spike on Grafana dashboard.

Usage:
  python scripts/lag_stress_test.py            # 500 messages (default)
  python scripts/lag_stress_test.py --n 2000   # 2 000 messages
"""

import argparse
import asyncio
import subprocess
import sys
import time

import httpx

ORDER_SERVICE_URL = "http://localhost:8001"
PRODUCTS = ["product-1", "product-2", "product-3"]


# ── 1. Seed inventory so orders are not all rejected ─────────────────────────

def seed_inventory(units: int = 99999):
    print(f"\n⚙  Seeding inventory ({units} units per product via Redis)…")
    for p in PRODUCTS:
        cmd = [
            "docker", "exec", "kafka-learning-redis",
            "redis-cli", "hset", "inventory", p, str(units),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        status = "✅" if result.returncode == 0 else "❌"
        print(f"  {status} {p}: {result.stdout.strip() or result.stderr.strip()}")


# ── 2. Fire N orders at Kafka (all async, fire-and-forget) ───────────────────

async def send_order(client: httpx.AsyncClient, index: int) -> bool:
    try:
        resp = await client.post(
            f"{ORDER_SERVICE_URL}/orders",
            json={
                "customer_email": "loadtest@example.com",
                "product_id": PRODUCTS[index % len(PRODUCTS)],
                "quantity": 1,
            },
            timeout=15.0,
        )
        return resp.status_code == 200
    except Exception:
        return False


async def blast_orders(n: int, batch: int = 100) -> int:
    """Send N orders in batches to avoid overwhelming the HTTP client."""
    print(f"\n🚀 Firing {n} orders at Kafka in batches of {batch}…")
    successes = 0
    async with httpx.AsyncClient() as client:
        for start in range(0, n, batch):
            end = min(start + batch, n)
            tasks = [send_order(client, i) for i in range(start, end)]
            results = await asyncio.gather(*tasks)
            successes += sum(results)
            pct = int(successes / n * 40)
            bar = "█" * pct + "░" * (40 - pct)
            print(f"  [{bar}] {end}/{n} sent  ({successes} accepted)", end="\r")
    print()
    return successes


# ── 3. Poll lag from InfluxDB while messages drain ───────────────────────────

def poll_lag(duration_seconds: int = 60):
    import urllib.request, json

    print(f"\n📊 Watching lag drain for {duration_seconds}s  (Ctrl+C to stop early)…")
    print(f"  {'Time':>8}  {'Max Lag':>8}  {'Total Lag':>10}  Bar")
    print("  " + "─" * 70)

    start = time.time()
    try:
        while time.time() - start < duration_seconds:
            url = (
                "http://localhost:8086/query"
                "?db=k6"
                "&q=SELECT+last%28%22lag%22%29+FROM+%22consumer_lag%22"
                "+GROUP+BY+%22group%22%2C%22topic%22"
            )
            try:
                with urllib.request.urlopen(url, timeout=5) as r:
                    data = json.load(r)
                series = data["results"][0].get("series", [])
                lags = [s["values"][0][1] for s in series if s["values"][0][1] is not None]
                total = int(sum(lags))
                peak  = int(max(lags)) if lags else 0
                bar   = "█" * min(peak // 5, 40)
                elapsed = int(time.time() - start)
                print(f"  {elapsed:>7}s  {peak:>8}  {total:>10}  {bar}")
            except Exception as e:
                print(f"  (poll error: {e})")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n  Stopped early.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--n",       type=int, default=500,  help="Total orders to send (default: 500)")
    p.add_argument("--batch",   type=int, default=100,  help="Async batch size (default: 100)")
    p.add_argument("--watch",   type=int, default=90,   help="Seconds to watch lag drain (default: 90)")
    p.add_argument("--no-seed", action="store_true",    help="Skip inventory seeding")
    return p.parse_args()


async def main():
    args = parse_args()

    print("=" * 60)
    print("  Kafka Consumer Lag Stress Test")
    print("=" * 60)

    if not args.no_seed:
        seed_inventory()

    t0 = time.time()
    accepted = await blast_orders(args.n, args.batch)
    elapsed  = time.time() - t0

    print(f"\n  Done: {accepted}/{args.n} orders accepted in {elapsed:.1f}s")
    print(f"\n  💡 Email-service rate limit means these drain slowly.")
    print(f"  💡 Watch the Grafana dashboard at:")
    print(f"     http://localhost:3000/d/consumer-lag-v2/kafka-consumer-lag")
    print(f"  💡 Or watch the live lag poll below:\n")

    poll_lag(args.watch)


if __name__ == "__main__":
    asyncio.run(main())
