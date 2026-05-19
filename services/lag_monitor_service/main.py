"""
Kafka Consumer Lag Monitor
==========================
Polls Kafka every LAG_POLL_INTERVAL seconds, computes consumer lag
per group / topic / partition, and writes the results to InfluxDB v1.

  measurement : consumer_lag
  tags        : group, topic, partition
  fields      : lag (int64)
                committed_offset (int64)
                end_offset (int64)

Environment variables
---------------------
  KAFKA_BOOTSTRAP_SERVERS   default: localhost:9092
  INFLUXDB_URL              default: http://localhost:8086
  INFLUXDB_DATABASE         default: k6
  LAG_POLL_INTERVAL         seconds between polls, default: 5
"""

import asyncio
import logging
import os
import signal
import sys

from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import KafkaError
from influxdb import InfluxDBClient  # influxdb v1 SDK  (pip install influxdb)

# ── logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("lag-monitor")

# ── config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INFLUXDB_URL    = os.getenv("INFLUXDB_URL",            "http://localhost:8086")
INFLUXDB_DB     = os.getenv("INFLUXDB_DATABASE",       "k6")
POLL_INTERVAL   = int(os.getenv("LAG_POLL_INTERVAL",   "5"))


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_influx_url(url: str) -> tuple[str, int]:
    """Split 'http://host:port' → ('host', port)."""
    stripped = url.removeprefix("https://").removeprefix("http://")
    host, _, port_s = stripped.partition(":")
    return host, int(port_s) if port_s else 8086


async def _connect_kafka() -> AIOKafkaAdminClient:
    """Return a started AIOKafkaAdminClient, retrying until Kafka is up."""
    while True:
        try:
            client = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
            await client.start()
            log.info("kafka admin client connected to %s", KAFKA_BOOTSTRAP)
            return client
        except KafkaError as exc:
            log.warning("kafka not ready (%s) — retrying in 3 s", exc)
            await asyncio.sleep(3)


def _build_influx_client() -> InfluxDBClient:
    """Create and return an InfluxDB v1 client; ensure the database exists."""
    host, port = _parse_influx_url(INFLUXDB_URL)
    client = InfluxDBClient(host=host, port=port, database=INFLUXDB_DB)
    client.create_database(INFLUXDB_DB)  # no-op if already present
    log.info("influxdb ready  host=%s port=%d db=%s", host, port, INFLUXDB_DB)
    return client


# ── one poll cycle ────────────────────────────────────────────────────────────

async def _poll_once(
    admin: AIOKafkaAdminClient,
    offset_consumer: AIOKafkaConsumer,
    influx: InfluxDBClient,
) -> None:
    """
    1. List all active consumer groups.
    2. For each group, fetch committed offsets.
    3. Fetch log-end offsets for the same partitions.
    4. Compute lag and write points to InfluxDB.
    """
    points: list[dict] = []

    # 1 ── discover groups ─────────────────────────────────────────────────────
    try:
        groups = await admin.list_consumer_groups()
    except KafkaError:
        log.exception("cannot list consumer groups — skipping cycle")
        return

    # aiokafka returns plain (group_id, protocol_type) tuples
    group_ids = [g[0] for g in groups]
    log.debug("discovered %d consumer groups: %s", len(group_ids), group_ids)

    # 2 ── per-group lag calculation ───────────────────────────────────────────
    for group_id in group_ids:
        # fetch committed offsets
        try:
            committed = await admin.list_consumer_group_offsets(group_id)
        except KafkaError as exc:
            log.warning("skipping group=%s  reason=%s", group_id, exc)
            continue

        # keep only partitions that have a valid committed offset
        active_tps = [
            tp
            for tp, meta in committed.items()
            if meta is not None and meta.offset >= 0
        ]
        if not active_tps:
            log.debug("group=%s has no committed offsets", group_id)
            continue

        # 3 ── fetch log-end offsets ───────────────────────────────────────────
        try:
            end_offsets = await offset_consumer.end_offsets(active_tps)
        except KafkaError as exc:
            log.warning(
                "cannot fetch end offsets for group=%s  reason=%s", group_id, exc
            )
            continue

        # 4 ── build influxdb points ───────────────────────────────────────────
        for tp in active_tps:
            committed_offset = committed[tp].offset
            end_offset       = end_offsets.get(tp, committed_offset)
            lag              = max(0, end_offset - committed_offset)

            points.append({
                "measurement": "consumer_lag",
                "tags": {
                    "group":     group_id,
                    "topic":     tp.topic,
                    "partition": str(tp.partition),
                },
                "fields": {
                    "lag":              lag,
                    "committed_offset": committed_offset,
                    "end_offset":       end_offset,
                },
            })

    # write all points in a single batch request
    if points:
        influx.write_points(points)
        log.info("wrote %d lag points  groups=%d", len(points), len(group_ids))
    else:
        log.debug("no active consumer group offsets found")


# ── lifecycle ─────────────────────────────────────────────────────────────────

async def run(stop: asyncio.Event) -> None:
    """Main monitoring loop — runs until *stop* is set."""
    admin          = await _connect_kafka()
    offset_consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
        # No topics subscribed — we only use it for end_offsets() calls
    )
    await offset_consumer.start()
    influx = _build_influx_client()

    log.info("lag monitor running  poll_interval=%ds", POLL_INTERVAL)
    try:
        while not stop.is_set():
            await _poll_once(admin, offset_consumer, influx)
            try:
                await asyncio.wait_for(stop.wait(), timeout=POLL_INTERVAL)
            except asyncio.TimeoutError:
                pass  # normal — just means stop was not set during the sleep
    finally:
        log.info("shutting down")
        await admin.close()
        await offset_consumer.stop()
        influx.close()


# ── entry point ───────────────────────────────────────────────────────────────

async def _main() -> None:
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await run(stop)


if __name__ == "__main__":
    asyncio.run(_main())
