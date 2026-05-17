import asyncio
import logging
import os
import sys
from collections.abc import Sequence

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

logger = logging.getLogger("lag-monitor")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", "k6")
POLL_INTERVAL = int(os.getenv("LAG_POLL_INTERVAL", "5"))

CONSUMER_GROUPS: dict[str, Sequence[str]] = {
    "inventory-service": ["orders.created", "orders.confirmed", "orders.rejected"],
    "order-service": ["inventory.reserved", "inventory.rejected"],
    "email-service": ["orders.confirmed", "orders.rejected"],
}

ALL_TOPICS = list({t for ts in CONSUMER_GROUPS.values() for t in ts})


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    logger.info("starting lag monitor")

    meta_consumer = AIOKafkaConsumer(
        *ALL_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
    )
    await meta_consumer.start()

    # Get topic partition metadata once at startup
    all_partitions: dict[str, set[int]] = {}
    for topic in ALL_TOPICS:
        all_partitions[topic] = meta_consumer.partitions_for_topic(topic) or set()

    influx = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_DATABASE,
        org="-",
    )
    write_api = influx.write_api(write_options=SYNCHRONOUS)

    try:
        while True:
            try:
                for group_id, topics in CONSUMER_GROUPS.items():
                    # Create a lightweight consumer just to query committed offsets
                    # for this group. Each consumer is created per-tick to avoid
                    # interfering with the real consumer group membership.
                    group_consumer = AIOKafkaConsumer(
                        *topics,
                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                        group_id=group_id,
                        enable_auto_commit=False,
                    )
                    await group_consumer.start()

                    try:
                        for topic in topics:
                            partitions = all_partitions.get(topic, set())
                            if not partitions:
                                continue

                            tps = [
                                TopicPartition(topic, p) for p in partitions
                            ]

                            committed_map = {}
                            for tp in tps:
                                committed_map[tp] = (
                                    await group_consumer.committed(tp) or 0
                                )

                            end_map = await meta_consumer.end_offsets(tps)

                            for tp in tps:
                                lag = max(0, end_map.get(tp, 0) - committed_map[tp])

                                if lag < 0:
                                    continue

                                point = (
                                    Point("consumer_lag")
                                    .tag("group", group_id)
                                    .tag("topic", tp.topic)
                                    .tag("partition", str(tp.partition))
                                    .field("lag", lag)
                                )
                                write_api.write(
                                    bucket=f"{INFLUXDB_DATABASE}/autogen",
                                    record=point,
                                )

                            logger.debug(
                                "group=%s topic=%s partitions=%d",
                                group_id,
                                topic,
                                len(partitions),
                            )
                    finally:
                        await group_consumer.stop()

            except KafkaError:
                logger.exception("kafka error during lag poll")
            except Exception:
                logger.exception("unexpected error during lag poll")

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await meta_consumer.stop()
        influx.close()


if __name__ == "__main__":
    asyncio.run(main())
