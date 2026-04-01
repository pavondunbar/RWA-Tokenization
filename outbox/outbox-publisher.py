import asyncio
import json
import logging
import os
import uuid

import asyncpg
from aiokafka import AIOKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(
        self, db, kafka_producer,
        batch_size=100, max_retries=5
    ):
        self.db = db
        self.kafka = kafka_producer
        self.batch_size = batch_size
        self.max_retries = max_retries

    async def poll_and_publish(self):
        async with self.db.acquire() as conn:
            async with conn.transaction():
                events = await conn.fetch(
                    "SELECT oe.id, oe.aggregate_id, "
                    "  oe.event_type, oe.payload "
                    "FROM outbox_events oe "
                    "LEFT JOIN outbox_published op "
                    "  ON op.event_id = oe.id "
                    "LEFT JOIN outbox_dlq dlq "
                    "  ON dlq.event_id = oe.id "
                    "WHERE op.id IS NULL "
                    "AND dlq.id IS NULL "
                    "ORDER BY oe.created_at "
                    "LIMIT $1 "
                    "FOR UPDATE OF oe SKIP LOCKED",
                    self.batch_size,
                )

                if not events:
                    return

                logger.info(
                    "Found %d unpublished event(s)",
                    len(events),
                )

                for event in events:
                    event_id = event["id"]

                    prior_attempts = await conn.fetchval(
                        "SELECT COUNT(*) FROM "
                        "outbox_publish_attempts "
                        "WHERE event_id = $1",
                        event_id,
                    )

                    if prior_attempts >= self.max_retries:
                        await conn.execute(
                            "INSERT INTO outbox_dlq "
                            "(id, event_id, "
                            " error_message, attempts, "
                            " created_at) "
                            "VALUES ($1,$2,$3,$4,NOW())",
                            uuid.uuid4(),
                            event_id,
                            "Max retries exceeded",
                            prior_attempts,
                        )
                        try:
                            dlq_topic = (
                                "rwa.dlq."
                                f"{event['event_type']}"
                            )
                            payload = event["payload"]
                            if not isinstance(
                                payload, str
                            ):
                                payload = json.dumps(
                                    payload
                                )
                            await self.kafka.send(
                                topic=dlq_topic,
                                key=event[
                                    "aggregate_id"
                                ].encode(),
                                value=payload.encode(),
                            )
                        except Exception:
                            pass
                        logger.warning(
                            "Event %s moved to DLQ "
                            "after %d attempts",
                            event_id,
                            prior_attempts,
                        )
                        continue

                    topic = f"rwa.{event['event_type']}"
                    payload = event["payload"]
                    if not isinstance(payload, str):
                        payload = json.dumps(payload)

                    try:
                        await self.kafka.send(
                            topic=topic,
                            key=event[
                                "aggregate_id"
                            ].encode(),
                            value=payload.encode(),
                        )
                    except Exception as exc:
                        await conn.execute(
                            "INSERT INTO "
                            "outbox_publish_attempts "
                            "(id, event_id, "
                            " error_message, "
                            " attempted_at) "
                            "VALUES ($1,$2,$3,NOW())",
                            uuid.uuid4(),
                            event_id,
                            str(exc)[:1000],
                        )
                        logger.error(
                            "Failed to publish event "
                            "%s: %s",
                            event_id,
                            exc,
                        )
                        continue

                    logger.info(
                        "Published event %s -> %s",
                        event_id,
                        topic,
                    )

                    await conn.execute(
                        "INSERT INTO outbox_published "
                        "(id, event_id, published_at) "
                        "VALUES ($1, $2, NOW())",
                        uuid.uuid4(),
                        event_id,
                    )

    async def run_forever(self, poll_interval=1):
        while True:
            try:
                await self.poll_and_publish()
            except Exception as e:
                logger.error("Outbox poll failed: %s", e)
            await asyncio.sleep(poll_interval)


async def main():
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "rwa")
    db_user = os.environ.get("DB_USER", "readonly_user")
    db_pass = os.environ.get("DB_PASSWORD", "")
    dsn = (
        f"postgresql://{db_user}:{db_pass}"
        f"@{db_host}:{db_port}/{db_name}"
    )
    kafka_broker = os.environ.get(
        "KAFKA_BROKER", "localhost:9092"
    )

    max_retries = 10
    retry_delay = 2
    pool = None
    for attempt in range(1, max_retries + 1):
        try:
            pool = await asyncpg.create_pool(dsn)
            break
        except (OSError, asyncpg.PostgresError) as exc:
            if attempt == max_retries:
                raise
            print(
                f"[DB] Connection attempt {attempt}/{max_retries}"
                f" failed: {exc}. Retrying in {retry_delay}s..."
            )
            await asyncio.sleep(retry_delay)

    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_broker,
    )
    for attempt in range(1, max_retries + 1):
        try:
            await producer.start()
            break
        except Exception as exc:
            if attempt == max_retries:
                await pool.close()
                raise SystemExit(
                    f"Failed to connect to Kafka: {exc}\n"
                    "Ensure Kafka is running."
                )
            print(
                f"[Kafka] Connection attempt "
                f"{attempt}/{max_retries}"
                f" failed: {exc}. Retrying in {retry_delay}s..."
            )
            await asyncio.sleep(retry_delay)

    publisher = OutboxPublisher(
        db=pool, kafka_producer=producer
    )
    print("OutboxPublisher started — polling for events...")
    print("Press Ctrl+C to stop.\n")

    try:
        await publisher.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print("\nShutting down...")
        await producer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
