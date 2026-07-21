"""Track how far each (ticker, session) has been published, in a compacted Kafka topic.

Each record holds the max published trade_id for one key, e.g. "PETR4:2026-07-14".
Trade ids are sequential per instrument WITHIN a session, so the key must include
the session date: after-market trades of session D appear in D+1 files still
carrying D's id sequence.

Reading state = consume the topic start to end, last value per key wins. Log
compaction eventually drops superseded versions; that is a size optimization,
never a correctness requirement.
"""

import json
import logging
import os
from collections.abc import Iterable, Iterator
from datetime import date

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from ingestion.trades.models import ACTION_CANCELLED, Trade

logger = logging.getLogger(__name__)

WATERMARKS_TOPIC = "trades.watermarks"


def watermark_key(ticker: str, trade_date: date) -> str:
    return f"{ticker}:{trade_date.isoformat()}"


def filter_new_trades(trades: Iterable[Trade], watermarks: dict[str, int]) -> list[Trade]:
    """Keep trades above their session watermark.

    Cancellations always pass: they reuse the id of the trade they cancel, which
    is below the watermark by definition. The sink deduplicates re-published ones.
    """
    return [
        trade
        for trade in trades
        if trade.action == ACTION_CANCELLED
        or trade.trade_id > watermarks.get(watermark_key(trade.ticker, trade.trade_date), 0)
    ]


def updated_watermarks(trades: Iterable[Trade]) -> dict[str, int]:
    """Max published trade_id per (ticker, session), ignoring cancellations."""
    maxes: dict[str, int] = {}
    for trade in trades:
        if trade.action == ACTION_CANCELLED:
            continue
        key = watermark_key(trade.ticker, trade.trade_date)
        maxes[key] = max(maxes.get(key, 0), trade.trade_id)
    return maxes


def fold_watermark_records(records: Iterable[tuple[str, bytes | None]]) -> dict[str, int]:
    """Last value per key wins; a null value (compaction tombstone) clears the key."""
    state: dict[str, int] = {}
    for key, value in records:
        if value is None:
            state.pop(key, None)
        else:
            state[key] = json.loads(value)["max_trade_id"]
    return state


def publish_watermarks(producer: Producer, updates: dict[str, int]) -> None:
    for key, max_trade_id in updates.items():
        producer.produce(
            WATERMARKS_TOPIC,
            key=key.encode(),
            value=json.dumps({"max_trade_id": max_trade_id}).encode(),
        )
    producer.flush()


def load_watermarks() -> dict[str, int]:
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
            "group.id": "watermark-loader",  # required by the client; unused with assign()
            "enable.auto.commit": False,
        }
    )
    try:
        records = list(_consume_all(consumer))
    finally:
        consumer.close()
    state = fold_watermark_records(records)
    logger.info("watermarks loaded keys=%d records=%d", len(state), len(records))
    return state


def _consume_all(consumer: Consumer) -> Iterator[tuple[str, bytes | None]]:
    metadata = consumer.list_topics(WATERMARKS_TOPIC, timeout=10)
    topic = metadata.topics[WATERMARKS_TOPIC]
    if topic.error is not None:
        raise KafkaException(topic.error)
    for partition_id in topic.partitions:
        partition = TopicPartition(WATERMARKS_TOPIC, partition_id)
        low, high = consumer.get_watermark_offsets(partition, timeout=10)
        if high == low:
            continue
        consumer.assign([TopicPartition(WATERMARKS_TOPIC, partition_id, low)])
        position = low
        while position < high:
            message = consumer.poll(timeout=10)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            yield message.key().decode(), message.value()
            position = message.offset() + 1
        consumer.unassign()
