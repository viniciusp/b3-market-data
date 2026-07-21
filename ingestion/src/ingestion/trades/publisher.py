"""Publish trades and dead letters to their Kafka topics."""

import logging
import os
from collections.abc import Iterable
from datetime import date

from confluent_kafka import Producer

from ingestion.trades.events import serialize_dead_letter, serialize_trade
from ingestion.trades.models import Trade

logger = logging.getLogger(__name__)

TRADES_TOPIC = "trades.raw"
DLQ_TOPIC = "trades.dlq"


class PublishError(Exception):
    """Raised when one or more deliveries fail; callers must not advance state."""


def create_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
            # Deduplicates broker-side retries within this producer session.
            "enable.idempotence": True,
            "compression.type": "zstd",
        }
    )


def publish_trades(producer: Producer, trades: Iterable[Trade], source: str) -> int:
    """Publish trades keyed by ticker, so each instrument stays ordered in one partition.

    Blocks until every delivery is confirmed and raises PublishError if any failed.
    """
    failures: list[str] = []

    def on_delivery(error, _message) -> None:
        if error is not None:
            failures.append(str(error))

    count = 0
    for trade in trades:
        producer.produce(
            TRADES_TOPIC,
            key=trade.ticker.encode(),
            value=serialize_trade(trade, source),
            on_delivery=on_delivery,
        )
        count += 1
        if count % 1000 == 0:
            producer.poll(0)  # serve delivery callbacks so the local queue never fills up
    producer.flush()
    if failures:
        raise PublishError(f"{len(failures)} of {count} deliveries failed: {failures[0]}")
    return count


def publish_dead_letters(
    producer: Producer,
    malformed: Iterable[tuple[str, str]],
    ticker: str,
    session_date: date,
    filename: str,
    source: str,
) -> int:
    """Route (line, error) pairs to the dead letter topic instead of dropping them."""
    failures: list[str] = []

    def on_delivery(error, _message) -> None:
        if error is not None:
            failures.append(str(error))

    count = 0
    for raw_line, error in malformed:
        producer.produce(
            DLQ_TOPIC,
            key=ticker.encode(),
            value=serialize_dead_letter(raw_line, error, ticker, session_date, filename, source),
            on_delivery=on_delivery,
        )
        count += 1
    producer.flush()
    if failures:
        raise PublishError(f"{len(failures)} of {count} dead letters failed: {failures[0]}")
    return count
