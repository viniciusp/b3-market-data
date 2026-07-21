"""Long-running capture service for the watchlist.

Publishes new trades to Kafka during market hours, backfills recent sessions at
boot and daily, and syncs corporate actions into Postgres every few hours.
"""

import logging
import os
from datetime import UTC, date, datetime, time, timedelta
from time import sleep

import httpx
from confluent_kafka import Producer

from ingestion.corporate_actions import storage
from ingestion.corporate_actions.sync import sync_corporate_actions
from ingestion.logging_setup import configure_logging
from ingestion.timezones import SAO_PAULO
from ingestion.trades.files import REQUEST_TIMEOUT_SECONDS, TradeFileError, fetch_trade_file
from ingestion.trades.models import parse_trades
from ingestion.trades.publisher import create_producer, publish_dead_letters, publish_trades
from ingestion.trades.watermarks import (
    filter_new_trades,
    load_watermarks,
    publish_watermarks,
    updated_watermarks,
)
from ingestion.watchlist import WATCHLIST

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "600"))
# B3 equities trade 10:00-17:55 Sao Paulo time, with after-market until ~18:00.
# Snapshots lag ~15 min, so the window extends a little past both ends.
WINDOW_START = time(9, 55)
WINDOW_END = time(18, 35)
# Lookback covering B3's retention of ~20 trading sessions (~28 calendar days).
BACKFILL_CALENDAR_DAYS = 28
# Corporate actions change a few times a day at most; announcements can land any time.
CA_SYNC_INTERVAL_SECONDS = int(os.environ.get("CA_SYNC_INTERVAL_SECONDS", str(6 * 3600)))


def in_polling_window(now: datetime) -> bool:
    local = now.astimezone(SAO_PAULO)
    return local.weekday() < 5 and WINDOW_START <= local.time() <= WINDOW_END


def poll_watchlist(
    client: httpx.Client,
    producer: Producer,
    watermarks: dict[str, int],
    session_date: date,
    source: str,
) -> None:
    """Fetch every watched ticker once, publishing only trades above the watermark."""
    for tracked in WATCHLIST:
        try:
            trade_file = fetch_trade_file(tracked.ticker, session_date, client)
        except TradeFileError as exc:
            # One ticker failing must never take the polling loop down.
            logger.warning("fetch failed ticker=%s reason=%s", tracked.ticker, exc)
            continue
        trades, malformed = parse_trades(trade_file.lines)
        if malformed:
            publish_dead_letters(
                producer, malformed, tracked.ticker, session_date, trade_file.filename, source
            )
            logger.warning("dead-lettered ticker=%s lines=%d", tracked.ticker, len(malformed))
        new_trades = filter_new_trades(trades, watermarks)
        published = publish_trades(producer, new_trades, source)
        # Watermarks advance only after every delivery above was confirmed:
        # a crash in between re-publishes trades but never loses them.
        updates = updated_watermarks(new_trades)
        publish_watermarks(producer, updates)
        watermarks.update(updates)
        last = new_trades[-1] if new_trades else None
        logger.info(
            "published ticker=%s file=%s total=%d new=%d last_price=%s last_traded_at=%s",
            tracked.ticker,
            trade_file.filename,
            len(trades),
            published,
            last.price if last else "-",
            last.traded_at.time() if last else "-",
        )


def recent_weekdays(today: date, calendar_days: int = BACKFILL_CALENDAR_DAYS) -> list[date]:
    """Past weekdays inside the lookback window, oldest first, excluding today.

    Weekends are skipped by date math; holidays are unknown locally and resolve
    downstream: B3 answers 404 and the fetch skips the date as unavailable.
    """
    days = (today - timedelta(days=offset) for offset in range(1, calendar_days + 1))
    return sorted(day for day in days if day.weekday() < 5)


def backfill_recent_sessions(
    client: httpx.Client,
    producer: Producer,
    watermarks: dict[str, int],
    today: date,
) -> None:
    """Fetch the final file of every recent session, publishing whatever is missing.

    Idempotent completeness check: fully covered sessions publish zero events.
    This is what heals downtime gaps before B3 deletes the files (~20 sessions).
    """
    sessions = recent_weekdays(today)
    logger.info(
        "backfill started sessions=%d range=%s..%s", len(sessions), sessions[0], sessions[-1]
    )
    for session_date in sessions:
        poll_watchlist(client, producer, watermarks, session_date, "eod_file")
    logger.info("backfill finished")


def sync_corporate_actions_safely(client: httpx.Client) -> bool:
    """Run one corporate actions sync; never raises, so it cannot kill the loop.

    Opens a fresh connection per run: cheap at this cadence and immune to a
    Postgres restart between runs. Returns False so the caller retries on the
    next cycle instead of waiting a full interval.
    """
    try:
        with storage.connect() as conn:
            storage.ensure_schema(conn)
            failures = sync_corporate_actions(conn, client)
    except Exception:
        logger.exception("corporate actions sync failed; retrying next cycle")
        return False
    if failures:
        logger.warning("corporate actions sync finished with failures=%d", failures)
    return True


def main() -> None:
    configure_logging()
    producer = create_producer()
    watermarks = load_watermarks()
    logger.info(
        "poller started interval=%ds window=%s-%s ca_interval=%ds tickers=%d",
        POLL_INTERVAL_SECONDS,
        WINDOW_START,
        WINDOW_END,
        CA_SYNC_INTERVAL_SECONDS,
        len(WATCHLIST),
    )
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            # Corporate actions first: on a fresh boot the product data is ready
            # in seconds, while the trade backfill still sweeps old sessions.
            last_ca_sync = datetime.now(UTC) if sync_corporate_actions_safely(client) else None
            last_backfill = datetime.now(UTC).astimezone(SAO_PAULO).date()
            backfill_recent_sessions(client, producer, watermarks, last_backfill)
            while True:
                now = datetime.now(UTC)
                today = now.astimezone(SAO_PAULO).date()
                ca_due = (
                    last_ca_sync is None
                    or (now - last_ca_sync).total_seconds() >= CA_SYNC_INTERVAL_SECONDS
                )
                if ca_due and sync_corporate_actions_safely(client):
                    last_ca_sync = now
                if in_polling_window(now):
                    # First in-window cycle of each day re-checks recent sessions:
                    # by 09:55 the previous session's final file is available.
                    if last_backfill != today:
                        backfill_recent_sessions(client, producer, watermarks, today)
                        last_backfill = today
                    poll_watchlist(client, producer, watermarks, today, "intraday_poll")
                else:
                    logger.info("outside polling window, idle")
                sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("poller stopped")


if __name__ == "__main__":
    main()
