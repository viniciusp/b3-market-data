"""Per-source freshness of the serving store.

Degradation is surfaced, never an error: the API serves last-known-good data by
design, so /health always answers 200 and reports staleness honestly.
"""

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

SAO_PAULO = ZoneInfo("America/Sao_Paulo")
# Mirrors the poller's market window (ingestion.poller).
WINDOW_START = time(9, 55)
WINDOW_END = time(18, 35)
# One poll cycle (10 min) + B3's snapshot delay (~15 min) + margin.
TRADES_MAX_AGE = timedelta(minutes=30)
# Corporate actions sync cadence (6 h) + margin.
CORPORATE_ACTIONS_MAX_AGE = timedelta(hours=7)

CHAIN_NOTE = "freshness here proves the whole chain: poller -> kafka -> risingwave -> postgres"


def market_window_open(now: datetime) -> bool:
    local = now.astimezone(SAO_PAULO)
    return local.weekday() < 5 and WINDOW_START <= local.time() <= WINDOW_END


def evaluate_health(
    now: datetime,
    latest_trade_at: datetime | None,
    corporate_actions_synced_at: datetime | None,
    latest_session: date | None,
    session_count: int,
) -> dict:
    """Pure evaluation: all clock and threshold logic lives here, testable."""
    window_open = market_window_open(now)
    trades_age = now - latest_trade_at if latest_trade_at else None
    # Outside the market window the newest trade is naturally old; only judge
    # staleness while B3 is actually producing data.
    trades_fresh = latest_trade_at is not None and (not window_open or trades_age <= TRADES_MAX_AGE)
    actions_age = now - corporate_actions_synced_at if corporate_actions_synced_at else None
    actions_fresh = actions_age is not None and actions_age <= CORPORATE_ACTIONS_MAX_AGE
    return {
        "status": "ok" if trades_fresh and actions_fresh else "degraded",
        "checked_at": now.isoformat(),
        "market_window_open": window_open,
        "sources": {
            "trades": {
                "latest_traded_at": latest_trade_at.isoformat() if latest_trade_at else None,
                "age_seconds": int(trades_age.total_seconds()) if trades_age else None,
                "fresh": trades_fresh,
                "note": CHAIN_NOTE,
            },
            "corporate_actions": {
                "last_synced_at": (
                    corporate_actions_synced_at.isoformat() if corporate_actions_synced_at else None
                ),
                "age_seconds": int(actions_age.total_seconds()) if actions_age else None,
                "fresh": actions_fresh,
            },
            "sessions": {
                "latest": latest_session.isoformat() if latest_session else None,
                "count": session_count,
            },
        },
    }


def utcnow() -> datetime:
    return datetime.now(UTC)
