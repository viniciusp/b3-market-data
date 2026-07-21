from datetime import UTC, date, datetime, timedelta

from api.health import evaluate_health, market_window_open

# 2026-07-21 is a Tuesday; 13:00 UTC = 10:00 Sao Paulo (window open).
OPEN = datetime(2026, 7, 21, 13, 0, tzinfo=UTC)
# 01:00 UTC = 22:00 Sao Paulo the previous evening (window closed).
CLOSED = datetime(2026, 7, 21, 1, 0, tzinfo=UTC)
SESSION = date(2026, 7, 21)


def healthy_kwargs(now):
    return dict(
        latest_trade_at=now - timedelta(minutes=5),
        corporate_actions_synced_at=now - timedelta(hours=1),
        latest_session=SESSION,
        session_count=210,
    )


def test_everything_fresh_is_ok():
    report = evaluate_health(OPEN, **healthy_kwargs(OPEN))
    assert report["status"] == "ok"
    assert report["market_window_open"] is True
    assert report["sources"]["trades"]["fresh"] is True


def test_stale_trades_during_market_hours_degrade():
    kwargs = healthy_kwargs(OPEN) | {"latest_trade_at": OPEN - timedelta(hours=2)}
    report = evaluate_health(OPEN, **kwargs)
    assert report["status"] == "degraded"
    assert report["sources"]["trades"]["fresh"] is False


def test_old_trades_outside_market_hours_are_fine():
    kwargs = healthy_kwargs(CLOSED) | {"latest_trade_at": CLOSED - timedelta(hours=8)}
    report = evaluate_health(CLOSED, **kwargs)
    assert report["status"] == "ok"
    assert report["market_window_open"] is False


def test_stale_corporate_actions_degrade_even_off_hours():
    kwargs = healthy_kwargs(CLOSED) | {
        "latest_trade_at": CLOSED - timedelta(hours=8),
        "corporate_actions_synced_at": CLOSED - timedelta(hours=10),
    }
    assert evaluate_health(CLOSED, **kwargs)["status"] == "degraded"


def test_empty_store_is_degraded_not_crash():
    report = evaluate_health(
        OPEN,
        latest_trade_at=None,
        corporate_actions_synced_at=None,
        latest_session=None,
        session_count=0,
    )
    assert report["status"] == "degraded"
    assert report["sources"]["trades"]["latest_traded_at"] is None


def test_weekend_is_outside_window():
    assert not market_window_open(datetime(2026, 7, 25, 13, 0, tzinfo=UTC))  # Saturday
