from datetime import UTC, date, datetime

from ingestion.poller import in_polling_window, recent_weekdays

# 2026-07-14 is a Tuesday; Sao Paulo is UTC-3.


def test_open_market_hours_are_inside_window():
    assert in_polling_window(datetime(2026, 7, 14, 13, 0, tzinfo=UTC))  # 10:00 SP


def test_before_open_is_outside_window():
    assert not in_polling_window(datetime(2026, 7, 14, 12, 30, tzinfo=UTC))  # 09:30 SP


def test_evening_is_outside_window():
    assert not in_polling_window(datetime(2026, 7, 14, 22, 0, tzinfo=UTC))  # 19:00 SP


def test_after_market_is_inside_window():
    assert in_polling_window(datetime(2026, 7, 14, 21, 0, tzinfo=UTC))  # 18:00 SP


def test_weekend_is_outside_window():
    assert not in_polling_window(datetime(2026, 7, 18, 13, 0, tzinfo=UTC))  # Saturday 10:00 SP


def test_recent_weekdays_skips_weekends_oldest_first():
    days = recent_weekdays(date(2026, 7, 15), calendar_days=7)
    assert days == [
        date(2026, 7, 8),
        date(2026, 7, 9),
        date(2026, 7, 10),
        date(2026, 7, 13),  # 11-12 Jul (Sat/Sun) skipped
        date(2026, 7, 14),
    ]


def test_recent_weekdays_excludes_today():
    assert date(2026, 7, 15) not in recent_weekdays(date(2026, 7, 15))
