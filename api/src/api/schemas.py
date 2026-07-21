"""Response schemas: the public contract, rendered into OpenAPI and enforced on output.

Money always travels as strings to preserve exact decimal values; dates and
timestamps are ISO-8601 strings (timestamps in UTC).
"""

from pydantic import BaseModel, Field


class PendingDividend(BaseModel):
    ticker: str = Field(examples=["PETR4"])
    type: str = Field(
        description="B3 label: DIVIDENDO, JRS CAP PROPRIO (JCP) or RENDIMENTO",
        examples=["JRS CAP PROPRIO"],
    )
    status: str = Field(
        description=(
            "with_rights: buying today still earns this payout (today <= buy_by). "
            "pending_payment: past the buy-by date; only holders as of buy_by receive."
        ),
        examples=["pending_payment"],
    )
    gross_per_share: str = Field(
        description="Rate per share as published by B3 (gross)", examples=["0.35048636000"]
    )
    net_per_share: str = Field(
        description="Gross minus 15% IRRF for JCP; equals gross for plain dividends",
        examples=["0.2979134060000"],
    )
    tax_note: str
    approved_on: str = Field(description="Announcement date", examples=["2026-05-11"])
    buy_by: str = Field(
        description=(
            "Last day on which buying the stock still earns this payout; "
            "from the next session onward it trades without the right"
        ),
        examples=["2026-06-01"],
    )
    payment_date: str | None = Field(
        description="Null while B3 lists the payment date as still to be defined",
        examples=["2026-08-20"],
    )
    payment_tbd: bool
    yield_on_last_price: float = Field(
        description="gross_per_share divided by last_price", examples=[0.008468]
    )
    last_price: str = Field(examples=["41.390"])
    price_as_of: str = Field(description="Timestamp of the trade behind last_price (UTC)")


class SessionOHLCV(BaseModel):
    date: str = Field(examples=["2026-07-21"])
    open: str | None = Field(
        description=(
            "OHLC covers regular-session trades only (B3's official convention); "
            "null before the day's first regular trade"
        )
    )
    high: str | None
    low: str | None
    close: str | None = Field(description="Last regular-session trade; matches the official close")
    quantity: int = Field(description="Shares traded, all session types included")
    financial_volume: str | None
    trades: int = Field(description="Trade count, all session types included")


class TickerSummary(BaseModel):
    ticker: str = Field(examples=["PETR4"])
    last_price: str = Field(examples=["41.390"])
    price_as_of: str
    latest_session: SessionOHLCV | None = Field(
        description="Most recent session seen for the ticker; in-flight during market hours"
    )
    pending_dividends: list[PendingDividend]


class TradesFreshness(BaseModel):
    latest_traded_at: str | None
    age_seconds: int | None
    fresh: bool = Field(
        description="Judged only during market hours: an old price on a closed market is healthy"
    )
    note: str


class CorporateActionsFreshness(BaseModel):
    last_synced_at: str | None
    age_seconds: int | None
    fresh: bool = Field(description="Sync cadence is 6h; stale past 7h regardless of market hours")


class SessionsInfo(BaseModel):
    latest: str | None = Field(examples=["2026-07-21"])
    count: int


class HealthSources(BaseModel):
    trades: TradesFreshness
    corporate_actions: CorporateActionsFreshness
    sessions: SessionsInfo


class HealthReport(BaseModel):
    status: str = Field(
        description="ok | degraded. Always HTTP 200: staleness is surfaced, never an error",
        examples=["ok"],
    )
    checked_at: str
    market_window_open: bool
    sources: HealthSources
