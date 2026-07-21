"""Read-only HTTP API over the serving store.

Run with: uvicorn api.main:app
"""

from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse
from psycopg.rows import dict_row

from api.caching import cache_control, make_etag, ttl_for
from api.db import create_pool
from api.health import evaluate_health, utcnow
from api.schemas import HealthReport, PendingDividend, TickerSummary

STATIC_DIR = Path(__file__).parent / "static"

JCP_LABEL = "JRS CAP PROPRIO"
JCP_WITHHOLDING = Decimal("0.15")  # IRRF withheld at source; the published rate is gross

# Dividend state is a function of the clock (Sao Paulo calendar), so it is
# computed at query time and never materialized. Mirrors
# ingestion.corporate_actions.api.dividend_status.
PENDING_QUERY = """
WITH today AS (
    SELECT (now() AT TIME ZONE 'America/Sao_Paulo')::date AS today
)
SELECT d.ticker,
       d.label,
       d.rate,
       d.approved_on,
       d.last_date_prior,
       d.payment_date,
       CASE WHEN t.today <= d.last_date_prior THEN 'with_rights'
            ELSE 'pending_payment' END AS status,
       p.price,
       p.traded_at
FROM cash_dividends d
JOIN last_price p USING (ticker)
CROSS JOIN today t
WHERE (d.payment_date IS NULL OR d.payment_date > t.today)
  AND (%(ticker)s::text IS NULL OR d.ticker = %(ticker)s)
ORDER BY d.ticker, d.payment_date NULLS LAST
"""

LAST_PRICE_QUERY = "SELECT ticker, price, traded_at FROM last_price WHERE ticker = %(ticker)s"

KNOWN_TICKERS_QUERY = "SELECT ticker FROM last_price ORDER BY ticker"

LATEST_SESSION_QUERY = """
SELECT trade_date, open, high, low, close, quantity, financial_volume, trades
FROM session_summary
WHERE ticker = %(ticker)s
ORDER BY trade_date DESC
LIMIT 1
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = create_pool()
    app.state.pool.open()
    yield
    app.state.pool.close()


app = FastAPI(
    title="B3 Market Data API",
    description=(
        "Read-only market data for the tracked B3 tickers. Responses carry `ETag` and "
        "`Cache-Control` headers; conditional requests with `If-None-Match` are answered "
        "with `304 Not Modified`."
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def cache_headers(request: Request, call_next):
    """Add ETag + Cache-Control to cacheable GETs and answer If-None-Match with 304."""
    response = await call_next(request)
    ttl = ttl_for(request.url.path)
    if request.method != "GET" or response.status_code != 200 or ttl is None:
        return response
    body = b"".join([chunk async for chunk in response.body_iterator])
    etag = make_etag(body)
    headers = {"etag": etag, "cache-control": cache_control(ttl)}
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)
    return Response(
        content=body,
        status_code=200,
        headers=headers,
        media_type=response.media_type,
    )


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


def pending_item(row: dict) -> dict:
    """Shape one query row into the public payload. Money travels as strings."""
    gross = row["rate"]
    is_jcp = row["label"] == JCP_LABEL
    net = gross * (1 - JCP_WITHHOLDING) if is_jcp else gross
    return {
        "ticker": row["ticker"],
        "type": row["label"],
        "status": row["status"],
        "gross_per_share": str(gross),
        "net_per_share": str(net),
        "tax_note": (
            "JCP: 15% IRRF withheld at source; published rate is gross"
            if is_jcp
            else "dividends currently exempt from IRRF for individuals"
        ),
        "approved_on": row["approved_on"].isoformat(),
        "buy_by": row["last_date_prior"].isoformat(),
        "payment_date": row["payment_date"].isoformat() if row["payment_date"] else None,
        "payment_tbd": row["payment_date"] is None,
        "yield_on_last_price": round(float(gross / row["price"]), 6),
        "last_price": str(row["price"]),
        "price_as_of": row["traded_at"].isoformat(),
    }


def money(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def session_item(row: dict) -> dict:
    """Shape one session_summary row; OHLC can be NULL before the first regular trade."""
    return {
        "date": row["trade_date"].isoformat(),
        "open": money(row["open"]),
        "high": money(row["high"]),
        "low": money(row["low"]),
        "close": money(row["close"]),
        "quantity": row["quantity"],
        "financial_volume": money(row["financial_volume"]),
        "trades": row["trades"],
    }


@app.get("/dividends/pending", tags=["dividends"], response_model=list[PendingDividend])
def dividends_pending() -> list[dict]:
    """Cash dividends announced but not yet paid, joined with the latest stream price."""
    with app.state.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cursor:
        rows = cursor.execute(PENDING_QUERY, {"ticker": None}).fetchall()
    return [pending_item(row) for row in rows]


@app.get(
    "/tickers/{ticker}/summary",
    tags=["tickers"],
    response_model=TickerSummary,
    responses={
        404: {
            "description": "Ticker not tracked; the body lists the available tickers",
            "content": {
                "application/json": {
                    "example": {
                        "detail": {
                            "error": "ticker XPTO4 is not tracked",
                            "available_tickers": ["ABEV3", "B3SA3", "PETR4"],
                        }
                    }
                }
            },
        }
    },
)
def ticker_summary(ticker: str) -> dict:
    """Latest price, most recent session OHLCV, and pending dividends for one ticker."""
    ticker = ticker.strip().upper()
    with app.state.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cursor:
        price_row = cursor.execute(LAST_PRICE_QUERY, {"ticker": ticker}).fetchone()
        if price_row is None:
            known = [r["ticker"] for r in cursor.execute(KNOWN_TICKERS_QUERY).fetchall()]
            raise HTTPException(
                status_code=404,
                detail={"error": f"ticker {ticker} is not tracked", "available_tickers": known},
            )
        session_row = cursor.execute(LATEST_SESSION_QUERY, {"ticker": ticker}).fetchone()
        pending_rows = cursor.execute(PENDING_QUERY, {"ticker": ticker}).fetchall()
    return {
        "ticker": ticker,
        "last_price": str(price_row["price"]),
        "price_as_of": price_row["traded_at"].isoformat(),
        "latest_session": session_item(session_row) if session_row else None,
        "pending_dividends": [pending_item(row) for row in pending_rows],
    }


HEALTH_QUERY = """
SELECT (SELECT max(traded_at) FROM last_price)        AS latest_trade_at,
       (SELECT max(synced_at) FROM cash_dividends)    AS corporate_actions_synced_at,
       (SELECT max(trade_date) FROM session_summary)  AS latest_session,
       (SELECT count(*) FROM session_summary)         AS session_count
"""


@app.get("/health", tags=["operations"], response_model=HealthReport)
def health() -> dict:
    """Freshness per source. Always 200: stale data is surfaced, not an error."""
    with app.state.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cursor:
        row = cursor.execute(HEALTH_QUERY).fetchone()
    return evaluate_health(utcnow(), **row)
