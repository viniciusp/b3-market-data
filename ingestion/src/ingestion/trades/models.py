"""Parse B3 trade file lines into typed trades.

Lines are semicolon separated:

    2026-07-14;PETR4;0;41,200;5500;100719126;10;1;2026-07-14;85;8

    #  field                        example     meaning
    0  DataReferencia               2026-07-14  session file the line came from
    1  CodigoInstrumento            PETR4       ticker
    2  AcaoAtualizacao              0           0 = new trade, 2 = cancellation
    3  PrecoNegocio                 41,200      price (comma as decimal separator)
    4  QuantidadeNegociada          5500        quantity
    5  HoraFechamento               100719126   HHMMSSmmm, Sao Paulo wall clock
    6  CodigoIdentificadorNegocio   10          trade id, sequential per instrument/session
    7  TipoSessaoPregao             1           1 = regular session, 6 = after-market
    8  DataNegocio                  2026-07-14  session the trade belongs to
    9  CodigoParticipanteComprador  85          buyer broker code (may be empty)
    10 CodigoParticipanteVendedor   8           seller broker code (may be empty)
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

SAO_PAULO = ZoneInfo("America/Sao_Paulo")

ACTION_NEW = 0
ACTION_CANCELLED = 2


class MalformedTradeError(Exception):
    """Raised when a line does not match the expected B3 trade format."""


@dataclass(frozen=True)
class Trade:
    ticker: str
    trade_id: int  # sequential per instrument within a session
    action: int  # ACTION_NEW or ACTION_CANCELLED (cancellations reuse the original trade_id)
    price: Decimal
    quantity: int
    traded_at: datetime  # instant in UTC (B3 publishes Sao Paulo wall-clock time)
    trade_date: date  # session the trade belongs to (DataNegocio), a Sao Paulo calendar date
    reference_date: date  # session file it came from; after-market rows land on the next file
    session_type: int  # 1 = regular session, 6 = after-market


def parse_trade(line: str) -> Trade:
    fields = line.split(";")
    if len(fields) != 11:
        raise MalformedTradeError(f"expected 11 fields, got {len(fields)}: {line!r}")
    try:
        trade_date = date.fromisoformat(fields[8])
        return Trade(
            ticker=fields[1],
            trade_id=int(fields[6]),
            action=int(fields[2]),
            price=Decimal(fields[3].replace(",", ".")),
            quantity=int(fields[4]),
            traded_at=_combine(trade_date, fields[5]),
            trade_date=trade_date,
            reference_date=date.fromisoformat(fields[0]),
            session_type=int(fields[7]),
        )
    except (ValueError, InvalidOperation) as exc:
        raise MalformedTradeError(f"{exc}: {line!r}") from exc


def parse_trades(lines: Iterable[str]) -> tuple[list[Trade], list[tuple[str, str]]]:
    """Parse lines into trades, partitioning out malformed ones as (line, error) pairs.

    Pure function: no logging or I/O. The caller decides what to do with
    malformed lines (log, count, route to a dead letter queue).
    """
    trades: list[Trade] = []
    malformed: list[tuple[str, str]] = []
    for line in lines:
        try:
            trades.append(parse_trade(line))
        except MalformedTradeError as exc:
            malformed.append((line, str(exc)))
    return trades, malformed


def _combine(trade_date: date, time_raw: str) -> datetime:
    # HoraFechamento is HHMMSSmmm (milliseconds, no separators), Sao Paulo wall clock.
    if len(time_raw) != 9:
        raise ValueError(f"expected 9-digit HHMMSSmmm time, got {time_raw!r}")
    local = datetime(
        trade_date.year,
        trade_date.month,
        trade_date.day,
        hour=int(time_raw[0:2]),
        minute=int(time_raw[2:4]),
        second=int(time_raw[4:6]),
        microsecond=int(time_raw[6:9]) * 1000,
        tzinfo=SAO_PAULO,
    )
    return local.astimezone(UTC)
