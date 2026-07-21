"""Tracked B3 companies.

`issuing_company` and `trading_name` are the exact keys B3's corporate actions API expects
(GetListedSupplementCompany and GetListedCashDividends respectively).
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class TrackedTicker:
    ticker: str
    company: str
    issuing_company: str
    trading_name: str


WATCHLIST: tuple[TrackedTicker, ...] = (
    TrackedTicker("ABEV3", "Ambev", "ABEV", "AMBEV S/A"),
    TrackedTicker("B3SA3", "B3", "B3SA", "B3"),
    TrackedTicker("BBAS3", "Banco do Brasil", "BBAS", "BRASIL"),
    TrackedTicker("BBDC4", "Banco Bradesco", "BBDC", "BRADESCO"),
    TrackedTicker("ITSA4", "Itaúsa", "ITSA", "ITAUSA"),
    TrackedTicker("ITUB4", "Itaú Unibanco", "ITUB", "ITAUUNIBANCO"),
    TrackedTicker("MGLU3", "Magazine Luiza", "MGLU", "MAGAZ LUIZA"),
    TrackedTicker("PETR4", "Petrobras", "PETR", "PETROBRAS"),
    TrackedTicker("VALE3", "Vale", "VALE", "VALE"),
    TrackedTicker("WEGE3", "WEG", "WEGE", "WEG"),
)
