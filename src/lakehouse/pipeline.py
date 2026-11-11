"""The whole pipeline as plain functions, used by the CLI, the tests and the Dagster assets."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

from . import bronze, gold, landing, silver
from .catalog import get_catalog
from .config import Config
from .contract import Contract, load_contract

log = logging.getLogger(__name__)


def month_range(spec: str) -> list[str]:
    """'2025-01' -> ['2025-01'];  '2025-01:2025-03' -> ['2025-01', '2025-02', '2025-03']."""
    start, _, end = spec.partition(":")
    end = end or start
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    if (y, m) > (ey, em):
        raise ValueError(f"empty month range {spec!r}")
    out = []
    while (y, m) <= (ey, em):
        date(y, m, 1)  # validates
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


@dataclass
class RunSummary:
    bronze: list[bronze.BronzeResult] = field(default_factory=list)
    silver: list[silver.SilverResult] = field(default_factory=list)
    gold: gold.GoldResult | None = None


def load_month(cfg: Config, month: str, contract: Contract | None = None) -> tuple:
    contract = contract or load_contract()
    cat = get_catalog(cfg)
    path = landing.land(cfg, month)
    b = bronze.load_bronze(cat, contract, month, path)
    s = silver.load_silver(cat, contract, month)
    return b, s


def run(cfg: Config, months: list[str], contract: Contract | None = None) -> RunSummary:
    contract = contract or load_contract()
    summary = RunSummary()
    for month in months:
        b, s = load_month(cfg, month, contract)
        summary.bronze.append(b)
        summary.silver.append(s)
    summary.gold = gold.build_gold(get_catalog(cfg))
    return summary
