"""The whole pipeline as plain functions, used by the CLI, the tests and the Dagster assets."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

from . import audit, bronze, gold, landing, silver
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
    run_id: str = ""
    bronze: list[bronze.BronzeResult] = field(default_factory=list)
    silver: list[silver.SilverResult] = field(default_factory=list)
    gold: gold.GoldResult | None = None


def load_month(cfg: Config, month: str, contract: Contract | None = None, run_id: str | None = None) -> tuple:
    """Land, load and validate one month; every stage is written to ops.load_audit."""
    contract = contract or load_contract()
    run_id = run_id or audit.new_run_id()
    cat = get_catalog(cfg)
    with audit.stage(cat, run_id, "landing", month) as rec:
        existed = landing.landing_path(cfg, month).exists()
        path = landing.land(cfg, month)
        rec["detail"] = ("already landed, " if existed else "downloaded, ") + f"{path.stat().st_size} bytes"
    with audit.stage(cat, run_id, "bronze", month) as rec:
        b = bronze.load_bronze(cat, contract, month, path)
        rec.update(
            rows_out=b.rows,
            snapshot_id=b.snapshot_id,
            detail=f"new source columns: {', '.join(b.new_columns)}" if b.new_columns else None,
        )
    with audit.stage(cat, run_id, "silver", month) as rec:
        s = silver.load_silver(cat, contract, month)
        rec.update(
            rows_in=s.bronze_rows,
            rows_out=s.silver_rows,
            rows_quarantined=s.quarantined_rows,
            snapshot_id=s.snapshot_id,
            detail=f"contract v{contract.version}",
        )
    return b, s


def run(cfg: Config, months: list[str], contract: Contract | None = None) -> RunSummary:
    contract = contract or load_contract()
    summary = RunSummary(run_id=audit.new_run_id())
    for month in months:
        b, s = load_month(cfg, month, contract, summary.run_id)
        summary.bronze.append(b)
        summary.silver.append(s)
    cat = get_catalog(cfg)
    with audit.stage(cat, summary.run_id, "gold") as rec:
        summary.gold = gold.build_gold(cat)
        rec.update(
            rows_out=summary.gold.rows.get("fact_flight"),
            detail=", ".join(f"{k}={v}" for k, v in summary.gold.rows.items()),
        )
    return summary
