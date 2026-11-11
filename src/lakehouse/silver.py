"""Silver: typed, validated, de-duplicated flights, with every rejected row kept in quarantine.

For each month:  bronze rows = silver rows + quarantined rows.  That reconciliation is checked
on every load, so a row can't disappear without a recorded reason.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo
from pyiceberg.transforms import IdentityTransform, MonthTransform

from . import bronze
from .catalog import conform, ensure_table
from .config import SQL_DIR
from .contract import Contract, lit, q

log = logging.getLogger(__name__)

TABLE = "silver.flights"
QUARANTINE = "silver.flights_quarantine"


class ContractBreach(RuntimeError):
    """Too much of a month failed the contract to trust the load."""


@dataclass(frozen=True)
class SilverResult:
    month: str
    bronze_rows: int
    silver_rows: int
    quarantined_rows: int
    violations: dict[str, int]
    snapshot_id: int

    @property
    def quarantine_rate(self) -> float:
        return self.quarantined_rows / self.bronze_rows if self.bronze_rows else 0.0


def transform_month(
    con: duckdb.DuckDBPyConnection, contract: Contract, raw: pa.Table
) -> tuple[pa.Table, pa.Table]:
    """Validate one month of bronze rows. Returns (silver rows, quarantined rows)."""
    con.register("bronze_month", raw)
    con.execute(
        "CREATE OR REPLACE TEMP TABLE checked AS "
        + contract.validated_select("bronze_month", bronze.META, order_by="_row_number")
    )
    keep = [q(c.name) for c in contract.columns] + [q(m) for m in bronze.META]
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW validated AS "
        f"SELECT {', '.join(keep)} FROM checked WHERE len(_violations) = 0"
    )
    silver = con.execute((SQL_DIR / "silver" / "flights.sql").read_text()).to_arrow_table()

    raw_record = ", ".join(f"{lit(c.source)}, {q('__raw_' + c.name)}" for c in contract.columns)
    quarantine = con.execute(f"""
        SELECT _source_month, _row_number, _batch_id, _violations AS violations,
               json_object({raw_record})::VARCHAR AS raw_record,
               {lit(contract.version)} AS contract_version,
               _ingested_at
        FROM checked WHERE len(_violations) > 0
        ORDER BY _row_number
    """).to_arrow_table()
    con.unregister("bronze_month")
    return silver, quarantine


def load_silver(cat: Catalog, contract: Contract, month: str) -> SilverResult:
    raw = cat.load_table(bronze.TABLE).scan(row_filter=EqualTo("_source_month", month)).to_arrow()
    if raw.num_rows == 0:
        raise RuntimeError(f"no bronze rows for {month}; load bronze first")

    with duckdb.connect() as con:
        silver, quarantine = transform_month(con, contract, raw)
        counts = dict(
            con.execute(
                "SELECT v, count(*) FROM checked, unnest(_violations) t(v) GROUP BY v ORDER BY 2 DESC"
            ).fetchall()
        )

    result_rows = (raw.num_rows, silver.num_rows, quarantine.num_rows)
    if result_rows[0] != result_rows[1] + result_rows[2]:  # can't happen unless the SQL is wrong
        raise AssertionError(f"reconciliation failed for {month}: {result_rows}")
    rate = quarantine.num_rows / raw.num_rows
    if rate > contract.max_quarantine_rate:
        top = ", ".join(f"{k} ({v})" for k, v in list(counts.items())[:5])
        raise ContractBreach(
            f"{month}: {rate:.2%} of rows broke contract {contract.name} v{contract.version} "
            f"(limit {contract.max_quarantine_rate:.2%}). Top checks: {top}. Nothing was written."
        )

    props = {"source_month": month, "contract_version": contract.version, "layer": "silver"}
    table = ensure_table(cat, TABLE, silver.schema, [("flight_date", MonthTransform(), "flight_month")])
    table.overwrite(
        conform(silver, table),
        overwrite_filter=EqualTo("_source_month", month),
        snapshot_properties={**props, "rows": str(silver.num_rows)},
    )
    qtable = ensure_table(
        cat, QUARANTINE, quarantine.schema, [("_source_month", IdentityTransform(), "source_month")]
    )
    qtable.overwrite(
        conform(quarantine, qtable),
        overwrite_filter=EqualTo("_source_month", month),
        snapshot_properties={**props, "rows": str(quarantine.num_rows)},
    )

    snap = table.refresh().current_snapshot()
    log.info(
        "silver %s: %s valid, %s quarantined (%.3f%%)",
        month,
        silver.num_rows,
        quarantine.num_rows,
        rate * 100,
    )
    return SilverResult(month, raw.num_rows, silver.num_rows, quarantine.num_rows, counts, snap.snapshot_id)
