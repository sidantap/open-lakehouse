"""Gold: a star schema and marts, rebuilt from silver with DuckDB SQL.

Bronze and silver load month by month; gold is a full rebuild. With DuckDB that takes seconds for
millions of rows, and it makes gold always consistent with silver: backfilling an old month or
fixing a contract can never leave a stale aggregate or a dangling dimension key behind.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import duckdb
import pyarrow.csv as pacsv
from pyiceberg.catalog import Catalog
from pyiceberg.transforms import MonthTransform

from . import silver
from .catalog import conform, ensure_table, register, table_name
from .config import SEEDS_DIR, SQL_DIR

log = logging.getLogger(__name__)

# (table, partitioning) in build order: each step can read the ones before it.
MODELS: list[tuple[str, list]] = [
    ("gold.dim_airport", []),
    ("gold.dim_carrier", []),
    ("gold.dim_date", []),
    ("gold.fact_flight", [("flight_date", MonthTransform(), "flight_month")]),
    ("gold.mart_carrier_month", []),
    ("gold.mart_route_month", []),
    ("gold.mart_airport_hour", []),
]


@dataclass(frozen=True)
class GoldResult:
    rows: dict[str, int]


class ReconciliationError(RuntimeError):
    pass


def build_gold(cat: Catalog, only: list[str] | None = None) -> GoldResult:
    rows: dict[str, int] = {}
    with duckdb.connect() as con:
        register(con, cat, [silver.TABLE])
        con.register("carriers_seed", pacsv.read_csv(SEEDS_DIR / "carriers.csv"))
        silver_rows = con.execute("SELECT count(*) FROM silver_flights").fetchone()[0]

        for ident, partition in MODELS:
            name = ident.split(".")[1]
            if only and name not in only:
                if cat.table_exists(ident):  # still readable by the models after it
                    register(con, cat, [ident])
                continue
            data = con.execute((SQL_DIR / "gold" / f"{name}.sql").read_text()).to_arrow_table()
            table = ensure_table(cat, ident, data.schema, partition)
            table.overwrite(
                conform(data, table), snapshot_properties={"layer": "gold", "rows": str(data.num_rows)}
            )
            con.register(table_name(ident), data)
            rows[name] = data.num_rows
            log.info("gold %s: %s rows", name, data.num_rows)

        fact_rows = rows.get("fact_flight")
        if fact_rows is not None and fact_rows != silver_rows:
            raise ReconciliationError(
                f"fact_flight has {fact_rows} rows but silver.flights has {silver_rows}"
            )
    return GoldResult(rows)
