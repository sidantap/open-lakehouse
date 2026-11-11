"""A one-page static report built from the gold marts (no server, no external libraries).

  lakehouse report --out site   ->   site/index.html

The page embeds the numbers as JSON and draws the charts as inline SVG in the browser.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from pyiceberg.catalog import Catalog

from . import bronze, silver
from .catalog import register
from .contract import load_contract

TOP_AIRPORTS = 12


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def collect(cat: Catalog) -> dict:
    with duckdb.connect() as con:
        register(
            con,
            cat,
            [
                "gold.mart_carrier_month",
                "gold.mart_route_month",
                "gold.mart_airport_hour",
                silver.QUARANTINE,
            ],
        )
        # bronze row counts per month, without scanning 100+ string columns: read only one column
        bt = cat.load_table(bronze.TABLE).scan(selected_fields=("_source_month",)).to_arrow()
        con.register("bronze_months", bt)

        totals = _rows(
            con,
            """
            SELECT sum(scheduled)::BIGINT AS flights,
                   sum(on_time) / sum(scheduled - cancelled - diverted) AS on_time_rate,
                   sum(cancelled) / sum(scheduled) AS cancel_rate, min(year_month) AS first_month,
                   max(year_month) AS last_month, count(DISTINCT carrier_code) AS carriers
            FROM gold_mart_carrier_month""",
        )[0]
        carriers = _rows(
            con,
            """
            SELECT carrier_code, carrier_name, sum(scheduled)::BIGINT AS flights,
                   round(sum(on_time) / sum(scheduled - cancelled - diverted), 4) AS on_time_rate,
                   round(sum(cancelled) / sum(scheduled), 4) AS cancel_rate
            FROM gold_mart_carrier_month GROUP BY ALL ORDER BY on_time_rate DESC""",
        )
        months = _rows(
            con,
            """
            SELECT year_month, sum(scheduled)::BIGINT AS flights,
                   round(sum(on_time) / sum(scheduled - cancelled - diverted), 4) AS on_time_rate,
                   round(sum(cancelled) / sum(scheduled), 4) AS cancel_rate
            FROM gold_mart_carrier_month GROUP BY 1 ORDER BY 1""",
        )
        causes = _rows(
            con,
            """
            UNPIVOT (SELECT sum(late_aircraft_delay_min)::BIGINT AS "Late-arriving aircraft",
                            sum(carrier_delay_min)::BIGINT AS "Carrier",
                            sum(nas_delay_min)::BIGINT AS "National Air System",
                            sum(weather_delay_min)::BIGINT AS "Weather",
                            sum(security_delay_min)::BIGINT AS "Security"
                     FROM gold_mart_carrier_month)
            ON COLUMNS(*) INTO NAME cause VALUE minutes ORDER BY minutes DESC""",
        )
        top = [
            r["origin"]
            for r in _rows(
                con,
                f"""
            SELECT origin, sum(scheduled) n FROM gold_mart_airport_hour GROUP BY 1 ORDER BY n DESC
            LIMIT {TOP_AIRPORTS}""",
            )
        ]
        heat = _rows(
            con,
            f"""
            SELECT origin, any_value(origin_city) AS city, hour, scheduled, dep_delayed_rate
            FROM gold_mart_airport_hour
            WHERE origin IN ({", ".join(repr(a) for a in top)}) AND hour BETWEEN 5 AND 23
            GROUP BY ALL ORDER BY origin, hour""",
        )
        routes = _rows(
            con,
            """
            SELECT origin, dest, origin_city, dest_city, sum(scheduled)::BIGINT AS flights,
                   round(sum(on_time_rate * scheduled) / sum(scheduled), 4) AS on_time_rate,
                   round(sum(cancel_rate * scheduled) / sum(scheduled), 4) AS cancel_rate
            FROM gold_mart_route_month GROUP BY ALL ORDER BY flights DESC LIMIT 12""",
        )
        quality = _rows(
            con,
            """
            SELECT b._source_month AS month, count(*) AS bronze, coalesce(any_value(q.n), 0) AS quarantined
            FROM bronze_months b
            LEFT JOIN (SELECT _source_month, count(*) n FROM silver_flights_quarantine GROUP BY 1) q
              USING (_source_month)
            GROUP BY 1 ORDER BY 1""",
        )
        examples = _rows(
            con,
            """
            SELECT _source_month AS month, _row_number AS row_number, violations, raw_record
            FROM silver_flights_quarantine ORDER BY _source_month, _row_number LIMIT 5""",
        )

    contract = load_contract()
    for e in examples:
        e["raw_record"] = json.loads(e["raw_record"])
        e["violations"] = list(e["violations"])
    return {
        "totals": totals,
        "carriers": carriers,
        "months": months,
        "causes": causes,
        "airports": top,
        "heat": heat,
        "routes": routes,
        "quality": quality,
        "examples": examples,
        "contract": {
            "name": contract.name,
            "version": contract.version,
            "checks": len(contract.all_checks()),
            "budget": contract.max_quarantine_rate,
        },
        "built": datetime.now(UTC).strftime("%Y-%m-%d"),
    }


def build_report(cat: Catalog, out: str | Path) -> Path:
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    data = json.dumps(collect(cat), default=str, separators=(",", ":"))
    html = (Path(__file__).parent / "report.html").read_text().replace("/*DATA*/null", data)
    path = out / "index.html"
    path.write_text(html)
    return path
