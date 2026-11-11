"""The flight operations report: a static, multi-page HTML report built from the gold layer.

  lakehouse report --out site   ->   site/index.html

The report ships the additive gold cubes (rpt_*) as compact JSON and does all filtering and
re-totaling in the browser, the way a BI tool works off an extract. The Data Operations page is
built from Iceberg metadata (snapshots, data files) and the landing manifests.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from pyiceberg.catalog import Catalog

from . import audit, bronze, silver
from .catalog import all_tables, register
from .contract import load_contract

TOP_ROUTES = 200


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _cube(con: duckdb.DuckDBPyConnection, sql: str) -> dict:
    """A table as {columns, rows} with rows as arrays: about a third of the size of a list of dicts."""
    cur = con.execute(sql)
    return {"columns": [d[0] for d in cur.description], "rows": [list(r) for r in cur.fetchall()]}


def _snapshot_log(cat: Catalog) -> tuple[list[dict], list[dict]]:
    log, inventory = [], []
    for ident in all_tables(cat):
        t = cat.load_table(ident)
        snaps = t.snapshots()
        for s in snaps:
            p = s.summary.additional_properties
            log.append(
                {
                    "table": ident,
                    "snapshot_id": str(s.snapshot_id),
                    "committed_at": datetime.fromtimestamp(s.timestamp_ms / 1000, UTC).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "operation": s.summary.operation.value,
                    "source_month": p.get("source_month"),
                    "added": int(p.get("added-records", 0)),
                    "deleted": int(p.get("deleted-records", 0)),
                    "total": int(p.get("total-records", 0)),
                    "batch_id": p.get("batch_id"),
                    "contract_version": p.get("contract_version"),
                }
            )
        files = t.inspect.files().select(["file_size_in_bytes"]).to_pylist()
        current = t.current_snapshot()
        inventory.append(
            {
                "table": ident,
                "layer": ident.split(".")[0],
                "rows": int(current.summary.additional_properties.get("total-records", 0)) if current else 0,
                "snapshots": len(snaps),
                "data_files": len(files),
                "bytes": sum(f["file_size_in_bytes"] for f in files),
                "partitioning": ", ".join(
                    f"{f.transform}({t.schema().find_field(f.source_id).name})" for f in t.spec().fields
                )
                or "none",
                "columns": len(t.schema().fields),
                "updated_at": datetime.fromtimestamp(current.timestamp_ms / 1000, UTC).strftime(
                    "%Y-%m-%d %H:%M"
                )
                if current
                else None,
            }
        )
    log.sort(key=lambda r: (r["committed_at"], r["table"]), reverse=True)
    return log, inventory


def _audit(cat: Catalog) -> list[dict]:
    if not cat.table_exists(audit.TABLE):
        return []
    rows = cat.load_table(audit.TABLE).scan().to_arrow().to_pylist()
    for r in rows:
        for k in ("started_at", "finished_at"):
            r[k] = r[k].strftime("%Y-%m-%d %H:%M:%S") if r[k] else None
    return rows


def _manifests(landing: Path | None) -> dict[str, dict]:
    if not landing or not landing.exists():
        return {}
    return {
        m.stem.removeprefix("ontime_"): json.loads(m.read_text())
        for m in sorted(landing.glob("ontime_*.json"))
    }


def collect(cat: Catalog, landing: Path | None = None) -> dict:
    contract = load_contract()
    with duckdb.connect() as con:
        register(
            con,
            cat,
            [
                "gold.rpt_flight_summary",
                "gold.rpt_departure_hour",
                "gold.rpt_route_summary",
                "gold.dim_carrier",
                "gold.dim_airport",
                "gold.fact_flight",
                silver.QUARANTINE,
            ],
        )
        con.register(
            "bronze_months", cat.load_table(bronze.TABLE).scan(selected_fields=("_source_month",)).to_arrow()
        )

        summary = _cube(con, "SELECT * FROM gold_rpt_flight_summary")
        hours = _cube(con, "SELECT * FROM gold_rpt_departure_hour")
        routes = _cube(
            con,
            f"""
            WITH top AS (SELECT origin, dest FROM gold_rpt_route_summary GROUP BY ALL
                         ORDER BY sum(scheduled) DESC LIMIT {TOP_ROUTES})
            SELECT r.* FROM gold_rpt_route_summary r JOIN top USING (origin, dest)""",
        )
        carriers = _rows(
            con, "SELECT carrier_code, carrier_name, carrier_group FROM gold_dim_carrier ORDER BY 1"
        )
        airports = _rows(
            con,
            """
            SELECT airport_code, any_value(airport_name) AS airport_name, any_value(city_name) AS city,
                   any_value(state_code) AS state, any_value(state_name) AS state_name,
                   any_value(latitude) AS lat, any_value(longitude) AS lon
            FROM gold_dim_airport WHERE is_current GROUP BY 1 ORDER BY 1""",
        )
        recon = _rows(
            con,
            """
            SELECT b._source_month AS month, b.n AS bronze_rows, coalesce(q.n, 0) AS quarantined_rows,
                   coalesce(f.n, 0) AS fact_rows
            FROM (SELECT _source_month, count(*) n FROM bronze_months GROUP BY 1) b
            LEFT JOIN (SELECT _source_month, count(*) n FROM silver_flights_quarantine GROUP BY 1) q
              USING (_source_month)
            LEFT JOIN (SELECT _source_month, count(*) n FROM gold_fact_flight GROUP BY 1) f
              USING (_source_month)
            ORDER BY 1""",
        )
        quarantine = _rows(
            con,
            """
            SELECT _source_month AS month, _row_number AS row_number, _batch_id AS batch_id,
                   violations, raw_record, contract_version
            FROM silver_flights_quarantine ORDER BY 1, 2 LIMIT 200""",
        )

    failures = Counter(v for q in quarantine for v in q["violations"])
    checks = [
        {"check": name, "kind": name.split(":")[0], "failures": failures.get(name, 0)}
        for name, _ in contract.all_checks()
    ]
    checks.append({"check": "duplicate_key", "kind": "key", "failures": failures.get("duplicate_key", 0)})
    for q in quarantine:
        q["violations"] = list(q["violations"])
        q["raw_record"] = json.loads(q["raw_record"])

    snapshots, inventory = _snapshot_log(cat)
    manifests = _manifests(landing)

    audit_rows = _audit(cat)
    # the latest successful run of each stage for each month
    latest = {}
    for r in sorted(audit_rows, key=lambda r: r["started_at"]):
        if r["status"] == "succeeded" and r["source_month"]:
            latest[(r["source_month"], r["stage"])] = r

    def ms(month: str, stage: str) -> int | None:
        return (latest.get((month, stage)) or {}).get("duration_ms")

    first = {}  # the data snapshot each bronze load committed, for its batch id
    for snap in reversed(snapshots):
        if snap["table"] == bronze.TABLE and snap["added"]:
            first[snap["source_month"]] = snap
    loads = []
    for r in recon:
        m, s_ = manifests.get(r["month"], {}), latest.get((r["month"], "silver")) or {}
        loads.append(
            {
                "month": r["month"],
                "file_bytes": m.get("bytes"),
                "sha256": (m.get("sha256") or "")[:12] or None,
                "download_seconds": m.get("download_seconds"),
                "bronze_rows": r["bronze_rows"],
                "bronze_ms": ms(r["month"], "bronze"),
                "batch_id": (first.get(r["month"]) or {}).get("batch_id"),
                "silver_rows": s_.get("rows_out"),
                "silver_ms": ms(r["month"], "silver"),
                "quarantined_rows": r["quarantined_rows"],
                "fact_rows": r["fact_rows"],
                "loaded_at": str(s_.get("finished_at") or "")[:19] or None,
            }
        )

    return {
        "meta": {
            "built_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
            "contract": {
                "name": contract.name,
                "version": contract.version,
                "checks": len(checks),
                "budget": contract.max_quarantine_rate,
            },
            "top_routes": TOP_ROUTES,
        },
        "carriers": carriers,
        "airports": airports,
        "summary": summary,
        "hours": hours,
        "routes": routes,
        "ops": {
            "runs": sorted(audit_rows, key=lambda r: r["started_at"], reverse=True)[:60],
            "loads": loads,
            "snapshots": snapshots[:80],
            "inventory": inventory,
            "checks": checks,
            "quarantine": quarantine,
        },
    }


def build_report(cat: Catalog, out: str | Path, landing: Path | None = None) -> Path:
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    data = json.dumps(collect(cat, landing), default=str, separators=(",", ":"))
    html = (Path(__file__).parent / "report.html").read_text().replace("/*DATA*/null", data)
    path = out / "index.html"
    path.write_text(html)
    return path
