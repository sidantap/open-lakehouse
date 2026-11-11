"""lakehouse: run the pipeline and inspect the tables from a terminal.

lakehouse run --months 2025-01:2025-03     land, load and validate months, then rebuild gold
lakehouse tables                            every table with its row count and snapshot count
lakehouse history silver.flights            the table's snapshots: what each load changed
lakehouse sql "SELECT ..."                  query tables with DuckDB (bronze_flights, gold_fact_flight, ...)
lakehouse sql "..." --at silver.flights=ID  ...as the table was at an earlier snapshot (time travel)
lakehouse quarantine --month 2025-01        why rows were rejected
lakehouse maintain --keep 5                 expire old snapshots, keeping the latest few per table
lakehouse report --out site                 build the static report from the gold marts
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import UTC, datetime

import duckdb

from . import pipeline
from .catalog import all_tables, get_catalog, register, table_name
from .config import Config


def _print(rel: duckdb.DuckDBPyRelation, limit: int = 40) -> None:
    rel.limit(limit).show(max_width=200)


def cmd_run(cfg: Config, args: argparse.Namespace) -> int:
    summary = pipeline.run(cfg, pipeline.month_range(args.months))
    for s in summary.silver:
        print(
            f"{s.month}: {s.bronze_rows:>9,} bronze  {s.silver_rows:>9,} silver  "
            f"{s.quarantined_rows:>6,} quarantined ({s.quarantine_rate:.3%})"
        )
    if summary.gold:
        print("gold: " + ", ".join(f"{k} {v:,}" for k, v in summary.gold.rows.items()))
    return 0


def cmd_tables(cfg: Config, args: argparse.Namespace) -> int:
    cat = get_catalog(cfg)
    for ident in all_tables(cat):
        t = cat.load_table(ident)
        snap = t.current_snapshot()
        rows = snap.summary.additional_properties.get("total-records", "?") if snap else "0"
        spec = ", ".join(f"{f.transform}({t.schema().find_field(f.source_id).name})" for f in t.spec().fields)
        print(
            f"{ident:<28} {int(rows):>11,} rows  {len(t.snapshots()):>3} snapshots  {spec or 'unpartitioned'}"
        )
    return 0


def cmd_history(cfg: Config, args: argparse.Namespace) -> int:
    t = get_catalog(cfg).load_table(args.table)
    for s in t.snapshots():
        p = s.summary.additional_properties
        when = datetime.fromtimestamp(s.timestamp_ms / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"{s.snapshot_id:>20}  {when}  {s.summary.operation.value:<9} "
            f"month={p.get('source_month', '-'):<8} +{p.get('added-records', '0'):>8} "
            f"-{p.get('deleted-records', '0'):>8}  total={p.get('total-records', '?')}"
        )
    return 0


def cmd_sql(cfg: Config, args: argparse.Namespace) -> int:
    cat = get_catalog(cfg)
    pins = {}
    for pin in args.at or []:
        ident, _, sid = pin.partition("=")
        pins[ident] = int(sid)
    # only scan the tables the query mentions
    needed = [t for t in all_tables(cat) if re.search(rf"\b{table_name(t)}\b", args.query)]
    with duckdb.connect() as con:
        register(con, cat, needed, pins)
        _print(con.sql(args.query), args.limit)
    return 0


def cmd_quarantine(cfg: Config, args: argparse.Namespace) -> int:
    cat = get_catalog(cfg)
    with duckdb.connect() as con:
        register(con, cat, ["silver.flights_quarantine"])
        where = f"WHERE _source_month = '{args.month}'" if args.month else ""
        _print(
            con.sql(f"""SELECT _source_month, v AS failed_check, count(*) AS rows
                           FROM silver_flights_quarantine, unnest(violations) t(v) {where}
                           GROUP BY ALL ORDER BY 1, 3 DESC""")
        )
        _print(
            con.sql(f"""SELECT _source_month, _row_number, violations, raw_record
                           FROM silver_flights_quarantine {where} ORDER BY 1, 2"""),
            10,
        )
    return 0


def cmd_maintain(cfg: Config, args: argparse.Namespace) -> int:
    cat = get_catalog(cfg)
    for ident in all_tables(cat):
        t = cat.load_table(ident)
        snaps = sorted(t.snapshots(), key=lambda s: s.timestamp_ms)
        current = t.current_snapshot().snapshot_id if t.current_snapshot() else None
        old = [s.snapshot_id for s in snaps[: max(0, len(snaps) - args.keep)] if s.snapshot_id != current]
        if old:
            t.maintenance.expire_snapshots().by_ids(old).commit()
        print(f"{ident:<28} expired {len(old):>3}, kept {len(snaps) - len(old)}")
    return 0


def cmd_report(cfg: Config, args: argparse.Namespace) -> int:
    from .report import build_report

    path = build_report(get_catalog(cfg), args.out)
    print(f"report written to {path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lakehouse", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run", help="land, load and validate months, then rebuild gold")
    p.add_argument("--months", required=True, help="2025-01 or 2025-01:2025-06")
    sub.add_parser("tables", help="list tables")
    p = sub.add_parser("history", help="snapshots of a table")
    p.add_argument("table")
    p = sub.add_parser("sql", help="query tables with DuckDB")
    p.add_argument("query")
    p.add_argument("--at", action="append", metavar="TABLE=SNAPSHOT_ID", help="time travel a table")
    p.add_argument("--limit", type=int, default=40)
    p = sub.add_parser("quarantine", help="rejected rows and why")
    p.add_argument("--month")
    p = sub.add_parser("maintain", help="expire old snapshots")
    p.add_argument("--keep", type=int, default=5)
    p = sub.add_parser("report", help="build the static report")
    p.add_argument("--out", default="site")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose or args.cmd == "run" else logging.WARNING,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("pyiceberg").setLevel(logging.WARNING)
    cfg = Config.from_env()
    handler = {
        "run": cmd_run,
        "tables": cmd_tables,
        "history": cmd_history,
        "sql": cmd_sql,
        "quarantine": cmd_quarantine,
        "maintain": cmd_maintain,
        "report": cmd_report,
    }[args.cmd]
    return handler(cfg, args)


if __name__ == "__main__":
    sys.exit(main())
