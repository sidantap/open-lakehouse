"""Idempotent loads, schema drift and evolution, time travel and the SCD Type 2 airport dimension."""

from __future__ import annotations

import json
import shutil

import duckdb
import pytest

from conftest import HEADER, flight, flights_on_days, write_month
from lakehouse import bronze, gold, silver
from lakehouse.catalog import get_catalog, register
from lakehouse.pipeline import load_month, month_range, run


def query(cfg, tables: list[str], sql: str, at: dict | None = None) -> list[tuple]:
    with duckdb.connect() as con:
        register(con, get_catalog(cfg), tables, at)
        return con.execute(sql).fetchall()


def test_month_range():
    assert month_range("2024-11:2025-02") == ["2024-11", "2024-12", "2025-01", "2025-02"]
    assert month_range("2025-03") == ["2025-03"]
    with pytest.raises(ValueError):
        month_range("2025-03:2025-01")


def test_reloading_a_month_replaces_it(cfg, contract):
    write_month(cfg, "2025-01", flights_on_days("2025-01", 5))
    write_month(cfg, "2025-02", flights_on_days("2025-02", 3))
    load_month(cfg, "2025-01", contract)
    load_month(cfg, "2025-02", contract)
    load_month(cfg, "2025-01", contract)  # again: no duplicates, February untouched

    counts = query(
        cfg,
        [bronze.TABLE, silver.TABLE],
        """
        SELECT b._source_month, b.n, s.n FROM
          (SELECT _source_month, count(*) n FROM bronze_flights GROUP BY 1) b
          JOIN (SELECT _source_month, count(*) n FROM silver_flights GROUP BY 1) s USING (_source_month)
        ORDER BY 1""",
    )
    assert counts == [("2025-01", 5, 5), ("2025-02", 3, 3)]
    assert len(get_catalog(cfg).load_table(bronze.TABLE).snapshots()) >= 3


def test_time_travel_reads_an_earlier_snapshot(cfg, contract):
    write_month(cfg, "2025-01", flights_on_days("2025-01", 5))
    load_month(cfg, "2025-01", contract)
    first = get_catalog(cfg).load_table(silver.TABLE).current_snapshot().snapshot_id

    write_month(cfg, "2025-01", flights_on_days("2025-01", 8))  # BTS re-publishes a corrected file
    load_month(cfg, "2025-01", contract)
    sql = "SELECT count(*) FROM silver_flights"
    assert query(cfg, [silver.TABLE], sql) == [(8,)]
    assert query(cfg, [silver.TABLE], sql, at={silver.TABLE: first}) == [(5,)]


def test_missing_contract_column_is_schema_drift(cfg, contract):
    header = [h for h in HEADER if h != "Distance"]
    write_month(cfg, "2025-01", [flight()], header=header)
    with pytest.raises(bronze.SchemaDriftError, match="Distance"):
        load_month(cfg, "2025-01", contract)


def test_new_source_column_evolves_bronze_schema(cfg, contract):
    write_month(cfg, "2025-01", [flight()])
    load_month(cfg, "2025-01", contract)
    write_month(cfg, "2025-02", [flight(month="2025-02", Co2Kg="1234")], header=[*HEADER, "Co2Kg"])
    b, s = load_month(cfg, "2025-02", contract)

    assert b.new_columns == ["Co2Kg"]
    assert s.silver_rows == 1  # silver is defined by the contract, so it doesn't change
    rows = query(cfg, [bronze.TABLE], 'SELECT _source_month, "Co2Kg" FROM bronze_flights ORDER BY 1')
    assert rows == [("2025-01", None), ("2025-02", "1234")]


def city_flights(month: str, city: str) -> list[dict]:
    return [flight(month=month, OriginCityName=city)]


@pytest.mark.parametrize(
    "order",
    [["2025-01", "2025-02", "2025-03"], ["2025-03", "2025-01", "2025-02"]],
    ids=["in-order", "backfilled"],
)
def test_scd2_airport_versions(cfg, contract, order):
    cities = {
        "2025-01": "Dallas/Fort Worth, TX",
        "2025-02": "Dallas-Fort Worth, TX",
        "2025-03": "Dallas/Fort Worth, TX",
    }
    for m in cities:
        write_month(cfg, m, city_flights(m, cities[m]))
    run(cfg, order, contract)

    versions = query(
        cfg,
        ["gold.dim_airport"],
        """
        SELECT version, city_name, valid_from::VARCHAR, valid_to::VARCHAR, is_current
        FROM gold_dim_airport WHERE airport_code = 'DFW' ORDER BY version""",
    )
    # A -> B -> A is three versions, not two: the dimension records each change in order
    assert versions == [
        (1, "Dallas/Fort Worth, TX", "2025-01-15", "2025-02-14", False),
        (2, "Dallas-Fort Worth, TX", "2025-02-15", "2025-03-14", False),
        (3, "Dallas/Fort Worth, TX", "2025-03-15", "9999-12-31", True),
    ]
    # each flight joins to the airport as it was named on its own date
    joined = query(
        cfg,
        ["gold.fact_flight", "gold.dim_airport"],
        """
        SELECT f._source_month, a.city_name FROM gold_fact_flight f
        JOIN gold_dim_airport a ON a.airport_sk = f.origin_airport_sk ORDER BY 1""",
    )
    assert joined == sorted(cities.items())


def test_gold_reconciles_and_marts_add_up(cfg, contract):
    write_month(
        cfg,
        "2025-01",
        flights_on_days("2025-01", 40)
        + flights_on_days(
            "2025-01",
            10,
            Reporting_Airline="ZZ",
            Cancelled="1.00",
            CancellationCode="B",
            ArrTime="",
            ArrDelay="",
            ArrDelayMinutes="",
            ArrDel15="",
            DepTime="",
            DepDelay="",
            DepDelayMinutes="",
            DepDel15="",
            TaxiOut="",
            TaxiIn="",
            AirTime="",
            ActualElapsedTime="",
        ),
    )
    summary = run(cfg, ["2025-01"], contract)
    assert summary.gold.rows["fact_flight"] == 50

    marts = query(
        cfg,
        ["gold.mart_carrier_month"],
        """
        SELECT carrier_code, carrier_name, scheduled, cancelled, on_time_rate, cancel_rate
        FROM gold_mart_carrier_month ORDER BY 1""",
    )
    assert marts == [
        ("AA", "American Airlines", 40, 0, 1.0, 0.0),
        ("ZZ", "Unknown carrier (ZZ)", 10, 10, None, 1.0),
    ]
    [(routes,)] = query(cfg, ["gold.mart_route_month"], "SELECT sum(scheduled) FROM gold_mart_route_month")
    assert routes == 50


def test_gold_fails_loudly_if_the_fact_loses_rows(cfg, contract, monkeypatch, tmp_path):
    write_month(cfg, "2025-01", [flight()])
    load_month(cfg, "2025-01", contract)
    broken = tmp_path / "sql"
    shutil.copytree(gold.SQL_DIR, broken)
    fact = broken / "gold" / "fact_flight.sql"
    fact.write_text(fact.read_text() + "WHERE f.flight_number <> 100\n")  # a join or filter that drops rows
    monkeypatch.setattr(gold, "SQL_DIR", broken)
    with pytest.raises(gold.ReconciliationError, match="0 rows but silver.flights has 1"):
        gold.build_gold(get_catalog(cfg))


def test_report_embeds_the_gold_numbers(cfg, contract, tmp_path):
    from lakehouse.report import build_report

    write_month(
        cfg,
        "2025-01",
        flights_on_days("2025-01", 30) + [flight(Origin="??", Flight_Number_Reporting_Airline="9")],
    )
    run(cfg, ["2025-01"], contract)
    html = build_report(get_catalog(cfg), tmp_path / "site").read_text()
    data = json.loads(html.split("const D = ", 1)[1].split(";\n", 1)[0])
    assert data["totals"]["flights"] == 30
    assert data["quality"] == [{"month": "2025-01", "bronze": 31, "quarantined": 1}]
    assert data["examples"][0]["violations"] == ["pattern:origin"]
