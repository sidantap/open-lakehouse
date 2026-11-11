"""The data contract: every kind of bad row is caught, named and quarantined, never dropped."""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime

import duckdb
import pytest

from conftest import HEADER, cancelled, flight, flights_on_days, late, write_month
from lakehouse import silver
from lakehouse.catalog import get_catalog, register
from lakehouse.contract import load_contract
from lakehouse.pipeline import load_month


def quarantined(cfg) -> list[tuple[int, list[str], dict]]:
    with duckdb.connect() as con:
        register(con, get_catalog(cfg), [silver.QUARANTINE])
        rows = con.execute(
            "SELECT _row_number, violations, raw_record FROM silver_flights_quarantine ORDER BY _row_number"
        ).fetchall()
    return [(n, list(v), json.loads(raw)) for n, v, raw in rows]


def silver_rows(cfg, cols: str = "*") -> list[tuple]:
    with duckdb.connect() as con:
        register(con, get_catalog(cfg), [silver.TABLE])
        return con.execute(f"SELECT {cols} FROM silver_flights ORDER BY flight_number").fetchall()


def test_contract_is_well_formed():
    c = load_contract()
    names = [name for name, _ in c.all_checks()]
    assert len(names) == len(set(names)), "check names must be unique"
    assert set(c.source_columns) <= set(HEADER), "every contract column exists in the real BTS header"
    assert c.max_quarantine_rate == 0.005


def test_valid_flights_pass(cfg, contract):
    write_month(
        cfg,
        "2025-01",
        [
            flight(),
            flight(Flight_Number_Reporting_Airline="101", **late(45, CarrierDelay=30, LateAircraftDelay=15)),
            flight(Flight_Number_Reporting_Airline="102", **cancelled("A")),
            flight(
                Flight_Number_Reporting_Airline="103",
                Diverted="1.00",
                ArrTime="",
                ArrDelay="",
                ArrDelayMinutes="",
                ArrDel15="",
            ),
        ],
    )
    _, s = load_month(cfg, "2025-01", contract)
    assert (s.bronze_rows, s.silver_rows, s.quarantined_rows) == (4, 4, 0)
    rows = silver_rows(cfg, "flight_number, is_on_time, cancelled, cancellation_reason, arr_delay_late_min")
    assert rows == [
        (100, True, False, None, 2),
        (101, False, False, None, 45),
        (102, False, True, "Carrier", None),
        (103, False, False, None, None),
    ]


BAD_ROWS = [
    ({"FlightDate": "2025-13-40"}, "type:flight_date"),
    ({"FlightDate": "2025-02-03"}, "rule:flight_in_source_month"),
    ({"Origin": "DF"}, "pattern:origin"),
    ({"Dest": "DFW", "DestAirportID": "11298"}, "rule:origin_differs_from_dest"),
    ({"Flight_Number_Reporting_Airline": ""}, "required:flight_number"),
    ({"Flight_Number_Reporting_Airline": "12.5"}, "type:flight_number"),
    ({"ArrDelay": "abc"}, "type:arr_delay_min"),
    ({"Distance": "-5.00"}, "min:distance_mi"),
    ({"CRSDepTime": "2460"}, "pattern:crs_dep_hhmm"),
    ({"Reporting_Airline": "american"}, "pattern:carrier_code"),
    ({**cancelled(), "CancellationCode": ""}, "rule:cancelled_has_reason"),
    ({**cancelled(), "CancellationCode": "Z"}, "allowed:cancellation_code"),
    ({**cancelled(), "ArrTime": "1035"}, "rule:cancelled_did_not_arrive"),
    ({"CancellationCode": "B"}, "rule:cancelled_has_reason"),
    ({"ArrDelay": "", "ArrDelayMinutes": ""}, "rule:completed_has_arrival_delay"),
    ({**late(45, CarrierDelay=30, WeatherDelay=5)}, "rule:delay_causes_add_up"),
]


@pytest.mark.parametrize(("overrides", "expected"), BAD_ROWS, ids=[e for _, e in BAD_ROWS])
def test_bad_rows_are_quarantined_with_a_reason(cfg, contract, overrides, expected):
    write_month(cfg, "2025-01", [flight(), flight(**{"Flight_Number_Reporting_Airline": "200", **overrides})])
    _, s = load_month(cfg, "2025-01", contract)
    assert (s.silver_rows, s.quarantined_rows) == (1, 1)
    [(row_number, violations, raw)] = quarantined(cfg)
    assert row_number == 2
    assert expected in violations
    # the raw values are kept exactly as they arrived, for debugging and replay
    for col, value in overrides.items():
        if col in raw:
            assert raw[col] == (value or None)


def test_duplicate_keys_keep_the_first_row(cfg, contract):
    write_month(
        cfg, "2025-01", [flight(), flight(TaxiOut="20.00"), flight(Flight_Number_Reporting_Airline="7")]
    )
    _, s = load_month(cfg, "2025-01", contract)
    assert (s.silver_rows, s.quarantined_rows) == (2, 1)
    assert quarantined(cfg)[0][:2] == (2, ["duplicate_key"])
    assert silver_rows(cfg, "flight_number, taxi_out_min") == [(7, 15), (100, 15)]


def test_midnight_is_written_2400(cfg, contract):
    write_month(cfg, "2025-01", [flight(day=31, CRSDepTime="2400", DepTime="2400")])
    load_month(cfg, "2025-01", contract)
    [(dep, hour)] = silver_rows(cfg, "crs_dep_local, crs_dep_hour")
    assert dep == datetime(2025, 2, 1, 0, 0)
    assert hour == 0


def test_a_bad_month_breaches_the_contract_and_writes_nothing(cfg):
    strict = load_contract()  # 0.5% budget
    write_month(
        cfg,
        "2025-01",
        flights_on_days("2025-01", 50) + [flight(Origin="??", Flight_Number_Reporting_Airline="9")],
    )
    with pytest.raises(silver.ContractBreach, match=r"pattern:origin"):
        load_month(cfg, "2025-01", strict)
    assert not get_catalog(cfg).table_exists(silver.TABLE)


def test_budget_allows_rare_bad_rows(cfg):
    lenient = dataclasses.replace(load_contract(), max_quarantine_rate=0.05)
    write_month(
        cfg,
        "2025-01",
        flights_on_days("2025-01", 50) + [flight(Origin="??", Flight_Number_Reporting_Airline="9")],
    )
    _, s = load_month(cfg, "2025-01", lenient)
    assert (s.silver_rows, s.quarantined_rows) == (50, 1)
