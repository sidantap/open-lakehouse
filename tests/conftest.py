"""Test helpers: tiny BTS-format files built from the real header, with made-up flights."""

from __future__ import annotations

import csv
import dataclasses
import io
import zipfile
from pathlib import Path

import pytest

from lakehouse.config import Config
from lakehouse.contract import load_contract
from lakehouse.landing import landing_path

HEADER = next(csv.reader((Path(__file__).parent / "fixtures" / "bts_header.csv").open()))
HEADER = [h for h in HEADER if h]  # the real header ends with a comma


def flight(day: int = 15, month: str = "2025-01", **overrides: str) -> dict[str, str]:
    """A valid, on-time flight; override any BTS column by name."""
    row = {
        "FlightDate": f"{month}-{day:02d}",
        "Reporting_Airline": "AA",
        "DOT_ID_Reporting_Airline": "19805",
        "IATA_CODE_Reporting_Airline": "AA",
        "Tail_Number": "N101AA",
        "Flight_Number_Reporting_Airline": "100",
        "OriginAirportID": "11298",
        "Origin": "DFW",
        "OriginCityName": "Dallas/Fort Worth, TX",
        "OriginState": "TX",
        "OriginStateName": "Texas",
        "DestAirportID": "13930",
        "Dest": "ORD",
        "DestCityName": "Chicago, IL",
        "DestState": "IL",
        "DestStateName": "Illinois",
        "CRSDepTime": "0800",
        "DepTime": "0805",
        "DepDelay": "5.00",
        "DepDelayMinutes": "5.00",
        "DepDel15": "0.00",
        "TaxiOut": "15.00",
        "TaxiIn": "8.00",
        "CRSArrTime": "1030",
        "ArrTime": "1032",
        "ArrDelay": "2.00",
        "ArrDelayMinutes": "2.00",
        "ArrDel15": "0.00",
        "Cancelled": "0.00",
        "CancellationCode": "",
        "Diverted": "0.00",
        "CRSElapsedTime": "150.00",
        "ActualElapsedTime": "147.00",
        "AirTime": "124.00",
        "Distance": "802.00",
    }
    row.update(overrides)
    return row


def late(minutes: int, **causes: int) -> dict[str, str]:
    """Overrides for a flight that arrived `minutes` late, with delay minutes by cause."""
    keys = ["CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]
    return {
        "ArrDelay": f"{minutes}.00",
        "ArrDelayMinutes": f"{minutes}.00",
        "ArrDel15": "1.00",
        **{k: f"{causes.get(k, 0)}.00" for k in keys},
    }


def cancelled(code: str = "B") -> dict[str, str]:
    return {
        "Cancelled": "1.00",
        "CancellationCode": code,
        "DepTime": "",
        "ArrTime": "",
        "DepDelay": "",
        "DepDelayMinutes": "",
        "DepDel15": "",
        "ArrDelay": "",
        "ArrDelayMinutes": "",
        "ArrDel15": "",
        "TaxiOut": "",
        "TaxiIn": "",
        "ActualElapsedTime": "",
        "AirTime": "",
    }


def write_month(cfg: Config, month: str, rows: list[dict[str, str]], header: list[str] | None = None) -> Path:
    """Write rows as the month's landed zip, in the BTS layout (all quoted, trailing comma)."""
    header = header or HEADER
    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    w.writerow([*header, ""])
    for r in rows:
        w.writerow([r.get(h, "") for h in header] + [""])
    path = landing_path(cfg, month)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"On_Time_Reporting_{month}.csv", buf.getvalue())
    return path


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    return Config(tmp_path / "lh").ensure()


@pytest.fixture
def contract():
    """The real contract, with the quarantine budget lifted so small test files can hold bad rows."""
    return dataclasses.replace(load_contract(), max_quarantine_rate=1.0)


def flights_on_days(month: str, n: int, **overrides: str) -> list[dict[str, str]]:
    return [
        flight(day=1 + i % 28, month=month, Flight_Number_Reporting_Airline=str(100 + i), **overrides)
        for i in range(n)
    ]
