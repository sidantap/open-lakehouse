"""Where things live. Everything sits under one folder so a laptop run is easy to inspect and delete."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PACKAGE_DIR = Path(__file__).parent
SQL_DIR = PACKAGE_DIR / "sql"
CONTRACTS_DIR = PACKAGE_DIR / "contracts"
SEEDS_DIR = PACKAGE_DIR / "seeds"

# BTS "Reporting Carrier On-Time Performance (1987-present)", one zip per month.
BTS_URL = (
    "https://www.transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)


@dataclass(frozen=True)
class Config:
    home: Path

    @property
    def landing(self) -> Path:
        return self.home / "landing"

    @property
    def warehouse(self) -> Path:
        return self.home / "warehouse"

    @property
    def catalog_db(self) -> Path:
        return self.home / "catalog.db"

    def ensure(self) -> Config:
        for p in (self.landing, self.warehouse):
            p.mkdir(parents=True, exist_ok=True)
        return self

    @classmethod
    def from_env(cls) -> Config:
        return cls(Path(os.environ.get("LAKEHOUSE_HOME", "lakehouse_data")).resolve()).ensure()
