"""Bronze: the month's file as it arrived, every column kept, every value still a string.

Bronze never rejects a row: typing and validation happen in silver, where failures can be
quarantined with a reason. Bronze does reject a *file* whose header is missing columns the
silver contract needs (schema drift), because loading it would only fail later and quieter.

Each load replaces exactly one `_source_month` partition, so re-running a month is safe.
"""

from __future__ import annotations

import csv
import io
import logging
import uuid
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo
from pyiceberg.transforms import IdentityTransform

from .catalog import conform, ensure_table
from .contract import Contract

log = logging.getLogger(__name__)

TABLE = "bronze.flights"
META = ["_source_month", "_source_file", "_row_number", "_batch_id", "_ingested_at"]


class SchemaDriftError(RuntimeError):
    """The source file no longer carries a column the contract depends on."""


@dataclass(frozen=True)
class BronzeResult:
    month: str
    rows: int
    batch_id: str
    snapshot_id: int
    new_columns: list[str]


def read_month_zip(path: Path) -> tuple[pa.Table, str]:
    """Read the CSV inside a BTS zip with every column as a string."""
    with zipfile.ZipFile(path) as zf:
        member = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        with zf.open(member) as f:
            header = next(csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig")))
        names = [h.strip() for h in header]
        keep = [n for n in names if n]  # BTS ends every line with a comma: one unnamed column
        with zf.open(member) as f:
            table = pacsv.read_csv(
                f,
                read_options=pacsv.ReadOptions(
                    column_names=[n or f"__blank{i}" for i, n in enumerate(names)],
                    skip_rows=1,
                    block_size=64 << 20,
                ),
                convert_options=pacsv.ConvertOptions(
                    column_types=dict.fromkeys(keep, pa.string()),
                    include_columns=keep,
                    strings_can_be_null=True,
                    null_values=[""],
                ),
            )
    return table, member


def check_drift(columns: list[str], contract: Contract) -> None:
    missing = [c for c in contract.source_columns if c not in columns]
    if missing:
        raise SchemaDriftError(f"source file is missing contract columns: {', '.join(missing)}")


def load_bronze(cat: Catalog, contract: Contract, month: str, zip_path: Path) -> BronzeResult:
    raw, member = read_month_zip(zip_path)
    check_drift(raw.column_names, contract)

    batch_id = uuid.uuid4().hex[:12]
    n = raw.num_rows
    raw = (
        raw.append_column("_source_month", pa.array([month] * n, pa.string()))
        .append_column("_source_file", pa.array([member] * n, pa.string()))
        .append_column("_row_number", pa.array(range(1, n + 1), pa.int64()))
        .append_column("_batch_id", pa.array([batch_id] * n, pa.string()))
        .append_column(
            "_ingested_at", pa.array([datetime.now(UTC).replace(tzinfo=None)] * n, pa.timestamp("us"))
        )
    )

    before = set()
    if cat.table_exists(TABLE):
        before = {f.name for f in cat.load_table(TABLE).schema().fields}
    table = ensure_table(cat, TABLE, raw.schema, [("_source_month", IdentityTransform(), "source_month")])
    new_columns = sorted(set(raw.column_names) - before) if before else []
    if new_columns:
        log.warning("bronze: source added columns, table schema evolved: %s", new_columns)

    table.overwrite(
        conform(raw, table),
        overwrite_filter=EqualTo("_source_month", month),
        snapshot_properties={"source_month": month, "batch_id": batch_id, "rows": str(n), "layer": "bronze"},
    )
    snap = table.refresh().current_snapshot()
    log.info("bronze %s: %s rows, batch %s, snapshot %s", month, n, batch_id, snap.snapshot_id)
    return BronzeResult(month, n, batch_id, snap.snapshot_id, new_columns)
