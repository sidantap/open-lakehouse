"""ops.load_audit: one row per pipeline stage, per month, per run.

Iceberg snapshots record *what* changed in a table; the audit table records *how the run went*:
when each stage started and finished, the row counts it saw, and the error if it failed. A failed
stage is written too (status = 'failed', with the message) before the exception is re-raised, so
an operator can see what broke without digging through logs.

    with audit.stage(cat, run_id, "silver", month) as rec:
        result = silver.load_silver(...)
        rec.update(rows_in=..., rows_out=..., rows_quarantined=...)
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

import pyarrow as pa
from pyiceberg.catalog import Catalog

from .catalog import conform, ensure_table

TABLE = "ops.load_audit"
SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("stage", pa.string()),
        ("source_month", pa.string()),
        ("status", pa.string()),
        ("started_at", pa.timestamp("us")),
        ("finished_at", pa.timestamp("us")),
        ("duration_ms", pa.int64()),
        ("rows_in", pa.int64()),
        ("rows_out", pa.int64()),
        ("rows_quarantined", pa.int64()),
        ("snapshot_id", pa.string()),
        ("detail", pa.string()),
    ]
)


def new_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


@contextmanager
def stage(cat: Catalog, run_id: str, name: str, month: str | None = None) -> Iterator[dict]:
    rec: dict = {"run_id": run_id, "stage": name, "source_month": month, "started_at": _now()}
    started = time.perf_counter()
    try:
        yield rec
        rec["status"] = "succeeded"
    except Exception as e:
        rec["status"] = "failed"
        rec["detail"] = f"{type(e).__name__}: {e}"[:2000]
        raise
    finally:
        rec["finished_at"] = _now()
        rec["duration_ms"] = round((time.perf_counter() - started) * 1000)
        write(cat, rec)


def write(cat: Catalog, rec: dict) -> None:
    row = {f.name: rec.get(f.name) for f in SCHEMA}
    if row["snapshot_id"] is not None:
        row["snapshot_id"] = str(row["snapshot_id"])
    table = ensure_table(cat, TABLE, SCHEMA)
    table.append(
        conform(pa.Table.from_pylist([row], schema=SCHEMA), table),
        snapshot_properties={"layer": "ops", "run_id": rec["run_id"]},
    )
