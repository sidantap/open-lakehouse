"""The Iceberg catalog: a SQLite file that maps table names to their current metadata.

A real deployment would swap this for a REST, Glue or Nessie catalog; nothing else changes,
because every layer talks to tables only through this module.
"""

from __future__ import annotations

from collections.abc import Sequence

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table
from pyiceberg.transforms import Transform

from .config import Config

NAMESPACES = ("bronze", "silver", "gold")
Partition = tuple[str, Transform, str]  # (source column, transform, partition field name)


def get_catalog(cfg: Config) -> Catalog:
    cat = SqlCatalog(
        "lakehouse",
        uri=f"sqlite:///{cfg.catalog_db}",
        warehouse=cfg.warehouse.as_uri(),
    )
    for ns in NAMESPACES:
        cat.create_namespace_if_not_exists(ns)
    return cat


def ensure_table(
    cat: Catalog, identifier: str, schema: pa.Schema, partition: Sequence[Partition] = ()
) -> Table:
    """Create the table on first use; afterwards evolve its schema to accept any new columns.

    Schema evolution is additive only (union by name): a column that disappears from a source
    stays in the table and is simply null for new rows, so old snapshots and readers keep working.
    """
    if not cat.table_exists(identifier):
        table = cat.create_table(identifier, schema=schema)
        if partition:
            with table.update_spec() as spec:
                for source, transform, name in partition:
                    spec.add_field(source, transform, name)
        return cat.load_table(identifier)
    table = cat.load_table(identifier)
    current = {f.name for f in table.schema().fields}
    if any(name not in current for name in schema.names):
        with table.update_schema() as update:
            update.union_by_name(schema)
        table = cat.load_table(identifier)
    return table


def conform(df: pa.Table, table: Table) -> pa.Table:
    """Order and cast an Arrow table to the Iceberg table's schema (missing columns become null)."""
    target = table.schema().as_arrow()
    cols = []
    for field in target:
        if field.name in df.column_names:
            cols.append(df[field.name].cast(field.type))
        else:
            cols.append(pa.nulls(df.num_rows, field.type))
    return pa.Table.from_arrays(cols, schema=target)


def table_name(identifier: str) -> str:
    """bronze.flights -> bronze_flights, the name a table gets as a DuckDB view."""
    return identifier.replace(".", "_")


def register(
    con: duckdb.DuckDBPyConnection,
    cat: Catalog,
    identifiers: Sequence[str],
    snapshot_ids: dict[str, int] | None = None,
) -> None:
    """Expose Iceberg tables to DuckDB as views, optionally pinned to a snapshot (time travel)."""
    snapshot_ids = snapshot_ids or {}
    for ident in identifiers:
        table = cat.load_table(ident)
        scan = table.scan(snapshot_id=snapshot_ids.get(ident))
        con.register(table_name(ident), scan.to_arrow())


def all_tables(cat: Catalog) -> list[str]:
    return sorted(".".join(t) for ns in NAMESPACES for t in cat.list_tables(ns))
