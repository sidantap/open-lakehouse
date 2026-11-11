"""Load a data contract and compile it to DuckDB SQL.

From one YAML file this produces:
  * the typed SELECT that turns raw strings into the silver schema, and
  * one named check per constraint, collected into a `_violations` list on every row.

Rows with an empty list are valid. Everything else is quarantined with the list as the reason.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .config import CONTRACTS_DIR

SQL_TYPES = {"date": "DATE", "int": "INTEGER", "float": "DOUBLE", "bool": "BOOLEAN", "string": "VARCHAR"}


def q(name: str) -> str:
    """Quote an identifier for DuckDB."""
    return '"' + name.replace('"', '""') + '"'


def lit(value: object) -> str:
    """A SQL literal for a contract value (numbers stay numbers, everything else is a string)."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class Column:
    name: str
    source: str
    type: str
    required: bool = False
    min: float | None = None
    max: float | None = None
    pattern: str | None = None
    allowed: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if self.type not in SQL_TYPES:
            raise ValueError(f"column {self.name}: unknown type {self.type!r}")

    def cast(self, raw: str) -> str:
        """SQL that turns the raw string column into this column's type, or NULL if it can't."""
        blank = f"NULLIF(TRIM({raw}), '')"
        if self.type == "string":
            return blank
        if self.type == "bool":  # BTS writes flags as 0.00 / 1.00
            num = f"TRY_CAST({blank} AS DOUBLE)"
            return f"CASE WHEN {num} = 1 THEN TRUE WHEN {num} = 0 THEN FALSE END"
        if self.type == "int":  # integers arrive as "15.00"
            num = f"TRY_CAST({blank} AS DOUBLE)"
            return f"CASE WHEN {num} = ROUND({num}) THEN CAST({num} AS INTEGER) END"
        return f"TRY_CAST({blank} AS {SQL_TYPES[self.type]})"

    def checks(self) -> list[tuple[str, str]]:
        """(check name, SQL that is TRUE when the row FAILS the check)."""
        col, raw = q(self.name), q(f"__raw_{self.name}")
        out = [(f"type:{self.name}", f"NULLIF(TRIM({raw}), '') IS NOT NULL AND {col} IS NULL")]
        if self.required:
            out.append((f"required:{self.name}", f"NULLIF(TRIM({raw}), '') IS NULL"))
        if self.min is not None:
            out.append((f"min:{self.name}", f"{col} < {lit(self.min)}"))
        if self.max is not None:
            out.append((f"max:{self.name}", f"{col} > {lit(self.max)}"))
        if self.pattern is not None:
            out.append((f"pattern:{self.name}", f"NOT regexp_full_match({col}, {lit(self.pattern)})"))
        if self.allowed is not None:
            values = ", ".join(lit(v) for v in self.allowed)
            out.append((f"allowed:{self.name}", f"{col} NOT IN ({values})"))
        return out


@dataclass(frozen=True)
class Rule:
    name: str
    sql: str
    description: str = ""


@dataclass(frozen=True)
class Contract:
    name: str
    version: str
    grain: str
    primary_key: tuple[str, ...]
    columns: tuple[Column, ...]
    rules: tuple[Rule, ...] = ()
    max_quarantine_rate: float = 1.0
    owner: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def source_columns(self) -> list[str]:
        return [c.source for c in self.columns]

    def typed_select(self, relation: str, passthrough: list[str]) -> str:
        """SELECT the typed columns (plus the raw value of each, for the type checks)."""
        parts = []
        for c in self.columns:
            raw = q(c.source)
            parts.append(f"{c.cast(raw)} AS {q(c.name)}")
            parts.append(f"{raw} AS {q('__raw_' + c.name)}")
        parts += [q(p) for p in passthrough]
        return "SELECT\n  " + ",\n  ".join(parts) + f"\nFROM {relation}"

    def all_checks(self) -> list[tuple[str, str]]:
        checks = [chk for c in self.columns for chk in c.checks()]
        # A rule fails when it isn't TRUE; NULL (e.g. a missing input) counts as a failure.
        checks += [(f"rule:{r.name}", f"NOT COALESCE(({r.sql}), FALSE)") for r in self.rules]
        return checks

    def validated_select(self, relation: str, passthrough: list[str], order_by: str) -> str:
        """The full validation query: typed columns, `_violations` and duplicate-key detection."""
        flags = ",\n    ".join(f"CASE WHEN {cond} THEN {lit(name)} END" for name, cond in self.all_checks())
        pk = ", ".join(q(k) for k in self.primary_key)
        return f"""
WITH typed AS (
{self.typed_select(relation, passthrough)}
), checked AS (
  SELECT *, list_filter([
    {flags}
  ], lambda x: x IS NOT NULL) AS _violations
  FROM typed
), ranked AS (
  -- among rows that are otherwise valid, the first occurrence of a key wins
  SELECT *, CASE WHEN len(_violations) = 0
              THEN row_number() OVER (PARTITION BY len(_violations) = 0, {pk} ORDER BY {order_by})
            END AS _key_rank
  FROM checked
)
SELECT * EXCLUDE (_violations, _key_rank),
  CASE WHEN _key_rank > 1 THEN list_append(_violations, 'duplicate_key') ELSE _violations END
    AS _violations
FROM ranked
"""


def load_contract(path: Path | str | None = None) -> Contract:
    path = Path(path) if path else CONTRACTS_DIR / "silver_flights.yml"
    spec = yaml.safe_load(Path(path).read_text())
    columns = tuple(
        Column(
            name=name,
            source=c["source"],
            type=c["type"],
            required=c.get("required", False),
            min=c.get("min"),
            max=c.get("max"),
            pattern=c.get("pattern"),
            allowed=tuple(str(v) for v in c["allowed"]) if "allowed" in c else None,
        )
        for name, c in spec["columns"].items()
    )
    names = {c.name for c in columns}
    missing = [k for k in spec["primary_key"] if k not in names]
    if missing:
        raise ValueError(f"primary key columns not in contract: {missing}")
    return Contract(
        name=spec["name"],
        version=str(spec["version"]),
        grain=spec.get("grain", ""),
        owner=spec.get("owner", ""),
        primary_key=tuple(spec["primary_key"]),
        columns=columns,
        rules=tuple(Rule(r["name"], r["sql"], r.get("description", "")) for r in spec.get("rules", [])),
        max_quarantine_rate=float(spec.get("expectations", {}).get("max_quarantine_rate", 1.0)),
    )
