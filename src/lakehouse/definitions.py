"""Dagster: the same pipeline as software-defined assets, one partition per month.

  landing/ontime_zip -> bronze/flights -> silver/flights (+ quarantine) -> gold star schema and marts

Each month is a partition, so a backfill is "select the months, click Materialize", and Dagster
records which months are loaded, when, and with what result. Data-quality results show up as
asset checks next to the data they describe.

  dagster dev            # http://localhost:3000
"""

import dagster as dg

from . import bronze, gold, landing, silver
from .catalog import get_catalog
from .config import Config
from .contract import load_contract

MONTHS = dg.MonthlyPartitionsDefinition(start_date="2024-01-01")


class Lakehouse(dg.ConfigurableResource):
    """Where the lakehouse lives on disk. Defaults to $LAKEHOUSE_HOME or ./lakehouse_data."""

    home: str = ""

    def config(self) -> Config:
        from pathlib import Path

        return Config(Path(self.home).resolve()).ensure() if self.home else Config.from_env()


@dg.asset(
    key=["landing", "ontime_zip"],
    partitions_def=MONTHS,
    group_name="landing",
    kinds={"python"},
    description="The month's zip from BTS, downloaded once and kept unchanged.",
)
def ontime_zip(context: dg.AssetExecutionContext, lakehouse: Lakehouse) -> dg.MaterializeResult:
    path = landing.land(lakehouse.config(), context.partition_key[:7])
    return dg.MaterializeResult(metadata={"path": str(path), "bytes": path.stat().st_size})


@dg.asset(
    key=["bronze", "flights"],
    deps=[ontime_zip],
    partitions_def=MONTHS,
    group_name="bronze",
    kinds={"iceberg"},
    description="Every source column as a string, one partition per month.",
)
def bronze_flights(context: dg.AssetExecutionContext, lakehouse: Lakehouse) -> dg.MaterializeResult:
    month = context.partition_key[:7]
    cfg = lakehouse.config()
    result = bronze.load_bronze(get_catalog(cfg), load_contract(), month, landing.landing_path(cfg, month))
    return dg.MaterializeResult(
        metadata={
            "rows": result.rows,
            "batch_id": result.batch_id,
            "snapshot_id": str(result.snapshot_id),
            "new_source_columns": ", ".join(result.new_columns) or "none",
        }
    )


@dg.asset(
    key=["silver", "flights"],
    deps=[bronze_flights],
    partitions_def=MONTHS,
    group_name="silver",
    kinds={"iceberg", "duckdb"},
    description="Typed, validated, de-duplicated flights. Rejected rows go to silver.flights_quarantine.",
    check_specs=[
        dg.AssetCheckSpec(
            "reconciles_with_bronze",
            asset=["silver", "flights"],
            description="bronze rows = silver rows + quarantined rows",
        ),
        dg.AssetCheckSpec(
            "quarantine_within_budget",
            asset=["silver", "flights"],
            description="the share of rejected rows is under the contract's limit",
        ),
    ],
)
def silver_flights(context: dg.AssetExecutionContext, lakehouse: Lakehouse):
    contract = load_contract()
    r = silver.load_silver(get_catalog(lakehouse.config()), contract, context.partition_key[:7])
    yield dg.MaterializeResult(
        metadata={
            "bronze_rows": r.bronze_rows,
            "silver_rows": r.silver_rows,
            "quarantined_rows": r.quarantined_rows,
            "quarantine_rate": round(r.quarantine_rate, 6),
            "contract_version": contract.version,
            "failed_checks": dg.MetadataValue.json(r.violations),
        }
    )
    yield dg.AssetCheckResult(
        check_name="reconciles_with_bronze", passed=r.bronze_rows == r.silver_rows + r.quarantined_rows
    )
    yield dg.AssetCheckResult(
        check_name="quarantine_within_budget",
        passed=r.quarantine_rate <= contract.max_quarantine_rate,
        metadata={"rate": r.quarantine_rate, "limit": contract.max_quarantine_rate},
    )


GOLD_SPECS = [
    dg.AssetSpec(
        ["gold", ident.split(".")[1]],
        deps=[silver_flights],
        group_name="gold",
        kinds={"iceberg", "duckdb"},
        automation_condition=dg.AutomationCondition.eager(),
    )
    for ident, _ in gold.MODELS
]


@dg.multi_asset(
    specs=GOLD_SPECS,
    can_subset=True,
    check_specs=[dg.AssetCheckSpec("reconciles_with_silver", asset=["gold", "fact_flight"])],
)
def gold_models(context: dg.AssetExecutionContext, lakehouse: Lakehouse):
    selected = [key.path[-1] for key in context.selected_asset_keys]
    result = gold.build_gold(get_catalog(lakehouse.config()), only=selected)
    for name, rows in result.rows.items():
        yield dg.MaterializeResult(asset_key=["gold", name], metadata={"rows": rows})
    if "fact_flight" in result.rows:  # build_gold raises if the counts differ
        yield dg.AssetCheckResult(
            asset_key=["gold", "fact_flight"], check_name="reconciles_with_silver", passed=True
        )


monthly_load = dg.define_asset_job(
    "monthly_load", selection=dg.AssetSelection.groups("landing", "bronze", "silver"), partitions_def=MONTHS
)


@dg.schedule(job=monthly_load, cron_schedule="0 6 20 * *", execution_timezone="UTC")
def monthly_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """BTS publishes a month about three months after it ends, so on the 20th load month M-3."""
    t = context.scheduled_execution_time
    y, m = (t.year, t.month - 3) if t.month > 3 else (t.year - 1, t.month + 9)
    key = f"{y:04d}-{m:02d}-01"
    return dg.RunRequest(run_key=key, partition_key=key)


defs = dg.Definitions(
    assets=[ontime_zip, bronze_flights, silver_flights, gold_models],
    jobs=[monthly_load],
    schedules=[monthly_schedule],
    resources={"lakehouse": Lakehouse()},
)
