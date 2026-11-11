"""The Dagster assets run the same pipeline and report its checks."""

from __future__ import annotations

from datetime import datetime

import dagster as dg

from conftest import flights_on_days, write_month
from lakehouse import definitions as d


def test_definitions_load():
    graph = d.defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert {"landing/ontime_zip", "bronze/flights", "silver/flights", "gold/fact_flight"} <= keys


def test_schedule_targets_the_month_bts_has_published():
    ctx = dg.build_schedule_context(scheduled_execution_time=datetime(2026, 2, 20, 6))
    req = d.monthly_schedule(ctx)
    assert req.partition_key == "2025-11-01"


def test_materialize_one_month_then_gold(cfg):
    write_month(cfg, "2025-01", flights_on_days("2025-01", 20))  # landed already: no download
    resources = {"lakehouse": d.Lakehouse(home=str(cfg.home))}

    monthly = dg.materialize(
        [d.ontime_zip, d.bronze_flights, d.silver_flights], partition_key="2025-01-01", resources=resources
    )
    assert monthly.success
    checks = {e.check_name: e.passed for e in monthly.get_asset_check_evaluations()}
    assert checks == {"reconciles_with_bronze": True, "quarantine_within_budget": True}

    result = dg.materialize([d.gold_models], resources=resources)
    assert result.success
    rows = {
        e.materialization.asset_key.path[-1]: e.materialization.metadata["rows"].value
        for e in result.get_asset_materialization_events()
    }
    assert rows["fact_flight"] == 20
