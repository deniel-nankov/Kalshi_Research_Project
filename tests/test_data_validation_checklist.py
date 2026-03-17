"""
Tests that run the data validation checklist (validate_data_health.run_checks).

Ensures: no FAIL results, and critical checks (schema, uniqueness, value constraints,
temporal, boundary overlap) pass when data is present.

Uses a single shared run of run_checks (fixture) so the full validation runs once per session.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

# Load the health validator script (run_checks and helpers live there)
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT_PATH = _PROJECT_ROOT / "scripts" / "validate_data_health.py"


def _load_validator():
    spec = importlib.util.spec_from_file_location("validate_data_health", _SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {_SCRIPT_PATH}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["validate_data_health"] = mod  # required so dataclass can resolve cls.__module__
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def validation_results():
    """Run the full health checklist once; all tests in this module use this result."""
    mod = _load_validator()
    con = duckdb.connect()
    try:
        return mod.run_checks(con)
    finally:
        con.close()


@pytest.fixture(scope="module")
def validation_by_name(validation_results):
    return {r.name: r for r in validation_results}


def _has_data(validation_results) -> bool:
    """Return True if validator sees trade and market data (no DATA_PRESENCE fail)."""
    by_name = {r.name: r for r in validation_results}
    if "DATA_PRESENCE" in by_name and by_name["DATA_PRESENCE"].status == "FAIL":
        return False
    return True


@pytest.mark.slow
def test_data_health_no_failures(validation_results):
    """Run all validation checks; no check must report FAIL."""
    fails = [r for r in validation_results if r.status == "FAIL"]
    if not validation_results:
        pytest.skip("No checks produced (no data?)")
    assert not fails, f"Validation FAILs: {[(r.name, r.message) for r in fails]}"


@pytest.mark.slow
def test_data_health_critical_checks_pass_when_data_present(validation_results, validation_by_name):
    """When data exists, schema, uniqueness, value constraints, temporal, and boundary overlap must pass."""
    if not _has_data(validation_results):
        pytest.skip("No trade/market data in workspace")

    by_name = validation_by_name
    critical = [
        "SCHEMA_TRADES",
        "SCHEMA_MARKETS",
        "UNIQUENESS_TRADES",
        "VALUE_CONSTRAINTS_TRADES",
        "TEMPORAL_TRADES",
        "BOUNDARY_OVERLAP",
    ]
    missing = [c for c in critical if c not in by_name]
    assert not missing, f"Expected checks not run: {missing}"

    failed_or_warn = []
    for c in critical:
        r = by_name[c]
        if r.status == "FAIL":
            failed_or_warn.append(f"{c}: FAIL - {r.message}")
        elif r.status == "WARN" and c == "BOUNDARY_OVERLAP":
            if "overlap" in r.message.lower():
                failed_or_warn.append(f"{c}: {r.status} - {r.message}")

    assert not failed_or_warn, "Critical checks failed or unexpected warn: " + "; ".join(failed_or_warn)


@pytest.mark.slow
def test_trade_ids_unique(validation_results, validation_by_name):
    """Every trade_id appears at most once in combined trades."""
    if not _has_data(validation_results):
        pytest.skip("No trade data in workspace")
    assert "UNIQUENESS_TRADES" in validation_by_name
    assert validation_by_name["UNIQUENESS_TRADES"].status == "PASS", validation_by_name["UNIQUENESS_TRADES"].message


@pytest.mark.slow
def test_trade_prices_in_valid_range(validation_results, validation_by_name):
    """Trade yes_price and no_price are in [0, 100] and yes+no=100 where applicable."""
    if not _has_data(validation_results):
        pytest.skip("No trade data in workspace")
    assert "VALUE_CONSTRAINTS_TRADES" in validation_by_name
    assert validation_by_name["VALUE_CONSTRAINTS_TRADES"].status != "FAIL", validation_by_name["VALUE_CONSTRAINTS_TRADES"].message


@pytest.mark.slow
def test_trade_created_time_parseable_no_future(validation_results, validation_by_name):
    """All trade created_time values parse as timestamps and are not in the future."""
    if not _has_data(validation_results):
        pytest.skip("No trade data in workspace")
    assert "TEMPORAL_TRADES" in validation_by_name
    assert validation_by_name["TEMPORAL_TRADES"].status != "FAIL", validation_by_name["TEMPORAL_TRADES"].message


@pytest.mark.slow
def test_no_historical_forward_trade_id_overlap(validation_results, validation_by_name):
    """No trade_id appears in both historical and forward data."""
    if not _has_data(validation_results):
        pytest.skip("No trade data in workspace")
    assert "BOUNDARY_OVERLAP" in validation_by_name
    assert validation_by_name["BOUNDARY_OVERLAP"].status == "PASS", validation_by_name["BOUNDARY_OVERLAP"].message


@pytest.mark.slow
def test_boundary_gap_no_large_skip(validation_results, validation_by_name):
    """First forward trade is close in time to last historical (no multi-day skip)."""
    if not _has_data(validation_results):
        pytest.skip("No trade data in workspace")
    if "BOUNDARY_GAP" not in validation_by_name:
        pytest.skip("BOUNDARY_GAP not run (single source?)")
    assert validation_by_name["BOUNDARY_GAP"].status != "FAIL", validation_by_name["BOUNDARY_GAP"].message


@pytest.mark.slow
def test_trade_timeline_density(validation_results, validation_by_name):
    """No unexpectedly large gaps between consecutive trading days."""
    if not _has_data(validation_results):
        pytest.skip("No trade data in workspace")
    assert "TRADE_TIMELINE_DENSITY" in validation_by_name
    assert validation_by_name["TRADE_TIMELINE_DENSITY"].status != "FAIL", validation_by_name["TRADE_TIMELINE_DENSITY"].message
