"""Tests for evidence-gathering tools, using fakes instead of live infrastructure.

Adapted from the addon template to match the actual weather_quarantine schema:
- partition_date (not ingestion_date)
- validation_report JSON column (not validation_failure_reason)
- No raw_payload column; get_schema_diff checks column null-rates instead.
- get_ge_validation_result reads from the quarantine DB, not GE file system.
"""
from __future__ import annotations

import json

import pytest

from agent.config import AgentConfig
from agent.tools import get_ge_validation_result, get_schema_diff, get_airflow_task_logs


def make_cfg(tmp_path, **overrides) -> AgentConfig:
    base = dict(
        groq_api_key="test-key",
        groq_model="llama-3.3-70b-versatile",
        max_tool_iterations=3,
        postgres_dsn="postgresql://test",
        airflow_logs_dir=str(tmp_path / "logs"),
        reports_dir=str(tmp_path / "reports"),
        slack_webhook_url=None,
        schema_baseline_path=str(tmp_path / "baseline.json"),
    )
    base.update(overrides)
    return AgentConfig(**base)


# ---------------------------------------------------------------------------
# get_ge_validation_result — now reads from quarantine table, not GE files
# ---------------------------------------------------------------------------

class _FakeCursorWithReport:
    """Returns one row containing a validation_report JSON string."""

    def __init__(self, report_json: str):
        self._report_json = report_json

    def execute(self, *_args, **_kwargs):
        return None

    def fetchone(self):
        return (self._report_json,)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeCursorEmpty:
    def execute(self, *_args, **_kwargs):
        return None

    def fetchone(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _fake_conn(cursor):
    class FakeConn:
        def cursor(self, cursor_factory=None):
            return cursor

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    return FakeConn()


def test_get_ge_validation_result_returns_failures(tmp_path, monkeypatch):
    report = {
        "partition_date": "2026-06-16",
        "success": False,
        "outcomes": [
            {
                "expectation_type": "expect_column_values_to_be_between",
                "column": "temperature_c",
                "success": False,
                "details": "24/24 rows out of range",
                "unexpected_count": 24,
                "element_count": 24,
            },
            {
                "expectation_type": "expect_table_row_count_to_equal",
                "column": None,
                "success": True,
                "details": "Expected 24 rows, got 24",
                "unexpected_count": 0,
                "element_count": 0,
            },
        ],
    }
    cfg = make_cfg(tmp_path)
    monkeypatch.setattr(
        "agent.tools.psycopg2.connect",
        lambda dsn: _fake_conn(_FakeCursorWithReport(json.dumps(report))),
    )

    outcome = get_ge_validation_result(cfg, "2026-06-16")

    assert outcome["found"] is True
    assert len(outcome["failed_expectations"]) == 1
    assert outcome["failed_expectations"][0]["expectation_type"] == (
        "expect_column_values_to_be_between"
    )
    assert outcome["failed_expectations"][0]["column"] == "temperature_c"


def test_get_ge_validation_result_no_quarantine_rows(tmp_path, monkeypatch):
    cfg = make_cfg(tmp_path)
    monkeypatch.setattr(
        "agent.tools.psycopg2.connect",
        lambda dsn: _fake_conn(_FakeCursorEmpty()),
    )

    outcome = get_ge_validation_result(cfg, "2026-06-16")

    assert outcome["found"] is False
    assert outcome["failed_expectations"] == []


# ---------------------------------------------------------------------------
# get_schema_diff — checks column null-rates, not raw_payload keys
# ---------------------------------------------------------------------------

class _FakeCursorNullCheck:
    """Simulates a row where temperature_c and wind_speed_kmh are all-null."""

    def execute(self, *_args, **_kwargs):
        return None

    def fetchone(self):
        # total=24, temperature_c_nn=0 (all null), others populated
        return {
            "total": 24,
            "temperature_c_nn": 0,
            "humidity_pct_nn": 24,
            "precipitation_mm_nn": 24,
            "wind_speed_kmh_nn": 0,
            "pressure_hpa_nn": 24,
        }

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_get_schema_diff_detects_all_null_columns(tmp_path, monkeypatch):
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(
        json.dumps({"expected_keys": ["temperature_2m", "wind_speed_10m", "relative_humidity_2m"]})
    )
    cfg = make_cfg(tmp_path, schema_baseline_path=str(baseline_path))
    monkeypatch.setattr(
        "agent.tools.psycopg2.connect",
        lambda dsn: _fake_conn(_FakeCursorNullCheck()),
    )

    outcome = get_schema_diff(cfg, "2026-06-16")

    assert outcome["comparable"] is True
    assert "temperature_c" in outcome["columns_all_null"]
    assert "wind_speed_kmh" in outcome["columns_all_null"]
    assert "humidity_pct" not in outcome["columns_all_null"]
    assert outcome["total_rows"] == 24


def test_get_schema_diff_no_rows(tmp_path, monkeypatch):
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps({"expected_keys": []}))
    cfg = make_cfg(tmp_path, schema_baseline_path=str(baseline_path))
    monkeypatch.setattr(
        "agent.tools.psycopg2.connect",
        lambda dsn: _fake_conn(_FakeCursorEmpty()),
    )

    outcome = get_schema_diff(cfg, "2026-06-16")

    assert outcome["comparable"] is False


# ---------------------------------------------------------------------------
# get_airflow_task_logs — file-based, tests log path resolution
# ---------------------------------------------------------------------------

def test_get_airflow_task_logs_finds_correct_path(tmp_path):
    logs_dir = tmp_path / "logs"
    log_file = (
        logs_dir
        / "dag_id=self_healing_daily_pipeline"
        / "run_id=scheduled__2026-06-16T06:00:00+00:00"
        / "task_id=validate"
    )
    log_file.mkdir(parents=True)
    (log_file / "attempt=1.log").write_text("line1\nline2\nERROR: something bad\n")

    cfg = make_cfg(tmp_path, airflow_logs_dir=str(logs_dir))
    result = get_airflow_task_logs(
        cfg, "self_healing_daily_pipeline", "validate", "2026-06-16"
    )

    assert result["found"] is True
    assert "ERROR: something bad" in result["log_tail"]


def test_get_airflow_task_logs_missing(tmp_path):
    cfg = make_cfg(tmp_path, airflow_logs_dir=str(tmp_path / "logs"))
    result = get_airflow_task_logs(
        cfg, "self_healing_daily_pipeline", "validate", "2026-06-16"
    )

    assert result["found"] is False
    assert result["log_tail"] == ""
