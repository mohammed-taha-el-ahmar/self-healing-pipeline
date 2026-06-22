"""
Data quality gate for the daily weather ingestion pipeline.

Primary validation engine: Great Expectations (PandasDataset API, GE
0.18.x). Expectations are run against the day's batch and produce a single
pass/fail verdict plus a structured report of every expectation result.

Design: quarantine on failure, not fail-fast
----------------------------------------------
This pipeline deliberately does NOT use a multi-tier blocking/warning
split. Any expectation failure routes the entire batch to quarantine:

  - The batch is isolated in `weather_quarantine` (Postgres) along with the
    full GE validation report, for inspection and replay.
  - The DAG run itself does not fail -- `validate` and `quarantine` both
    succeed. Only the *data* is rejected, not the pipeline run.
  - A Slack alert is sent with the specific expectations that failed and
    how many rows were affected.
  - Other partitions/dates are completely unaffected -- good data still
    flows for every other day's run.

This avoids the failure mode of fail-fast pipelines, where one bad day's
data can halt the whole DAG (and queue up every subsequent scheduled run)
until a human intervenes.

Expectations checked
---------------------
- Schema: all expected columns exist.
- Row count == 24 (a full day of hourly data; a partial pull is suspect).
- record_id: not null, unique (this is the warehouse primary key).
- observation_ts: not null, parseable, and within the partition date.
- temperature_c: within a physically plausible range for Paris
  (-30 to 50 C) -- catches sensor/unit errors (e.g. Fahrenheit instead of
  Celsius, or a stuck/garbage reading).
- humidity_pct: between 0 and 100.
- precipitation_mm: >= 0 (negative precipitation is nonsensical).
- wind_speed_kmh: 0-300 (well above any recorded surface gust).
- pressure_hpa: within a physically plausible sea-level range (870-1085).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pandas as pd
from great_expectations.dataset import PandasDataset

REQUIRED_COLUMNS = [
    "record_id",
    "location",
    "observation_ts",
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "wind_speed_kmh",
    "pressure_hpa",
]

EXPECTED_ROWS_PER_DAY = 24


@dataclass
class ExpectationOutcome:
    expectation_type: str
    column: str | None
    success: bool
    details: str
    unexpected_count: int = 0
    element_count: int = 0


@dataclass
class ValidationReport:
    partition_date: str
    outcomes: list[ExpectationOutcome] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(o.success for o in self.outcomes)

    @property
    def failures(self) -> list[ExpectationOutcome]:
        return [o for o in self.outcomes if not o.success]

    def summary(self) -> str:
        lines = [f"Validation for {self.partition_date}: success={self.success}"]
        for o in self.outcomes:
            status = "PASS" if o.success else "FAIL"
            lines.append(f"  [{status}] {o.expectation_type}({o.column}): {o.details}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "partition_date": self.partition_date,
            "success": self.success,
            "outcomes": [
                {
                    "expectation_type": o.expectation_type,
                    "column": o.column,
                    "success": o.success,
                    "details": o.details,
                    "unexpected_count": o.unexpected_count,
                    "element_count": o.element_count,
                }
                for o in self.outcomes
            ],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ValidationReport":
        return cls(
            partition_date=d["partition_date"],
            outcomes=[
                ExpectationOutcome(
                    expectation_type=o["expectation_type"],
                    column=o["column"],
                    success=o["success"],
                    details=o["details"],
                    unexpected_count=o.get("unexpected_count", 0),
                    element_count=o.get("element_count", 0),
                )
                for o in d["outcomes"]
            ],
        )


def _outcome_from_ge_result(result, expectation_type: str, column: str | None, details: str = "") -> ExpectationOutcome:
    res = result.result if hasattr(result, "result") else {}
    return ExpectationOutcome(
        expectation_type=expectation_type,
        column=column,
        success=bool(result.success),
        details=details or json.dumps({k: v for k, v in res.items() if k != "partial_unexpected_list"}),
        unexpected_count=res.get("unexpected_count", 0),
        element_count=res.get("element_count", 0),
    )


def run_quality_gate(df: pd.DataFrame, partition_date: str) -> ValidationReport:
    """Run the Great Expectations suite against `df` for `partition_date`
    (YYYY-MM-DD). Returns a ValidationReport; the DAG branches on
    `report.success`.
    """
    report = ValidationReport(partition_date=partition_date)

    # --- Schema check (not a GE expectation -- guards against KeyErrors
    # in the expectations below if the upstream API shape changed) ---
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        report.outcomes.append(
            ExpectationOutcome(
                expectation_type="expect_table_columns_to_match_set",
                column=None,
                success=False,
                details=f"Missing required columns: {missing}",
            )
        )
        return report

    ds = PandasDataset(df.copy())

    r = ds.expect_table_row_count_to_equal(EXPECTED_ROWS_PER_DAY, result_format="BASIC")
    report.outcomes.append(
        _outcome_from_ge_result(
            r,
            "expect_table_row_count_to_equal",
            None,
            details=f"Expected {EXPECTED_ROWS_PER_DAY} rows, got {len(df)}",
        )
    )

    r = ds.expect_column_values_to_not_be_null("record_id", result_format="BASIC")
    report.outcomes.append(_outcome_from_ge_result(r, "expect_column_values_to_not_be_null", "record_id"))

    r = ds.expect_column_values_to_be_unique("record_id", result_format="BASIC")
    report.outcomes.append(_outcome_from_ge_result(r, "expect_column_values_to_be_unique", "record_id"))

    r = ds.expect_column_values_to_not_be_null("observation_ts", result_format="BASIC")
    report.outcomes.append(_outcome_from_ge_result(r, "expect_column_values_to_not_be_null", "observation_ts"))

    ts = pd.to_datetime(df["observation_ts"], errors="coerce", utc=True)
    bad_dates = int((ts.isna() | (ts.dt.date.astype("string") != partition_date)).sum())
    report.outcomes.append(
        ExpectationOutcome(
            expectation_type="expect_column_values_to_match_partition_date",
            column="observation_ts",
            success=bad_dates == 0,
            details=f"{bad_dates}/{len(df)} rows have observation_ts unparseable or outside {partition_date}",
            unexpected_count=bad_dates,
            element_count=len(df),
        )
    )

    r = ds.expect_column_values_to_be_between(
        "temperature_c", min_value=-30, max_value=35, result_format="BASIC"
    )
    report.outcomes.append(_outcome_from_ge_result(r, "expect_column_values_to_be_between", "temperature_c"))

    r = ds.expect_column_values_to_be_between(
        "humidity_pct", min_value=0, max_value=100, result_format="BASIC"
    )
    report.outcomes.append(_outcome_from_ge_result(r, "expect_column_values_to_be_between", "humidity_pct"))

    r = ds.expect_column_values_to_be_between(
        "precipitation_mm", min_value=0, max_value=None, result_format="BASIC"
    )
    report.outcomes.append(_outcome_from_ge_result(r, "expect_column_values_to_be_between", "precipitation_mm"))

    r = ds.expect_column_values_to_be_between(
        "wind_speed_kmh", min_value=0, max_value=300, result_format="BASIC"
    )
    report.outcomes.append(_outcome_from_ge_result(r, "expect_column_values_to_be_between", "wind_speed_kmh"))

    r = ds.expect_column_values_to_be_between(
        "pressure_hpa", min_value=870, max_value=1085, result_format="BASIC"
    )
    report.outcomes.append(_outcome_from_ge_result(r, "expect_column_values_to_be_between", "pressure_hpa"))

    return report
