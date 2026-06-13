import pandas as pd

from plugins.checks.quality_gate import run_quality_gate


def _good_df(partition_date: str = "2026-06-12") -> pd.DataFrame:
    hours = pd.date_range(f"{partition_date}T00:00:00Z", periods=24, freq="h")
    return pd.DataFrame(
        {
            "record_id": [f"paris-{h.strftime('%Y-%m-%d-%H')}" for h in hours],
            "location": ["Paris"] * 24,
            "observation_ts": hours.astype(str),
            "temperature_c": [15.0 + i * 0.1 for i in range(24)],
            "humidity_pct": [60.0] * 24,
            "precipitation_mm": [0.0] * 24,
            "wind_speed_kmh": [10.0] * 24,
            "pressure_hpa": [1013.0] * 24,
        }
    )


def test_clean_batch_passes():
    report = run_quality_gate(_good_df(), partition_date="2026-06-12")
    assert report.success
    assert report.failures == []


def test_missing_column_fails_fast():
    df = _good_df().drop(columns=["pressure_hpa"])
    report = run_quality_gate(df, partition_date="2026-06-12")
    assert not report.success
    assert len(report.outcomes) == 1
    assert "pressure_hpa" in report.outcomes[0].details


def test_row_count_mismatch_fails():
    df = _good_df().iloc[:20]
    report = run_quality_gate(df, partition_date="2026-06-12")
    assert not report.success
    types = [o.expectation_type for o in report.failures]
    assert "expect_table_row_count_to_equal" in types


def test_duplicate_record_id_fails():
    df = _good_df()
    df.loc[1, "record_id"] = df.loc[0, "record_id"]
    report = run_quality_gate(df, partition_date="2026-06-12")
    assert not report.success
    types = [o.expectation_type for o in report.failures]
    assert "expect_column_values_to_be_unique" in types


def test_null_record_id_fails():
    df = _good_df()
    df.loc[0, "record_id"] = None
    report = run_quality_gate(df, partition_date="2026-06-12")
    assert not report.success
    types = [o.expectation_type for o in report.failures]
    assert "expect_column_values_to_not_be_null" in types


def test_implausible_temperature_fails():
    df = _good_df()
    df.loc[0, "temperature_c"] = 999.0  # e.g. Fahrenheit/Celsius mixup
    report = run_quality_gate(df, partition_date="2026-06-12")
    assert not report.success
    failed_cols = [o.column for o in report.failures]
    assert "temperature_c" in failed_cols


def test_humidity_out_of_range_fails():
    df = _good_df()
    df.loc[0, "humidity_pct"] = 150.0
    report = run_quality_gate(df, partition_date="2026-06-12")
    assert not report.success
    failed_cols = [o.column for o in report.failures]
    assert "humidity_pct" in failed_cols


def test_negative_precipitation_fails():
    df = _good_df()
    df.loc[0, "precipitation_mm"] = -5.0
    report = run_quality_gate(df, partition_date="2026-06-12")
    assert not report.success
    failed_cols = [o.column for o in report.failures]
    assert "precipitation_mm" in failed_cols


def test_observation_ts_wrong_partition_fails():
    df = _good_df()
    df.loc[0, "observation_ts"] = "2026-06-10T00:00:00Z"
    report = run_quality_gate(df, partition_date="2026-06-12")
    assert not report.success
    types = [o.expectation_type for o in report.failures]
    assert "expect_column_values_to_match_partition_date" in types


def test_pressure_out_of_range_fails():
    df = _good_df()
    df.loc[0, "pressure_hpa"] = 2000.0
    report = run_quality_gate(df, partition_date="2026-06-12")
    assert not report.success
    failed_cols = [o.column for o in report.failures]
    assert "pressure_hpa" in failed_cols


def test_report_round_trips_through_dict():
    report = run_quality_gate(_good_df(), partition_date="2026-06-12")
    d = report.to_dict()
    from plugins.checks.quality_gate import ValidationReport

    rebuilt = ValidationReport.from_dict(d)
    assert rebuilt.success == report.success
    assert len(rebuilt.outcomes) == len(report.outcomes)
