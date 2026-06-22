"""Evidence-gathering tools the investigation agent can call.

Every tool is read-only by design: the agent's job is to diagnose and
report, not to mutate pipeline state. Remediation, if any, stays a human
decision.

Adaptation notes vs. the addon template
-----------------------------------------
- `weather_quarantine` stores the full GE `validation_report` as a JSON
  column, not a `validation_failure_reason` string. Failure reasons are
  extracted by parsing that JSON.
- The partition key is `partition_date`, not `ingestion_date`.
- Raw API payloads are not stored; `get_schema_diff` instead checks which
  observation columns are fully null (a proxy for upstream field removal).
- `get_ge_validation_result` reads the stored ValidationReport from the
  quarantine table rather than from Great Expectations' file system.
- Airflow log path structure: `dag_id=X/run_id=Y/task_id=Z/attempt=N.log`.
"""
from __future__ import annotations

import glob
import json
import os
from typing import Any

import psycopg2
import psycopg2.extras

from agent.config import AgentConfig


def get_quarantine_summary(cfg: AgentConfig, run_date: str) -> dict[str, Any]:
    """Summarize rows quarantined for a given run date.

    Parses the stored GE ValidationReport JSON to extract which
    expectations failed and how many rows were affected.
    """
    count_query = """
        SELECT COUNT(*) AS n
        FROM weather_quarantine
        WHERE partition_date = %s;
    """
    sample_query = """
        SELECT record_id, observation_ts, temperature_c, humidity_pct,
               precipitation_mm, wind_speed_kmh, pressure_hpa, quarantined_at,
               validation_report
        FROM weather_quarantine
        WHERE partition_date = %s
        ORDER BY quarantined_at DESC
        LIMIT 5;
    """
    with psycopg2.connect(cfg.postgres_dsn) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(count_query, (run_date,))
            count_row = cur.fetchone()
            cur.execute(sample_query, (run_date,))
            samples = cur.fetchall()

    total = count_row["n"] if count_row else 0

    # Parse failed expectations out of the stored GE report (same for all
    # rows in a partition — every row shares the batch-level verdict).
    failed_expectations: list[dict[str, Any]] = []
    if samples:
        try:
            report = json.loads(samples[0]["validation_report"] or "{}")
            failed_expectations = [
                {
                    "expectation_type": o["expectation_type"],
                    "column": o.get("column"),
                    "details": o.get("details"),
                    "unexpected_count": o.get("unexpected_count"),
                }
                for o in report.get("outcomes", [])
                if not o.get("success", True)
            ]
        except (json.JSONDecodeError, KeyError):
            pass

    # Strip the full validation_report blob from sample rows — the
    # structured failures above are more useful to the agent.
    sample_records = [
        {k: v for k, v in row.items() if k != "validation_report"}
        for row in samples
    ]

    return {
        "run_date": run_date,
        "total_quarantined": total,
        "failed_expectations": failed_expectations,
        "sample_records": sample_records,
    }


def get_ge_validation_result(cfg: AgentConfig, run_date: str) -> dict[str, Any]:
    """Load the Great Expectations validation result for a run date.

    Reads the stored ValidationReport JSON from the quarantine table —
    the pipeline writes the full GE result to every quarantined row.
    """
    query = """
        SELECT validation_report
        FROM weather_quarantine
        WHERE partition_date = %s
        LIMIT 1;
    """
    with psycopg2.connect(cfg.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (run_date,))
            row = cur.fetchone()

    if row is None:
        return {"run_date": run_date, "found": False, "failed_expectations": []}

    try:
        report = json.loads(row[0] or "{}")
        failed = [
            {
                "expectation_type": o["expectation_type"],
                "column": o.get("column"),
                "details": o.get("details"),
                "unexpected_count": o.get("unexpected_count"),
                "element_count": o.get("element_count"),
            }
            for o in report.get("outcomes", [])
            if not o.get("success", True)
        ]
        return {"run_date": run_date, "found": True, "failed_expectations": failed}
    except (json.JSONDecodeError, KeyError) as exc:
        return {"run_date": run_date, "found": True, "parse_error": str(exc)}


def get_airflow_task_logs(
    cfg: AgentConfig, dag_id: str, task_id: str, run_date: str, tail_lines: int = 80
) -> dict[str, Any]:
    """Return the tail of the most recent Airflow log for a task on a given date.

    Airflow log path structure (Airflow 2.x):
        <logs_dir>/dag_id=<dag_id>/run_id=*<run_date>*/task_id=<task_id>/attempt=*.log
    """
    pattern = os.path.join(
        cfg.airflow_logs_dir,
        f"dag_id={dag_id}",
        f"run_id=*{run_date}*",
        f"task_id={task_id}",
        "attempt=*.log",
    )
    matches = sorted(glob.glob(pattern))
    if not matches:
        return {"dag_id": dag_id, "task_id": task_id, "found": False, "log_tail": ""}

    with open(matches[-1], encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    return {
        "dag_id": dag_id,
        "task_id": task_id,
        "found": True,
        "log_file": matches[-1],
        "log_tail": "".join(lines[-tail_lines:]),
    }


def get_schema_diff(cfg: AgentConfig, run_date: str) -> dict[str, Any]:
    """Check observation column null-rates against the stored schema baseline.

    Since raw API payloads are not stored in the quarantine table, this
    tool checks whether the expected observation columns are actually
    populated. A column with 0 non-null values across all quarantined rows
    for the date is a strong signal that the upstream API stopped sending
    that field.
    """
    with open(cfg.schema_baseline_path, encoding="utf-8") as f:
        baseline = json.load(f)

    observation_cols = [
        "temperature_c",
        "humidity_pct",
        "precipitation_mm",
        "wind_speed_kmh",
        "pressure_hpa",
    ]
    null_check_query = """
        SELECT
            COUNT(*) AS total,
            COUNT(temperature_c) AS temperature_c_nn,
            COUNT(humidity_pct) AS humidity_pct_nn,
            COUNT(precipitation_mm) AS precipitation_mm_nn,
            COUNT(wind_speed_kmh) AS wind_speed_kmh_nn,
            COUNT(pressure_hpa) AS pressure_hpa_nn
        FROM weather_quarantine
        WHERE partition_date = %s;
    """
    with psycopg2.connect(cfg.postgres_dsn) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(null_check_query, (run_date,))
            row = cur.fetchone()

    if row is None or row["total"] == 0:
        return {"run_date": run_date, "comparable": False}

    all_null_columns = [
        col for col in observation_cols if row.get(f"{col}_nn", 0) == 0
    ]
    return {
        "run_date": run_date,
        "comparable": True,
        "baseline_expected_keys": baseline.get("expected_keys", []),
        "columns_all_null": all_null_columns,
        "total_rows": row["total"],
    }


TOOL_SPECS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_quarantine_summary",
            "description": (
                "Get counts and sample rows from the quarantine table for a run date, "
                "including parsed GE expectation failures."
            ),
            "parameters": {
                "type": "object",
                "properties": {"run_date": {"type": "string", "description": "YYYY-MM-DD"}},
                "required": ["run_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_ge_validation_result",
            "description": "Get failed Great Expectations checks for a run date from the quarantine table.",
            "parameters": {
                "type": "object",
                "properties": {"run_date": {"type": "string", "description": "YYYY-MM-DD"}},
                "required": ["run_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_airflow_task_logs",
            "description": "Get the tail of an Airflow task's log for a run date.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "task_id": {"type": "string"},
                    "run_date": {"type": "string", "description": "YYYY-MM-DD"},
                },
                "required": ["dag_id", "task_id", "run_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_schema_diff",
            "description": (
                "Check whether expected observation columns are fully null for a run date "
                "(proxy for upstream field removal or schema drift)."
            ),
            "parameters": {
                "type": "object",
                "properties": {"run_date": {"type": "string", "description": "YYYY-MM-DD"}},
                "required": ["run_date"],
            },
        },
    },
]

DISPATCH = {
    "get_quarantine_summary": get_quarantine_summary,
    "get_ge_validation_result": get_ge_validation_result,
    "get_airflow_task_logs": get_airflow_task_logs,
    "get_schema_diff": get_schema_diff,
}
