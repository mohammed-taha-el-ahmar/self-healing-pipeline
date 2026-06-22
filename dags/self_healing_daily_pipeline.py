"""
self_healing_daily_pipeline
============================

Architecture
------------
Source API (Open-Meteo) -> Airflow DAG (extract -> validate -> load) ->
Great Expectations data quality gate ->
  - pass -> weather_observations (Postgres warehouse table)
  - fail -> weather_quarantine (Postgres) + Slack alert

--------------------------------------------------------------------------
Key decisions
--------------------------------------------------------------------------

Airflow over cron
  Needed dependency management (extract must finish before validate,
  validate before load), automatic retries with backoff for transient
  API/network failures, and the ability to backfill any historical date
  on demand via `airflow dags backfill -s ... -e ...`.

Quarantine on failure, not fail-fast
  If the Great Expectations suite fails for a partition, that batch is
  written to `weather_quarantine` (with the full validation report) and a
  Slack alert is sent -- but the DAG run itself succeeds. The pipeline
  does not halt, and every other date's data keeps flowing normally.
  Bad batches are isolated for inspection, not allowed to silently land
  in the warehouse table dashboards read from.

Idempotent task design
  Every task is keyed by `ds` (the logical date), never wall-clock time.
  `load_to_warehouse` and `load_to_quarantine` both DELETE existing rows
  for `partition_date` before inserting -- so re-running a date, or
  backfilling a range, never creates duplicates and always reflects the
  latest run's output. This is what makes backfills safe.

--------------------------------------------------------------------------
Stack
--------------------------------------------------------------------------
Apache Airflow, Python, Great Expectations, Postgres, Docker Compose.
See docker-compose.yml for the full local stack (Airflow webserver +
scheduler + Postgres metadata DB + Postgres warehouse DB).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.slack.notifications.slack import send_slack_notification

from agent.config import AgentConfig
from agent.investigator import investigate
from agent.reporters import markdown_reporter, slack_reporter
from agent.tools import get_quarantine_summary
from alerts.slack_alerts import build_quarantine_alert_blocks
from checks.quality_gate import ValidationReport, run_quality_gate
from load.postgres_loader import (
    clear_quarantine,
    clear_warehouse,
    load_to_quarantine,
    load_to_warehouse,
)
from source.source_api import fetch_daily_observations

logger = logging.getLogger(__name__)

SLACK_CONN_ID = "slack_alerts"
SLACK_CHANNEL = "#data-pipeline-alerts"


def _dag_run_url(context) -> str:
    try:
        dag_run = context["dag_run"]
        base_url = Variable.get("airflow_base_url", default_var="http://localhost:8080")
        return f"{base_url}/dags/{dag_run.dag_id}/grid?dag_run_id={dag_run.run_id}"
    except Exception:
        return "http://localhost:8080"


def _on_task_failure_alert(context):
    """on_failure_callback: posts an actionable Slack message for any
    unhandled task failure (e.g. the source API is unreachable after all
    retries are exhausted)."""
    from alerts.slack_alerts import build_task_failure_message

    ti = context["task_instance"]
    exception = str(context.get("exception", "unknown error"))
    msg = build_task_failure_message(
        partition_date=context["ds"],
        task_id=ti.task_id,
        exception=exception,
        dag_run_url=_dag_run_url(context),
    )
    notifier = send_slack_notification(
        slack_conn_id=SLACK_CONN_ID,
        text=msg["blocks"][0]["text"]["text"],
        channel=SLACK_CHANNEL,
        blocks=msg["blocks"],
    )
    notifier.notify(context)


default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "on_failure_callback": _on_task_failure_alert,
}


@dag(
    dag_id="self_healing_daily_pipeline",
    description="Source API -> extract -> validate (GE) -> load to warehouse or quarantine",
    schedule="0 6 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=True,
    max_active_runs=3,
    default_args=default_args,
    tags=["ingestion", "data-quality", "great-expectations", "postgres"],
    doc_md=__doc__,
)
def self_healing_daily_pipeline():

    @task
    def extract(ds: str) -> str:
        """Pull one day's hourly observations from the source API.

        Idempotent: always requests the same fixed date range (`ds` to
        `ds`) from the upstream API, so a re-fetch for the same partition
        returns the same logical data (modulo the upstream provider
        revising historical records, which is rare and itself a valid
        re-run scenario).

        Returns the data as JSON (records orientation) via XCom -- small
        enough (24 rows/day) that no intermediate file storage is needed.
        Network/API errors propagate so Airflow's retry+backoff applies.
        """
        df = fetch_daily_observations(ds)
        logger.info("Fetched %d observation rows for %s", len(df), ds)
        return df.to_json(orient="records", date_format="iso")

    @task
    def validate(raw_json: str, ds: str) -> dict:
        """Run the Great Expectations suite and return the serialized
        ValidationReport. Pure function of the input data + ds."""
        df = pd.read_json(StringIO(raw_json), orient="records")
        report = run_quality_gate(df, partition_date=ds)
        logger.info("Validation report for %s:\n%s", ds, report.summary())
        return report.to_dict()

    @task.branch
    def branch_on_quality(report: dict) -> str:
        """Route the batch based on the Great Expectations validation result.

        Returns exactly one downstream task_id:
          - "load_to_warehouse_task" if all expectations passed
          - "quarantine_task" if any expectation failed

        The returned string MUST match a downstream task's function name
        (which Airflow uses as the task_id). The other branch is
        automatically skipped by Airflow's branching mechanism.
        """
        return "load_to_warehouse_task" if report["success"] else "quarantine_task"

    @task
    def load_to_warehouse_task(raw_json: str, ds: str):
        """Validation passed -> delete-then-insert this partition's rows
        into weather_observations, and clear any stale quarantine entry
        for the same date (in case this is a re-run that fixed a prior
        failure)."""
        df = pd.read_json(StringIO(raw_json), orient="records")
        n = load_to_warehouse(df, partition_date=ds)
        # If this date previously failed and landed in quarantine, clean
        # up the stale quarantine row so data never exists in both tables.
        clear_quarantine(partition_date=ds)
        logger.info("Loaded %d rows for %s into weather_observations", n, ds)

    @task
    def quarantine_task(raw_json: str, report: dict, ds: str):
        """Validation failed -> delete-then-insert this partition's rows
        into weather_quarantine (with the validation report attached),
        clear any stale warehouse rows for the same date, and alert Slack.

        The DAG run succeeds even though the *data* was rejected -- other
        dates' runs are unaffected."""
        df = pd.read_json(StringIO(raw_json), orient="records")
        load_to_quarantine(df, partition_date=ds, validation_report=report)
        # Mirror of clear_quarantine above: if this date previously
        # passed but now fails (e.g. after a schema change upstream),
        # remove the stale warehouse rows.
        clear_warehouse(partition_date=ds)

        # Build and send a Slack alert with actionable failure details.
        rebuilt = ValidationReport.from_dict(report)
        context = get_current_context()
        slack_msg = build_quarantine_alert_blocks(
            partition_date=ds,
            report=rebuilt,
            dag_run_url=_dag_run_url(context),
        )
        notifier = send_slack_notification(
            slack_conn_id=SLACK_CONN_ID,
            text=slack_msg["blocks"][0]["text"]["text"],
            channel=SLACK_CHANNEL,
            blocks=slack_msg["blocks"],
        )
        # Slack alerting is best-effort: a missing or invalid token
        # should not prevent the quarantine write from succeeding.
        try:
            notifier.notify(context)
        except Exception as e:
            logger.warning("Slack alert failed (non-fatal): %s", e)
        logger.info("Quarantined %d rows for %s", len(df), ds)

    @task(trigger_rule="all_done")
    def investigate_if_quarantined(ds: str) -> None:
        """LLM-powered root-cause investigation, triggered after every run.

        trigger_rule="all_done" ensures this task fires regardless of which
        branch executed — a "successful" DAG run can still contain quarantined
        data worth investigating.

        Pre-check: if nothing was quarantined for this date, skip immediately
        so the Groq API is never called on clean runs.
        """
        cfg = AgentConfig.from_env()

        summary = get_quarantine_summary(cfg, ds)
        if summary["total_quarantined"] == 0:
            raise AirflowSkipException(f"No quarantined records for {ds} — nothing to investigate.")

        verdict = investigate(cfg, dag_id="self_healing_daily_pipeline", run_date=ds)
        report_path = markdown_reporter.write(
            verdict, "self_healing_daily_pipeline", ds, cfg.reports_dir
        )
        logger.info("Investigation verdict for %s: %s (confidence=%s)", ds, verdict.get("root_cause"), verdict.get("confidence"))

        if cfg.slack_webhook_url:
            try:
                slack_reporter.post(
                    cfg.slack_webhook_url, verdict, "self_healing_daily_pipeline", ds,
                    report_url=report_path,
                )
            except Exception as e:
                logger.warning("Agent Slack report failed (non-fatal): %s", e)

        logger.info("Investigation complete for %s -> %s", ds, report_path)

    # -----------------------------------------------------------------
    # DAG wiring
    # -----------------------------------------------------------------
    # TaskFlow API passes data between tasks via XCom automatically.
    # `ds="{{ ds }}"` injects the logical/partition date (YYYY-MM-DD)
    # at execution time -- this is what makes each run idempotent and
    # keyed by date rather than wall-clock time.
    raw = extract(ds="{{ ds }}")
    report = validate(raw, ds="{{ ds }}")
    branch = branch_on_quality(report)

    warehouse_t = load_to_warehouse_task(raw, ds="{{ ds }}")
    quarantine_t = quarantine_task(raw, report, ds="{{ ds }}")

    # Only one of these two tasks will actually execute per run;
    # the other is skipped by branch_on_quality.
    branch >> [warehouse_t, quarantine_t]

    # investigate_if_quarantined runs after both load paths regardless of
    # outcome (trigger_rule="all_done"). It skips itself on clean runs.
    investigation = investigate_if_quarantined(ds="{{ ds }}")
    [warehouse_t, quarantine_t] >> investigation


self_healing_daily_pipeline()
