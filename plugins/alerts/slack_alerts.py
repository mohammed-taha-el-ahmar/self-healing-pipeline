"""
Alerting for the daily weather pipeline.

Failures are reported with actionable context: which GE expectations
failed, how many rows were affected, where the batch was quarantined
(Postgres table + partition_date), and a direct link back to the Airflow
run. Built from the same ValidationReport the DAG already computed --
one source of truth for "what broke and why".
"""

from __future__ import annotations

from plugins.checks.quality_gate import ValidationReport


def build_quarantine_alert_blocks(
    *,
    partition_date: str,
    report: ValidationReport,
    dag_run_url: str,
) -> dict:
    """Slack Block Kit payload for a batch routed to weather_quarantine."""
    failure_lines = "\n".join(
        f"• *{f.expectation_type}*"
        + (f" on `{f.column}`" if f.column else "")
        + f" — {f.details}"
        for f in report.failures
    ) or "_none_"

    text = (
        f":rotating_light: *Pipeline quarantined batch for {partition_date}*\n\n"
        f"*Failed expectations:*\n{failure_lines}\n\n"
        f"*Quarantined to:* `weather_quarantine` (partition_date = `{partition_date}`)\n\n"
        f"<{dag_run_url}|View DAG run in Airflow>\n\n"
        f"*Next steps:* inspect `weather_quarantine` for `{partition_date}`, "
        f"investigate the upstream source for that date, then clear the "
        f"`validate` task and downstream tasks to re-run -- the load is "
        f"idempotent, so a re-run safely replaces this quarantine entry "
        f"or promotes the batch to `weather_observations`."
    )

    return {"blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]}


def build_task_failure_message(
    *, partition_date: str, task_id: str, exception: str, dag_run_url: str
) -> dict:
    text = (
        f":x: *Task `{task_id}` failed for partition {partition_date}*\n\n"
        f"```{exception[:1500]}```\n\n"
        f"<{dag_run_url}|View DAG run in Airflow>"
    )
    return {"blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]}
