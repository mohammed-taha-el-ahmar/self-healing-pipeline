"""Post an investigation verdict to Slack via an incoming webhook."""
from __future__ import annotations

from typing import Any

import requests

SEVERITY_EMOJI = {
    "critical": ":red_circle:",
    "warning": ":large_yellow_circle:",
    "info": ":large_blue_circle:",
}


def post(
    webhook_url: str,
    verdict: dict[str, Any],
    dag_id: str,
    run_date: str,
    report_url: str | None = None,
) -> None:
    """Send a formatted incident summary to a Slack channel."""
    emoji = SEVERITY_EMOJI.get(verdict.get("severity", "info"), ":white_circle:")
    lines = [
        f"{emoji} *Data incident — {dag_id} ({run_date})*",
        f"*Root cause:* {verdict.get('root_cause', 'Not determined')}",
        f"*Confidence:* {verdict.get('confidence', 'unknown')} · "
        f"*Requires human:* {'Yes' if verdict.get('requires_human') else 'No'}",
        f"*Recommended fix:* {verdict.get('recommended_fix', 'N/A')}",
    ]
    if report_url:
        lines.append(f"Full report: {report_url}")

    response = requests.post(webhook_url, json={"text": "\n".join(lines)}, timeout=10)
    response.raise_for_status()
