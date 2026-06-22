"""Render an investigation verdict as a markdown incident report."""
from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

TEMPLATE = """# Incident Report — {dag_id} / {run_date}

**Generated:** {generated_at}
**Severity:** {severity}
**Confidence:** {confidence}

## Root cause

{root_cause}

## Evidence

{evidence}

## Recommended fix

{recommended_fix}

## Requires human follow-up

{requires_human}
"""


def render(verdict: dict[str, Any], dag_id: str, run_date: str) -> str:
    evidence = "\n".join(f"- {item}" for item in verdict.get("evidence_summary", []))
    return TEMPLATE.format(
        dag_id=dag_id,
        run_date=run_date,
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        severity=verdict.get("severity", "unknown"),
        confidence=verdict.get("confidence", "unknown"),
        root_cause=verdict.get("root_cause", "Not determined"),
        evidence=evidence or "- No evidence captured",
        recommended_fix=verdict.get("recommended_fix", "N/A"),
        requires_human="Yes" if verdict.get("requires_human") else "No",
    )


def write(verdict: dict[str, Any], dag_id: str, run_date: str, reports_dir: str) -> str:
    """Write the rendered report to disk and return the file path."""
    os.makedirs(reports_dir, exist_ok=True)
    path = os.path.join(reports_dir, f"{run_date}_{dag_id}_incident.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(render(verdict, dag_id, run_date))
    return path
