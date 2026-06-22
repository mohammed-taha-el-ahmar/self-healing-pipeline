"""Agent loop: evidence gathering via tool calls, then a structured verdict."""
from __future__ import annotations

import json
import logging
from typing import Any

from agent.config import AgentConfig
from agent.groq_client import chat
from agent.prompts import SYSTEM_PROMPT
from agent.tools import DISPATCH, TOOL_SPECS

logger = logging.getLogger(__name__)


class InvestigationError(RuntimeError):
    """Raised when the agent fails to produce a parseable verdict."""


def investigate(cfg: AgentConfig, dag_id: str, run_date: str) -> dict[str, Any]:
    """Run the root-cause investigation loop for a given pipeline run.

    Returns a structured verdict dict (see prompts.SYSTEM_PROMPT for schema).
    Raises InvestigationError if the agent exhausts its tool-call budget
    without converging, or if the final response isn't valid JSON.
    """
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Investigate the pipeline run for dag_id='{dag_id}' on "
                f"run_date='{run_date}'. Data was quarantined and/or a validation "
                "failed for this run."
            ),
        },
    ]

    for iteration in range(cfg.max_tool_iterations):
        response = chat(cfg.groq_api_key, cfg.groq_model, messages, tools=TOOL_SPECS)
        choice = response["choices"][0]["message"]
        messages.append(choice)

        tool_calls = choice.get("tool_calls")
        if not tool_calls:
            return _parse_verdict(choice.get("content", ""))

        for call in tool_calls:
            result = _run_tool(cfg, dag_id, run_date, call)
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": call["id"],
                    "content": json.dumps(result, default=str),
                }
            )

        logger.info(
            "Investigation iteration %s/%s complete", iteration + 1, cfg.max_tool_iterations
        )

    raise InvestigationError(
        f"Agent did not converge on a verdict within {cfg.max_tool_iterations} tool iterations"
    )


def _run_tool(cfg: AgentConfig, dag_id: str, run_date: str, call: dict[str, Any]) -> dict[str, Any]:
    name = call["function"]["name"]
    args = json.loads(call["function"]["arguments"] or "{}")
    args.setdefault("run_date", run_date)
    if name == "get_airflow_task_logs":
        args.setdefault("dag_id", dag_id)

    fn = DISPATCH.get(name)
    if fn is None:
        return {"error": f"Unknown tool requested: {name}"}

    try:
        return fn(cfg, **args)
    except Exception as exc:  # noqa: BLE001 - surfaced to the agent, not swallowed silently
        logger.exception("Tool %s failed", name)
        return {"error": str(exc)}


def _parse_verdict(content: str) -> dict[str, Any]:
    try:
        return json.loads(content)
    except json.JSONDecodeError as exc:
        raise InvestigationError(f"Could not parse agent verdict as JSON: {content[:200]}") from exc
