"""Tests for the agent investigation loop using a stubbed Groq client."""
from __future__ import annotations

import json

import pytest

from agent.config import AgentConfig
from agent.investigator import InvestigationError, investigate


def make_cfg(**overrides) -> AgentConfig:
    base = dict(
        groq_api_key="test-key",
        groq_model="llama-3.3-70b-versatile",
        max_tool_iterations=3,
        postgres_dsn="postgresql://test",
        airflow_logs_dir="/tmp/logs",
        reports_dir="/tmp/reports",
        slack_webhook_url=None,
        schema_baseline_path="/tmp/baseline.json",
    )
    base.update(overrides)
    return AgentConfig(**base)


def test_investigate_resolves_after_one_tool_call(monkeypatch):
    call_log = []

    def fake_chat(api_key, model, messages, tools=None, temperature=0.1):
        call_log.append(messages)
        if len(call_log) == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call_1",
                                    "function": {
                                        "name": "get_quarantine_summary",
                                        "arguments": json.dumps({"run_date": "2026-06-15"}),
                                    },
                                }
                            ],
                        }
                    }
                ]
            }
        verdict = {
            "root_cause": "Schema drift in upstream API",
            "confidence": "high",
            "severity": "warning",
            "evidence_summary": ["12 records quarantined"],
            "recommended_fix": "Update schema baseline",
            "requires_human": True,
        }
        return {"choices": [{"message": {"role": "assistant", "content": json.dumps(verdict)}}]}

    def fake_tool(cfg, run_date):
        return {"run_date": run_date, "total_quarantined": 12}

    monkeypatch.setattr("agent.investigator.chat", fake_chat)
    monkeypatch.setattr("agent.investigator.DISPATCH", {"get_quarantine_summary": fake_tool})

    cfg = make_cfg()
    verdict = investigate(cfg, dag_id="weather_pipeline", run_date="2026-06-15")

    assert verdict["root_cause"] == "Schema drift in upstream API"
    assert verdict["requires_human"] is True
    assert len(call_log) == 2


def test_investigate_raises_when_no_verdict_within_budget(monkeypatch):
    def always_calls_tool(api_key, model, messages, tools=None, temperature=0.1):
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call_x",
                                "function": {
                                    "name": "get_quarantine_summary",
                                    "arguments": json.dumps({"run_date": "2026-06-15"}),
                                },
                            }
                        ],
                    }
                }
            ]
        }

    monkeypatch.setattr("agent.investigator.chat", always_calls_tool)
    monkeypatch.setattr(
        "agent.investigator.DISPATCH",
        {"get_quarantine_summary": lambda cfg, run_date: {"total_quarantined": 1}},
    )

    cfg = make_cfg(max_tool_iterations=2)
    with pytest.raises(InvestigationError):
        investigate(cfg, dag_id="weather_pipeline", run_date="2026-06-15")
