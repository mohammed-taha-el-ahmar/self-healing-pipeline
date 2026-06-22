"""Tests for the markdown and Slack reporters."""
from __future__ import annotations

from agent.reporters import markdown_reporter, slack_reporter

SAMPLE_VERDICT = {
    "root_cause": (
        "Open-Meteo added a new 'precipitation_probability' field, "
        "shifting downstream column expectations."
    ),
    "confidence": "high",
    "severity": "warning",
    "evidence_summary": ["Schema diff shows 1 new key", "12 records quarantined"],
    "recommended_fix": "Update schema_baseline.json to include the new field.",
    "requires_human": True,
}


def test_markdown_report_contains_key_fields(tmp_path):
    path = markdown_reporter.write(SAMPLE_VERDICT, "weather_pipeline", "2026-06-15", str(tmp_path))
    content = open(path, encoding="utf-8").read()

    assert "weather_pipeline" in content
    assert "warning" in content
    assert SAMPLE_VERDICT["recommended_fix"] in content


def test_slack_post_sends_expected_payload(monkeypatch):
    captured = {}

    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["json"] = json

        class FakeResponse:
            def raise_for_status(self):
                return None

        return FakeResponse()

    monkeypatch.setattr("agent.reporters.slack_reporter.requests.post", fake_post)
    slack_reporter.post(
        "https://hooks.slack.com/fake", SAMPLE_VERDICT, "weather_pipeline", "2026-06-15"
    )

    assert captured["url"] == "https://hooks.slack.com/fake"
    assert "Root cause" in captured["json"]["text"]
