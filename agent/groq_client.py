"""Thin wrapper around the Groq chat completions API with tool-calling support."""
from __future__ import annotations

import json
from typing import Any

import requests

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


def chat(
    api_key: str,
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    temperature: float = 0.1,
) -> dict[str, Any]:
    """Call the Groq chat completions endpoint and return the raw response JSON."""
    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    response = requests.post(
        GROQ_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        data=json.dumps(payload),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()
