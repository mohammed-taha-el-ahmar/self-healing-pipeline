"""Configuration for the root-cause investigation agent.

All settings are sourced from environment variables so the agent behaves
identically whether it's triggered locally via docker-compose or inside
an Airflow worker container.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AgentConfig:
    groq_api_key: str
    groq_model: str
    max_tool_iterations: int
    postgres_dsn: str
    airflow_logs_dir: str
    reports_dir: str
    slack_webhook_url: str | None
    schema_baseline_path: str

    @classmethod
    def from_env(cls) -> "AgentConfig":
        return cls(
            groq_api_key=_require_env("GROQ_API_KEY"),
            groq_model=os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
            max_tool_iterations=int(os.getenv("AGENT_MAX_TOOL_ITERATIONS", "6")),
            postgres_dsn=_resolve_postgres_dsn(),
            airflow_logs_dir=os.getenv("AIRFLOW_LOGS_DIR", "/opt/airflow/logs"),
            reports_dir=os.getenv("AGENT_REPORTS_DIR", "/opt/airflow/agent/reports"),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL") or None,
            schema_baseline_path=os.getenv(
                "SCHEMA_BASELINE_PATH", "/opt/airflow/agent/schema_baseline.json"
            ),
        )


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _resolve_postgres_dsn() -> str:
    """Return a psycopg2-native DSN for the warehouse database.

    Accepts either PIPELINE_POSTGRES_DSN (plain postgresql://) or falls
    back to WAREHOUSE_DB_URL (which may use the SQLAlchemy-specific
    'postgresql+psycopg2://' scheme) and strips the driver suffix.
    """
    dsn = os.getenv("PIPELINE_POSTGRES_DSN") or os.getenv("WAREHOUSE_DB_URL")
    if not dsn:
        raise RuntimeError(
            "Missing required environment variable: PIPELINE_POSTGRES_DSN or WAREHOUSE_DB_URL"
        )
    # Strip the SQLAlchemy driver suffix if present
    return dsn.replace("postgresql+psycopg2://", "postgresql://")
