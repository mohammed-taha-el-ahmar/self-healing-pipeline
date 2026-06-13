"""
Postgres load layer (SQLAlchemy core, no ORM needed for this volume).

Two tables, both partitioned logically by `partition_date`:

- weather_observations : the "warehouse table" dashboards read from.
- weather_quarantine    : rows from batches that failed the quality gate,
                          stored alongside the GE validation result for
                          inspection, instead of being silently dropped.

Idempotency: both loaders DELETE any existing rows for `partition_date`
before inserting the new batch (delete-then-insert), so re-running a day
or backfilling a range never creates duplicates and always reflects the
latest run's output for that date.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    delete,
)

metadata = MetaData()

weather_observations = Table(
    "weather_observations",
    metadata,
    Column("record_id", String, primary_key=True),
    Column("partition_date", String, nullable=False, index=True),
    Column("location", String, nullable=False),
    Column("observation_ts", DateTime(timezone=True), nullable=False),
    Column("temperature_c", Float),
    Column("humidity_pct", Float),
    Column("precipitation_mm", Float),
    Column("wind_speed_kmh", Float),
    Column("pressure_hpa", Float),
    Column("loaded_at", DateTime(timezone=True), nullable=False),
)

weather_quarantine = Table(
    "weather_quarantine",
    metadata,
    Column("record_id", String, primary_key=True),
    Column("partition_date", String, nullable=False, index=True),
    Column("location", String),
    Column("observation_ts", String),
    Column("temperature_c", Float),
    Column("humidity_pct", Float),
    Column("precipitation_mm", Float),
    Column("wind_speed_kmh", Float),
    Column("pressure_hpa", Float),
    Column("quarantined_at", DateTime(timezone=True), nullable=False),
    Column("validation_report", Text),  # JSON-serialized GE summary
)


def get_engine():
    url = os.environ.get(
        "WAREHOUSE_DB_URL",
        "postgresql+psycopg2://airflow:airflow@postgres:5432/warehouse",
    )
    return create_engine(url, future=True)


def init_db(engine=None) -> None:
    """Create tables if they don't exist. Safe to call repeatedly."""
    engine = engine or get_engine()
    metadata.create_all(engine, checkfirst=True)


def load_to_warehouse(df: pd.DataFrame, partition_date: str, engine=None) -> int:
    """Delete-then-insert all rows for `partition_date` into
    weather_observations. Returns the number of rows inserted.

    Idempotent: running this twice for the same partition_date with the
    same input produces the same table contents, not duplicates.
    """
    engine = engine or get_engine()
    init_db(engine)

    now = datetime.now(timezone.utc)
    records = df.copy()
    records["partition_date"] = partition_date
    records["loaded_at"] = now
    records["observation_ts"] = pd.to_datetime(records["observation_ts"], utc=True)

    with engine.begin() as conn:
        conn.execute(
            delete(weather_observations).where(
                weather_observations.c.partition_date == partition_date
            )
        )
        if not records.empty:
            conn.execute(
                weather_observations.insert(),
                records.to_dict(orient="records"),
            )

    return len(records)


def load_to_quarantine(
    df: pd.DataFrame, partition_date: str, validation_report: dict, engine=None
) -> int:
    """Delete-then-insert all rows for `partition_date` into
    weather_quarantine, attaching the GE validation report (as JSON) to
    every row for traceability.

    Idempotent for the same reasons as load_to_warehouse: a re-run after
    a fix either re-quarantines (overwriting the prior quarantine record)
    or, if the batch now passes, the warehouse loader removes any stale
    quarantine rows for that date (see clear_quarantine).
    """
    engine = engine or get_engine()
    init_db(engine)

    now = datetime.now(timezone.utc)
    records = df.copy()
    records["partition_date"] = partition_date
    records["quarantined_at"] = now
    records["validation_report"] = json.dumps(validation_report)
    # observation_ts stored as text here since malformed timestamps are
    # exactly the kind of thing that lands rows in quarantine.
    records["observation_ts"] = records["observation_ts"].astype(str)

    with engine.begin() as conn:
        conn.execute(
            delete(weather_quarantine).where(
                weather_quarantine.c.partition_date == partition_date
            )
        )
        if not records.empty:
            conn.execute(
                weather_quarantine.insert(),
                records.to_dict(orient="records"),
            )

    return len(records)


def clear_quarantine(partition_date: str, engine=None) -> None:
    """Remove any quarantine rows for `partition_date`. Called when a
    re-run for that date now passes validation, so quarantine doesn't
    show a stale failure for a date that's since been fixed."""
    engine = engine or get_engine()
    init_db(engine)
    with engine.begin() as conn:
        conn.execute(
            delete(weather_quarantine).where(
                weather_quarantine.c.partition_date == partition_date
            )
        )


def clear_warehouse(partition_date: str, engine=None) -> None:
    """Remove any warehouse rows for `partition_date`. Called when a
    batch fails validation, so a previously-loaded (and now-superseded)
    good batch for the same date doesn't linger alongside a quarantine
    entry."""
    engine = engine or get_engine()
    init_db(engine)
    with engine.begin() as conn:
        conn.execute(
            delete(weather_observations).where(
                weather_observations.c.partition_date == partition_date
            )
        )
