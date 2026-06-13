"""
Source API client for the daily ingestion pipeline.

Uses Open-Meteo (https://open-meteo.com), a free, no-API-key-required
weather API, as the upstream "source system". Each daily run pulls one
day's worth of hourly observations for a fixed location (Paris) via the
historical/forecast endpoint, which serves both:

- "today's" data for the live daily schedule, and
- arbitrary past dates for backfills (Open-Meteo's forecast endpoint
  retains recent history; for older backfills the archive endpoint at
  archive-api.open-meteo.com is used automatically).

Why this API for the demo:
- No auth/API key -> nothing to configure for reviewers.
- Returns a clean hourly time series -> natural row-per-hour partition
  that maps well to "one row per record, one file per day".
- Stable schema, but real-world enough that occasional missing/null
  readings occur -> gives the quality gate real things to catch.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# Paris, France
LATITUDE = 48.8566
LONGITUDE = 2.3522

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "pressure_msl",
]

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Open-Meteo's forecast endpoint exposes "past_days" but not arbitrary
# historical dates; anything older than this many days is fetched from
# the archive endpoint instead.
FORECAST_LOOKBACK_DAYS = 5


class SourceAPIError(Exception):
    """Raised when the upstream API call fails or returns an unusable shape."""


def _pick_endpoint(partition_date: date) -> str:
    if (date.today() - partition_date).days > FORECAST_LOOKBACK_DAYS:
        return ARCHIVE_URL
    return FORECAST_URL


def fetch_daily_observations(ds: str, timeout: int = 30) -> pd.DataFrame:
    """Fetch one day's hourly weather observations for `ds` (YYYY-MM-DD).

    Returns a dataframe with one row per hour (24 rows on a normal day),
    columns: record_id, location, observation_ts, temperature_c,
    humidity_pct, precipitation_mm, wind_speed_kmh, pressure_hpa.

    Raises SourceAPIError on network failure, non-200 response, or a
    response missing the expected `hourly` block -- callers (the Airflow
    `extract` task) should let this propagate so Airflow's retry/backoff
    kicks in.
    """
    partition_date = datetime.strptime(ds, "%Y-%m-%d").date()
    url = _pick_endpoint(partition_date)

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": ds,
        "end_date": ds,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",
    }

    try:
        resp = requests.get(url, params=params, timeout=timeout)
    except requests.RequestException as exc:
        raise SourceAPIError(f"Request to {url} failed: {exc}") from exc

    if resp.status_code != 200:
        raise SourceAPIError(
            f"Source API returned {resp.status_code} for {ds}: {resp.text[:500]}"
        )

    payload = resp.json()
    hourly = payload.get("hourly")
    if not hourly or "time" not in hourly:
        raise SourceAPIError(f"Unexpected response shape for {ds}: missing 'hourly.time'")

    df = pd.DataFrame(
        {
            "observation_ts": hourly["time"],
            "temperature_c": hourly.get("temperature_2m"),
            "humidity_pct": hourly.get("relative_humidity_2m"),
            "precipitation_mm": hourly.get("precipitation"),
            "wind_speed_kmh": hourly.get("wind_speed_10m"),
            "pressure_hpa": hourly.get("pressure_msl"),
        }
    )

    df["location"] = "Paris"
    # Deterministic, idempotent record_id: same (location, hour) -> same id
    # on every re-fetch, so re-running a day never creates duplicate keys.
    df["record_id"] = df["location"].str.lower() + "-" + df["observation_ts"].str.replace(
        r"[:T]", "-", regex=True
    )

    cols = [
        "record_id",
        "location",
        "observation_ts",
        "temperature_c",
        "humidity_pct",
        "precipitation_mm",
        "wind_speed_kmh",
        "pressure_hpa",
    ]
    return df[cols]


if __name__ == "__main__":
    import sys

    target_ds = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    out = fetch_daily_observations(target_ds)
    print(out)
