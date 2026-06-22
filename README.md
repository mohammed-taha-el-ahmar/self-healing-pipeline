# Self-Healing Daily Pipeline (Airflow + Great Expectations + Postgres)

A daily ingestion pipeline that catches bad data before it reaches
dashboards — and tells you exactly what broke and why.

![project worflow](https://raw.githubusercontent.com/mohammed-taha-el-ahmar/self-healing-pipeline/main/docs/img/workflow.png)

## Architecture

```
Source API (Open-Meteo)
   |
   v
Airflow DAG: extract -> validate -> load
   |              |          |
   |              |          +-- branch on GE validation result
   |              v
   |     Great Expectations quality gate
   |              |
   |     +--------+--------+
   |     | pass            | fail
   v     v                 v
weather_observations   weather_quarantine
(Postgres warehouse)   (Postgres, + GE report)
                              |
                              v
                         Slack alert
```

## Stack

- **Orchestration:** Apache Airflow 2.9 (TaskFlow API)
- **Source:** [Open-Meteo](https://open-meteo.com) weather API (free, no
  API key) — pulls 24 hourly observations for Paris per partition date
- **Data quality:** Great Expectations 0.18 (`PandasDataset` API) as the
  primary validation engine
- **Warehouse:** Postgres, loaded via SQLAlchemy
- **Alerting:** Slack (`apache-airflow-providers-slack`)
- **Investigation agent:** Groq `llama-3.3-70b-versatile` via tool-calling
  — automatically investigates quarantined batches and ships a structured
  incident report
- **Local stack:** Docker Compose (Airflow webserver + scheduler,
  Airflow metadata Postgres, warehouse Postgres)
- **Dependency management:** [uv](https://docs.astral.sh/uv/)

## Project layout

```
dags/
  self_healing_daily_pipeline.py   # the DAG (extract → validate → load → investigate)
plugins/
  source/source_api.py             # Open-Meteo client (extract)
  checks/quality_gate.py           # Great Expectations suite + ValidationReport
  load/postgres_loader.py          # SQLAlchemy load to warehouse/quarantine tables
  alerts/slack_alerts.py           # Slack message builders
agent/
  config.py                        # AgentConfig (env-driven)
  investigator.py                  # tool-calling loop → structured verdict
  tools.py                         # 4 read-only evidence tools (quarantine, GE, logs, schema)
  prompts.py                       # system prompt + JSON verdict schema
  groq_client.py                   # thin Groq API wrapper
  schema_baseline.json             # expected Open-Meteo field names
  reporters/
    markdown_reporter.py           # writes incident report to agent/reports/
    slack_reporter.py              # posts verdict summary to Slack webhook
tests/
  test_quality_gate.py             # GE suite tests (no Airflow/DB needed)
  test_agent_investigator.py       # agent loop tests (Groq + DB stubbed)
  test_agent_tools.py              # evidence tool tests (DB stubbed)
  test_agent_reporters.py          # reporter tests
docker-compose.yml                 # Airflow + warehouse Postgres stack
pyproject.toml                     # uv-managed dependencies
docs/commands.md                   # useful CLI commands quick-reference
```

## Setup

### 1. Local dev / unit tests (no Docker required)

```bash
uv sync --extra dev
uv run pytest tests/
```

### 2. Full stack via Docker Compose

```bash
# On Linux: map your host UID so mounted volumes are writable
echo "AIRFLOW_UID=$(id -u)" >> .env

# On macOS: use the container's built-in airflow user (see Troubleshooting)
echo "AIRFLOW_UID=50000" > .env

# Optional: fill in agent credentials in .env to enable LLM investigation
# GROQ_API_KEY=gsk_...
# SLACK_WEBHOOK_URL=https://hooks.slack.com/...

docker compose up airflow-init   # one-time DB migration + admin user
docker compose up -d             # starts webserver, scheduler, both Postgres DBs
```

- Airflow UI: http://localhost:8080 (admin/admin)
- Warehouse Postgres (interactive session from inside the container):

  ```bash
  docker compose exec warehouse-db psql -U warehouse warehouse
  ```

  Or, if you have `psql` installed locally: `psql -h localhost -p 5433 -U warehouse warehouse` (password: `warehouse`)

Configure the Slack connection (already stubbed via
`AIRFLOW_CONN_SLACK_ALERTS` in `docker-compose.yml` — replace the bot
token, or override via the Airflow UI under Admin → Connections →
`slack_alerts`).

### 3. Run the pipeline

The DAG `self_healing_daily_pipeline` is scheduled daily at 06:00. To
backfill historical dates (Open-Meteo's forecast endpoint serves the last
~5 days; older dates automatically use its archive endpoint):

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill self_healing_daily_pipeline -s 2026-06-08 -e 2026-06-12
```

Inspect results:

```bash
docker compose exec warehouse-db psql -U warehouse warehouse -c \
  "SELECT partition_date, count(*) FROM weather_observations GROUP BY 1 ORDER BY 1;"

docker compose exec warehouse-db psql -U warehouse warehouse -c \
  "SELECT partition_date, validation_report FROM weather_quarantine;"
```

## Key design decisions

### Airflow over cron

- **Dependency management**: `extract -> validate -> load` is a DAG —
  load only happens after validation has produced a verdict, and the
  branch decides which table receives the data. Cron would need
  hand-rolled glue for "don't load if validation failed."
- **Retries with backoff**: each task retries up to 3 times with
  exponential backoff (`retry_exponential_backoff=True`, capped at 15
  min) — covers transient Open-Meteo API/network failures without manual
  intervention.
- **Backfills**: `airflow dags backfill -s ... -e ...` reprocesses a date
  range through the exact same `extract -> validate -> load` path as the
  daily run. Cron has no equivalent — you'd be re-running today's script
  with a fake date and hoping nothing assumes "today".
- **Observability**: per-task logs, durations, and status in the Airflow
  UI. A failure shows exactly which step (extract / validate / load)
  broke and why.

### Quarantine on failure, not fail-fast

The `branch_on_quality` task routes every run to exactly one of two
outcomes based on the Great Expectations validation result:

- **Pass** → `load_to_warehouse_task` deletes-then-inserts the day's rows
  into `weather_observations` (the table dashboards read from), and
  clears any stale quarantine entry for that date.
- **Fail** → `quarantine_task` deletes-then-inserts the day's rows into
  `weather_quarantine` *along with the full GE validation report* (which
  expectations failed, on which columns, how many rows), clears any stale
  warehouse rows for that date, and posts a Slack alert with the failure
  details and a link back to the run.

Critically, **the DAG run succeeds in both cases**. A bad batch never
fails the pipeline — it's isolated for inspection while every other
date's run proceeds normally. This is the opposite of a fail-fast design,
where one bad day would halt the DAG and back up every subsequent
scheduled run until someone intervenes.

### Idempotent task design

- All Postgres writes are **delete-then-insert by `partition_date`**, in
  both `weather_observations` and `weather_quarantine`
  (`postgres_loader.py`). Re-running a date — whether manually cleared or
  via `airflow dags backfill` — replaces that date's rows exactly, never
  appends duplicates.
- `record_id` (the warehouse primary key) is deterministically derived
  from `(location, observation_ts)` in `source_api.py`, so the same
  upstream record always maps to the same row, even across re-fetches.
- A pass-then-fail or fail-then-pass transition for the same date is
  handled explicitly: `load_to_warehouse_task` calls `clear_quarantine()`
  and `quarantine_task` calls `clear_warehouse()`, so a date never ends up
  with stale rows in both tables simultaneously.
- No task touches another partition's rows, so any single day — or any
  range — can be cleared and re-run safely. This is what makes backfills
  safe.

## Great Expectations suite

`plugins/checks/quality_gate.py` runs the following expectations against
each day's 24-row batch (see module docstring for full rationale):

- `expect_table_row_count_to_equal(24)` — a full day of hourly data
- `expect_column_values_to_not_be_null` / `_to_be_unique` on `record_id`
- `observation_ts` not null and within the partition date
- `temperature_c` between -30 and 50°C (catches unit/sensor errors)
- `humidity_pct` between 0 and 100
- `precipitation_mm` >= 0
- `wind_speed_kmh` between 0 and 300
- `pressure_hpa` between 870 and 1085 hPa

Any single failed expectation routes the whole batch to quarantine.

## Investigation agent

When data is quarantined, `investigate_if_quarantined` fires automatically
(via `trigger_rule="all_done"`) after every run. It runs a bounded
tool-calling loop using Groq's `llama-3.3-70b-versatile` to gather evidence
and return a structured verdict:

```json
{
  "root_cause": "temperature_c threshold too tight: all 24 values exceeded max_value=20",
  "confidence": "high",
  "severity": "warning",
  "evidence_summary": ["24/24 rows quarantined", "expect_column_values_to_be_between on temperature_c failed"],
  "recommended_fix": "Review temperature_c threshold in quality_gate.py",
  "requires_human": true
}
```

The agent has four **read-only** tools: quarantine summary, GE validation
results, Airflow task logs, and a schema column-null check. It diagnoses
only — no pipeline state is ever mutated by the agent.

The verdict is written to `agent/reports/<date>_self_healing_daily_pipeline_incident.md`
and optionally posted to a Slack webhook.

**To enable:** set `GROQ_API_KEY` in `.env`. `SLACK_WEBHOOK_URL` is
optional. If `GROQ_API_KEY` is absent the task will fail gracefully.
Clean runs (nothing quarantined) are skipped with `AirflowSkipException`
so no LLM call is made.



### `uid not found: 501` on macOS

```
KeyError: 'getpwuid(): uid not found: 501'
airflow.exceptions.AirflowConfigException: The user that Airflow is running
as has no username; you must run Airflow as a full user …
```

**Cause:** `id -u` on macOS returns **501**, which has no matching entry
in the Airflow container's `/etc/passwd`. The container crashes because
Airflow requires a resolvable username.

**Fix:** Use the container's built-in `airflow` user (UID 50000) instead:

```bash
echo "AIRFLOW_UID=50000" > .env
docker compose down && docker compose up airflow-init
```

On macOS with Docker Desktop, bind-mount permissions are handled
transparently by the VM layer, so using UID 50000 does not cause
permission issues on the host `dags/`, `plugins/`, or `logs/` directories.

On **Linux**, `echo "AIRFLOW_UID=$(id -u)" > .env` is correct — Linux
UIDs (typically 1000+) are added to the container's `/etc/passwd` at
runtime by the Airflow entrypoint.
