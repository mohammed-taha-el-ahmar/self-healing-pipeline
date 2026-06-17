# Useful Commands

Quick reference for operating the self-healing pipeline locally.

---

## Docker Compose Lifecycle

```bash
# Create the .env file (macOS — use `$(id -u)` on Linux instead)
echo "AIRFLOW_UID=50000" > .env

# One-time: migrate Airflow DB + create admin user
docker compose up airflow-init

# Start all services in the background
docker compose up -d

# Stop all services (preserves volumes/data)
docker compose down

# Stop and destroy all data (fresh start)
docker compose down -v
```

---

## Airflow CLI (run inside the scheduler container)

### DAG Management

```bash
# List all registered DAGs
docker compose exec airflow-scheduler airflow dags list

# Check if the DAG parsed without errors
docker compose exec airflow-scheduler airflow dags report

# Pause / unpause the DAG
docker compose exec airflow-scheduler airflow dags pause self_healing_daily_pipeline
docker compose exec airflow-scheduler airflow dags unpause self_healing_daily_pipeline
```

### Triggering Runs

```bash
# Trigger a manual run for a specific logical date
docker compose exec airflow-scheduler \
  airflow dags trigger self_healing_daily_pipeline --exec-date 2026-06-15

# Backfill a date range (catchup must be True on the DAG)
docker compose exec airflow-scheduler \
  airflow dags backfill self_healing_daily_pipeline -s 2026-06-08 -e 2026-06-12
```

### Inspecting Runs

```bash
# List recent DAG runs
docker compose exec airflow-scheduler \
  airflow dags list-runs --dag-id self_healing_daily_pipeline

# Show task states for a specific run
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run self_healing_daily_pipeline \
  "scheduled__2026-06-01T06:00:00+00:00"
```

### Clearing Tasks (re-run)

```bash
# Clear a specific task for a date range (will be re-scheduled)
docker compose exec airflow-scheduler \
  airflow tasks clear self_healing_daily_pipeline \
  -t extract -s 2026-06-10 -e 2026-06-10 --yes

# Clear all tasks for a date (re-runs the entire pipeline for that day)
docker compose exec airflow-scheduler \
  airflow tasks clear self_healing_daily_pipeline \
  -s 2026-06-10 -e 2026-06-10 --yes

# Clear a task and all its downstream dependents
docker compose exec airflow-scheduler \
  airflow tasks clear self_healing_daily_pipeline \
  -t validate -s 2026-06-10 -e 2026-06-10 --downstream --yes
```

### Reading Logs

```bash
# Tail the scheduler logs
docker compose logs -f airflow-scheduler

# Read a specific task's log (attempt 1)
docker compose exec airflow-scheduler cat \
  "/opt/airflow/logs/dag_id=self_healing_daily_pipeline/run_id=scheduled__2026-06-10T06:00:00+00:00/task_id=validate/attempt=1.log"
```

---

## Warehouse Database (Postgres)

```bash
# Interactive session (uses psql inside the container — no local install needed)
docker compose exec warehouse-db psql -U warehouse warehouse

# Run a single query directly
docker compose exec warehouse-db psql -U warehouse warehouse -c "SELECT count(*) FROM weather_observations;"
```

If you have `psql` installed locally, you can also connect from the host:

```bash
psql -h localhost -p 5433 -U warehouse warehouse
# Password: warehouse
```

### Useful Queries

```sql
-- Count of rows loaded per day (good data)
SELECT partition_date, count(*)
FROM weather_observations
GROUP BY 1 ORDER BY 1;

-- View quarantined batches with their validation reports
SELECT partition_date, validation_report
FROM weather_quarantine
ORDER BY partition_date;

-- Check if a specific date landed in the warehouse or quarantine
SELECT 'warehouse' AS location, partition_date, count(*)
FROM weather_observations WHERE partition_date = '2026-06-10'
GROUP BY 1, 2
UNION ALL
SELECT 'quarantine', partition_date, count(*)
FROM weather_quarantine WHERE partition_date = '2026-06-10'
GROUP BY 1, 2;
```

---

## Local Development (no Docker)

```bash
# Install all dependencies including dev extras
uv sync --extra dev

# Run unit tests (quality gate suite, no Airflow or DB needed)
uv run pytest tests/ -v

# Run a single test
uv run pytest tests/test_quality_gate.py::test_valid_batch_passes -v
```

---

## Testing the Quarantine Path

To force data into quarantine without waiting for genuinely bad upstream
data, temporarily tighten a threshold in `plugins/checks/quality_gate.py`:

```python
# Original (production)
r = ds.expect_column_values_to_be_between(
    "temperature_c", min_value=-30, max_value=50, result_format="BASIC"
)

# Temporary (testing) — Paris in summer easily exceeds 20°C
r = ds.expect_column_values_to_be_between(
    "temperature_c", min_value=-30, max_value=20, result_format="BASIC"
)
```

Then trigger a run and verify the quarantine path:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger self_healing_daily_pipeline --exec-date 2026-06-15

# Wait ~30s, then check:
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run self_healing_daily_pipeline \
  "manual__2026-06-15T00:00:00+00:00"
```

Expected result: `load_to_warehouse_task` = skipped, `quarantine_task` = success.

**Remember to revert the threshold** back to `max_value=50` after testing.

---

## Airflow UI

- **URL:** http://localhost:8080
- **Credentials:** admin / admin
- **Useful views:**
  - *Grid* — shows per-date task outcomes (green/red/yellow/purple)
  - *Graph* — visualises the DAG structure and branching
  - *Admin → Connections* — configure `slack_alerts` with a real bot token
