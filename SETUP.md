# How to Configure & Run — India Weather & Air-Quality Pipeline

A step-by-step guide to set up this pipeline on a fresh machine (**macOS or Windows**)
and run it. It fetches weather + air-quality data for India from Open-Meteo and loads
it into a PostgreSQL database (`databank`).

For *what* the data means see [DATA_DICTIONARY.md](DATA_DICTIONARY.md); for *how it
works* see [DESIGN.md](DESIGN.md).

---

## 0. Prerequisites

| Need | Notes |
|---|---|
| **Python 3.10+** | the code uses modern type syntax (`X \| None`). Check: `python --version` |
| **PostgreSQL 14+** (running) | local install is fine. You must know the **superuser (`postgres`) password** you set at install. |
| **Internet** | to reach Open-Meteo + download the reference data |
| `curl` | ships with macOS and Windows 10+ |

> The pipeline is pure Python (pathlib, no shell calls) — identical commands on Mac
> and Windows. The only Windows-specific behaviour (a `__locked_retry` CSV fallback)
> is harmless and never triggers on macOS.

---

## 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

This installs pandas, geopandas, psycopg2, openmeteo-requests, etc. (wheels exist for
macOS incl. Apple Silicon, and Windows).

---

## 2. Make sure PostgreSQL is running

- **macOS:** e.g. `brew services start postgresql@16` (or Postgres.app).
- **Windows:** the "postgresql-x64-NN" service should be running (Services app), or it
  starts automatically after install.

Confirm it's listening on `localhost:5432` (the default). You do **not** need `psql` on
your PATH — the pipeline connects via Python.

---

## 3. Configure secrets — create `.env`

Create a file named **`.env`** in the project root (it is gitignored — never committed)
with **three** entries:

```env
# your PostgreSQL superuser password (set at install)
DATABANK_PASSWORD=your_postgres_password

# pick any passwords for the two pipeline roles (created automatically in step 6)
DATABANK_APP_PASSWORD=choose_a_strong_password
DATABANK_READER_PASSWORD=choose_another_password
```

- `DATABANK_PASSWORD` is the **admin** password (used only to create the DB + roles).
- `DATABANK_APP_PASSWORD` / `DATABANK_READER_PASSWORD` are passwords for the
  least-privilege roles the pipeline creates (`databank_app` writes; `databank_reader`
  is read-only). You choose these values; they don't need to pre-exist.
- A password containing `#` or other symbols is fine (the loader splits on the first `=`).

---

## 4. (Optional) Adjust `config.yaml`

Only needed if your setup is non-standard. The defaults assume a local install:

```yaml
database:
  host: localhost
  port: 5432
  dbname: databank
  user: databank_app
  admin_user: postgres     # change if your superuser has a different name
  sslmode: prefer          # use 'require'/'verify-full' for a remote DB
```

Other knobs you may tune (all optional):
- `region.resolution_deg` — grid spacing (default **0.5°** ≈ 1,109 India cells).
- `time.temporal_step_hours` — **6** keeps 4 rows/day (00/06/12/18 IST); set `1` for hourly.
- `openmeteo.forecast_days` / `airquality.forecast_days` — forecast horizon.

---

## 5. Download reference data (one-time)

Needed for the India grid filter and for `state`/`district`/`pincode` enrichment:

```bash
python -c "import os; os.makedirs('data/reference', exist_ok=True)"
curl -L -o data/reference/gadm41_IND.gpkg https://geodata.ucdavis.edu/gadm/gadm4.1/gpkg/gadm41_IND.gpkg
curl -L -o data/reference/IN.zip          https://download.geonames.org/export/zip/IN.zip
python -c "import zipfile; zipfile.ZipFile('data/reference/IN.zip').extract('IN.txt','data/reference')"
```

You should end up with `data/reference/gadm41_IND.gpkg` (~50 MB) and
`data/reference/IN.txt` (~12 MB).

---

## 6. Set up the database (one-time)

```bash
python main.py db-init      # creates the 'databank' DB + weather/air_quality tables
python main.py db-harden    # creates roles, constraints/FK/ENUM, BRIN indexes, etl_run_log
```

`db-harden` reads the three passwords from `.env` to create `databank_app` and
`databank_reader`, then transfers ownership of the tables to `databank_app`.

---

## 7. Run the pipeline

```bash
python main.py all          # builds grid_master, then loads history + forecast -> Postgres
```

That's the full first-time setup (steps 1–7). It may take a few minutes (the client
paces requests under Open-Meteo's free-tier limits).

---

## Everyday use

After the one-time setup, a single command refreshes everything:

```bash
python main.py all          # master refresh + history + forecast (recommended daily)
```

Or run individual stages:

| Command | What it does |
|---|---|
| `python main.py all` | master + history + forecast → Postgres (recommended) |
| `python main.py run` | history + forecast only (no master refresh) |
| `python main.py run --days 3 --aq-days 3` | cap the forecast horizon (weather ≤16, AQI ≤7) |
| `python main.py master` | rebuild the `grid_master` location dimension |
| `python main.py forecast` | forecast only → `data/forecast/` CSV |
| `python main.py daily` | trailing history only → `data/daily/` CSV |
| `python main.py historical 2025-01-01 2025-03-31` | a date range → `data/historical/` CSV |
| `python main.py db-init` / `db-harden` | (re-runnable, idempotent) DB setup/hardening |

Dates are automatic (anchored on today) — you never pass them except for `historical`.

---

## Verify it worked

Connect to `databank` (DBeaver / pgAdmin / `psql`) as `databank_reader` and run:

```sql
SELECT data_type, count(*) FROM weather GROUP BY data_type;
SELECT count(*) FROM grid_master;                          -- ~1,109 at 0.5°
SELECT * FROM etl_run_log ORDER BY run_id DESC LIMIT 5;    -- run history
-- location join:
SELECT w.cell_id, m.state, m.district, w.valid_time, w.temperature_2m
FROM weather w JOIN grid_master m USING (cell_id)
WHERE m.state = 'Delhi' LIMIT 10;
```

---

## Troubleshooting

| Symptom | Cause & fix |
|---|---|
| `RuntimeError: secret not set — put DATABANK_APP_PASSWORD=...` | `.env` missing an entry — add all three (step 3). |
| `FileNotFoundError: ... gadm41_IND.gpkg` | reference data not downloaded — do step 5. |
| `connection ... failed: Connection refused` | PostgreSQL isn't running — start the service (step 2). |
| `password authentication failed` | `DATABANK_PASSWORD` in `.env` ≠ your real `postgres` password. |
| `... role "databank_app" does not exist` | run `python main.py db-harden` (step 6) before `run`/`all`. |
| `Hourly/Daily API request limit exceeded` then `... SKIPPED` | free-tier quota hit (usually from many runs in one hour). The run **degrades gracefully** — history is saved, the skipped part logs to `etl_run_log`. Just re-run later; one run/day stays well within limits. |
| `... is locked (open in Excel?) ... wrote __locked_retry` | only affects the CSV building-block commands; close the file or ignore. Postgres (`run`/`all`) is unaffected. |

---

## What's where

```
main.py            single entry point (all commands)
config.yaml        all settings           .env  (you create) DB secrets
src/               pipeline code          data/reference/  GADM + GeoNames (you download)
PostgreSQL databank:  weather · air_quality · grid_master · etl_run_log
```

- **`weather` / `air_quality`** — measurements (cell × hour), `data_type` = historical|forecast.
- **`grid_master`** — single source of truth for location (`cell_id → lat/lon, state, district, pincode`).
- **`etl_run_log`** — one row per source/phase per run (observability).
