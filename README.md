# India Weather & Air-Quality Data Pipeline

Fetches **weather** and **air-quality** data for **India only** from Open-Meteo
(historical **and** forecast) and loads it into a **PostgreSQL** database
(`databank`), with a single location master mapping every grid cell to its
state / district / pincode. Built to train and serve a forecasting model.

## Sources

| Source | Endpoint | Variables | Cadence | Native res |
|---|---|---|---|---|
| Open-Meteo **weather** (ERA5 + forecast) | `archive-api` / `api` `/forecast` | 46 (temp, humidity, wind, pressure, radiation, soil, …) | hourly | 0.25° |
| Open-Meteo **air quality** (CAMS) | `air-quality-api` | 17 (PM2.5/PM10, gases, dust, US/EU AQI) | hourly | 0.4° |

> IMD was evaluated and removed — Open-Meteo gives full hourly coverage for both.

## Storage (PostgreSQL `databank`)

| Table | Grain | Columns | Holds |
|---|---|---|---|
| `weather` | cell × hour | 54 | `cell_id`, `src_lat/src_lon`, `valid_time`, 46 vars, lineage |
| `air_quality` | cell × hour | 25 | `cell_id`, `src_lat/src_lon`, `valid_time`, 17 vars, lineage |
| `grid_master` | cell | 8 | **single source of truth for location** — `cell_id → latitude, longitude, state, district, pincode` |

Data tables carry **no** location columns — join to `grid_master` on `cell_id`.
Each row is flagged `data_type` (`historical` | `forecast`) so observed and predicted
rows never clash. Rows accumulate via `ON CONFLICT (cell_id, valid_time, data_type)`.

## Quick start

> 📖 New machine? Follow the full step-by-step guide in **[SETUP.md](SETUP.md)** (prereqs,
> `.env`, reference data, DB setup, troubleshooting). The condensed version:

```bash
pip install -r requirements.txt

# one-time reference data for grid_master enrichment:
curl -L -o data/reference/gadm41_IND.gpkg https://geodata.ucdavis.edu/gadm/gadm4.1/gpkg/gadm41_IND.gpkg
curl -L -o data/reference/IN.zip https://download.geonames.org/export/zip/IN.zip   # unzip IN.txt into data/reference/

# DB password (gitignored):  echo 'DATABANK_PASSWORD=...' > .env
python main.py db-init     # create databank + weather/air_quality tables
python main.py run         # fetch history + forecast -> Postgres
python main.py master      # build grid_master (location mapping)
```

## Single entry point — `main.py`

| Command | What it does |
|---|---|
| `python main.py run` | **Primary.** History + forecast for both sources → `databank` tables |
| `python main.py master` | (Re)build `grid_master` — the location mapping |
| `python main.py db-init` | Create the `databank` DB + tables |
| `python main.py daily` | History-only trailing window → `data/daily/` CSV |
| `python main.py forecast` | Forecast only → `data/forecast/` CSV |
| `python main.py historical 2025-01-01 2025-03-31` | A date range (chunked, resumable) → `data/historical/` CSV |
| `python main.py backfill` | Bulk weather history → parquet |
| `python main.py all` | `run` + `master` |

`run`/`forecast` accept `--days N` (weather ≤16) and `--aq-days N` (AQI ≤7). Dates
are automatic (anchored on today).

## Configuration — `config.yaml`

All knobs: India bbox + `india_only` + `resolution_deg` (currently **0.5°** ≈ 1,109
cells), the variable lists, `forecast_days`, throttle limits, `storage.target`
(`postgres`|`csv`), the `database:` block, and per-source `pipeline_run_id`.
The DB password lives only in `.env`.

## How it works

`main.py` → `src/pipeline.py` → per-source `fetch.py` (`fetch_frame` history /
`forecast_frame` forecast) over the India grid (`src/grid.py`, point-in-polygon
filtered) → upsert into Postgres (`src/db/`). A cost-aware client
(`src/openmeteo/client.py`) paces under Open-Meteo's free-tier limits. Location is
enriched once into `grid_master` (`src/master/build_master.py` via `src/geo/enrich.py`).
See **[DESIGN.md](DESIGN.md)** for architecture and **[DATA_DICTIONARY.md](DATA_DICTIONARY.md)**
for every column.
