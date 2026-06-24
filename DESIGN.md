# India Weather & Air-Quality Pipeline — Design

**Status:** built & running — fetches to **PostgreSQL (`databank`)**
**Scope:** weather + air quality, **India only**, hourly, to train/serve a forecasting model
**Last updated:** 2026-06-18

Architecture/design reference. For the column-by-column definitions (meanings,
values, units) see **[DATA_DICTIONARY.md](DATA_DICTIONARY.md)**; for how to run it see
**[README.md](README.md)**.

---

## Table of contents
1. [Overview & sources](#1-overview--sources)
2. [Architecture](#2-architecture)
3. [Storage model (PostgreSQL `databank`)](#3-storage-model-postgresql-databank)
4. [India grid & resolution/cost trade-off](#4-india-grid--resolutioncost-trade-off)
5. [Coordinates & lineage](#5-coordinates--lineage)
6. [Entry point, commands & process flow](#6-entry-point-commands--process-flow)
7. [Cost model & rate limits](#7-cost-model--rate-limits)
8. [Forecast](#8-forecast)
9. [Risks & caveats](#9-risks--caveats)
10. [Glossary](#10-glossary)

---

## 1. Overview & sources

Two Open-Meteo APIs, full hourly coverage over India:

| Source | Role | Data | Cadence | Native resolution |
|---|---|---|---|---|
| **Open-Meteo weather** | primary | ERA5 reanalysis (history) + forecast; 46 surface variables | hourly | 0.25° (~25 km) |
| **Open-Meteo air quality** | AQI / pollutants | CAMS global; 17 variables incl. US/EU AQI | hourly | 0.4° (~45 km) |

**IMD was evaluated and removed** (only 3 gridded variables, no forecast, flaky
server). No CPCB/India AQI exists in Open-Meteo — `us_aqi` is the headline index;
raw pollutants are kept so a CPCB AQI can be derived downstream.

---

## 2. Architecture

```
  COMMAND          main.py              (argparse single entry point)
     |
  ORCHESTRATION    src/pipeline.py      (run_combined / daily / forecast / historical /
     |                                   backfill / build_master / db_init / run_all)
     |
  SOURCE FETCHERS  src/openmeteo/fetch.py   src/airquality/fetch.py
     |                 fetch_frame()  -> history (archive endpoint)
     |                 forecast_frame() -> forecast (forecast endpoint / forecast_days)
     |
  SHARED ENGINE    grid.py (India point-in-polygon)  ·  openmeteo/client.py (cost-aware)
     |             lineage.py (data_type + audit)
     |
  SINK / STORE     src/db/  (connection · schema · loader)  -> PostgreSQL databank
  LOCATION         src/master/build_master.py + src/geo/enrich.py -> grid_master
     |
  CONFIG           config.yaml + .env   (grid, vars, forecast, throttle, DB, secret)
```

File map:

```
main.py                      single entry point (subcommands)
config.yaml                  all settings;  .env  holds the DB password (gitignored)
src/
  config.py                  loads config.yaml
  grid.py                    bbox -> grid, filtered to India (point-in-polygon, memoised)
  lineage.py                 RunContext: data_type + pipeline_run_id + insertion_id + inserted_at
  pipeline.py                stage orchestration (shared logic; no geo on data tables)
  openmeteo/client.py        throttled, cost-aware API client (shared by AQ)
  openmeteo/fetch.py         weather fetch_frame (history) + forecast_frame
  airquality/fetch.py        air-quality fetch_frame + forecast_frame
  geo/enrich.py              GeoEnricher: GADM state/district + GeoNames pincode (used by master)
  master/build_master.py     builds grid_master (the single location source of truth)
  db/connection.py           psycopg2 connection (password from .env)
  db/schema.py               DDL for weather / air_quality (generated from var lists)
  db/loader.py               upsert via execute_values (ON CONFLICT)
data/reference/              gadm41_IND.gpkg, IN.txt  (enrichment inputs, one-time download)
data/{daily,forecast,historical}/   CSV outputs of the lower-level commands (optional)
data/master/grid_master.csv  CSV copy of the location master
```

Principle: **`src/` = logic, `main.py` = entry, `config.yaml`+`.env` = settings,
PostgreSQL = the store.**

---

## 3. Storage model (PostgreSQL `databank`)

Three tables. The two fact tables hold measurements only; **all location lives once
in `grid_master`** (normalized — a single source of truth, joined on `cell_id`).

| Table | Grain | Cols | Contents |
|---|---|---|---|
| `weather` | cell × hour | 54 | `cell_id`, `src_lat/src_lon`, `valid_time`, 46 vars, lineage |
| `air_quality` | cell × hour | 25 | `cell_id`, `src_lat/src_lon`, `valid_time`, 17 vars, lineage |
| `grid_master` | cell | 8 | `cell_id → latitude, longitude, resolution_deg, in_india, state, district, pincode` |

```sql
-- location dimension (single source of truth)
CREATE TABLE grid_master (
  cell_id INTEGER PRIMARY KEY, latitude REAL, longitude REAL, resolution_deg REAL,
  in_india BOOLEAN, state TEXT, district TEXT, pincode TEXT
);

-- fact table (air_quality is the same shape with its 17 variables)
CREATE TABLE weather (
  cell_id         INTEGER NOT NULL,
  src_lat REAL, src_lon REAL,              -- actual source cell (provenance)
  valid_time      TIMESTAMPTZ NOT NULL,    -- the weather hour (UTC)
  temperature_2m  REAL, /* … all 46, REAL; weather_code/is_day SMALLINT … */
  data_type       TEXT NOT NULL,           -- 'historical' | 'forecast'
  pipeline_run_id TEXT, insertion_id UUID, inserted_at TIMESTAMPTZ,
  PRIMARY KEY (cell_id, valid_time, data_type)
);
```

- **Accumulate via upsert:** loads use `INSERT … ON CONFLICT (cell_id, valid_time,
  data_type) DO UPDATE` — re-runs are idempotent and keep the latest `inserted_at`.
- **Join for location:** `weather JOIN grid_master USING (cell_id)`.
- Created by `python main.py db-init`; the DB password is read from `.env`
  (`DATABANK_PASSWORD`), never stored in config.
- CSV remains available as an alternate sink (`storage.target: csv`) for the
  lower-level commands; `run` targets `postgres`.

---

## 4. India grid & resolution/cost trade-off

`src/grid.py` lays a regular lat/lon mesh over the India bbox, then (with
`india_only`) keeps only cells **inside India** by point-in-polygon vs GADM
(memoised). `cell_id` is assigned on the full bbox grid and **preserved** through
filtering, so it is a stable identifier.

Current resolution **0.5° → ~1,109 India cells**. Cost scales ~linearly with cell
count; halving the spacing ~quadruples cells:

| Resolution | India cells | Relative cost | Notes |
|---|---|---|---|
| 1.0° | ~281 | 1× | coarse |
| **0.5°** (current) | ~1,109 | ~4× | balanced, free-tier friendly |
| 0.25° (ERA5 native) | ~4,461 | ~16× | finest useful for weather; heavier |

Going finer than the native resolution (0.25° weather, 0.4° AQI) adds no
information — it just resamples the same source cells.

---

## 5. Coordinates & lineage

**Two coordinate ideas:**
- `grid_master.latitude/longitude` — the **canonical grid point** we request; the
  same for both sources → the shared location anchor.
- `src_lat/src_lon` (on each data row) — the **actual source cell** the API returned
  (full precision). Weather (ERA5) snaps up to ~16 km off the grid point; AQI (CAMS)
  returns the grid point. So the same `cell_id` has different `src` per source —
  **join across sources on `cell_id`, not coordinates.**

**Lineage** (every data row, `src/lineage.py`):

| Column | Meaning |
|---|---|
| `data_type` | `historical` (observed/ERA5/CAMS) vs `forecast` (predicted) — keeps them unambiguous in one table |
| `pipeline_run_id` | fixed per source from config |
| `insertion_id` | random UUID, one per run |
| `inserted_at` | UTC fetch/write time (distinct from `valid_time` = when the weather happened) |

Location enrichment (state/district/pincode) runs **only** when building
`grid_master`, on the canonical grid point — so there is exactly one location label
per cell, shared by both sources.

---

## 6. Entry point, commands & process flow

Single entry point `main.py` (full table in the README). Primary flow,
`python main.py run`:

1. **Load** config; mint one `insertion_id` + `inserted_at` for the run.
2. **Window** — trailing history from `incremental` (ends `lag_days`=5 ago, spans
   `trailing_days`=3); forecast anchored on today.
3. **Phase 1 — history** (cheap, most important): for each source, `fetch_frame` over
   the India grid → **upsert into Postgres** (persisted immediately).
4. **Phase 2 — forecast**: for each source, `forecast_frame` → upsert. A quota /
   rate-limit failure here is **caught and skipped** — history is already saved.
5. `python main.py master` (re)builds `grid_master`. `all` = `run` + `master`.

The data path does **no** geo enrichment — location is materialised once in
`grid_master`.

---

## 7. Cost model & rate limits

Open-Meteo bills **weighted calls**, not HTTP requests:
`cost ≈ cells × (variables / 14) × (days / 14)`.

Free-tier limits, **per API subdomain** (archive, forecast, air-quality each have
their own): **600/min, 5,000/hour, 10,000/day, 300,000/month.**

The client (`src/openmeteo/client.py`):
- estimates each batch's cost and **paces** against a rolling 60-second budget;
- on a **per-minute** limit, waits ~65 s and retries;
- on an **hourly/daily** limit, **fails fast** so the run skips gracefully (it doesn't
  stall for minutes);
- caches responses on disk so re-runs are free and idempotent.

At 0.5° a `run` is ~1,000 (weather) + ~400 (AQI) weighted calls — within limits.
Long backfills / finer grids → use a paid `api_key` or chunk over days
(`historical` is resumable).

---

## 8. Forecast

| Source | Horizon | Endpoint |
|---|---|---|
| Weather | up to **16 days** | `api.open-meteo.com/v1/forecast` (`forecast_days`) |
| Air quality | up to **7 days** | `air-quality-api.open-meteo.com/v1/air-quality` (`forecast_days`) |

Forecast rows are flagged `data_type=forecast` and upserted into the same fact table
as history. Default horizon `forecast_days: 7` (weather) in config; override with
`--days` (≤16) / `--aq-days` (≤7). Forecast includes today, so one `run` gives
today + the days ahead with no date configuration.

---

## 9. Risks & caveats

- **Free-tier quota:** heavy testing / fine grids / long forecasts hit the hourly or
  daily caps (each subdomain separately). The client fails fast and the run skips
  forecast gracefully — **history is always persisted first**, so nothing is lost;
  re-run after the reset to add forecast rows.
- **ERA5 ~5-day lag:** today's *finalized* observation isn't available; the forecast
  covers today onward, and ERA5 backfills the gap on later runs.
- **Weather vs AQI per cell:** the same `cell_id` sits at slightly different `src`
  points in the two tables (different models); a single shared location is provided
  by `grid_master`. Join on `cell_id`.
- **Border cells:** a few cells whose grid point is just outside the India polygon get
  blank state/district/pincode in `grid_master`.

---

## 10. Glossary

**Sources & data**
- **Open-Meteo** — free open-source weather/air-quality API (reanalysis + forecast).
- **ERA5** — ECMWF global atmospheric **reanalysis** (0.25°); behind the weather history.
- **CAMS** — Copernicus Atmosphere Monitoring Service; behind the air-quality data.
- **GADM** — global administrative boundaries; v4.1 India gives state & district.
- **GeoNames** — open geo database; its India postal dump (`IN.txt`) gives pincode centroids.
- **AQI / us_aqi / european_aqi** — Air Quality Index (US-EPA / European scales). **CPCB** = India's official AQI (derivable from raw pollutants).

**Pipeline**
- **cell_id** — stable grid-cell id; the join key (to `grid_master` and across sources).
- **src_lat/src_lon** — the actual source cell coordinates (provenance), full precision.
- **data_type** — `historical` vs `forecast` flag on every row.
- **pipeline_run_id / insertion_id / inserted_at** — fixed per-API id / random per-run id / fetch time.
- **weighted call** — Open-Meteo's billing unit (scales with variables × days).
- **point-in-polygon** — testing which admin polygon a coordinate falls inside.
- **single source of truth** — location stored once, in `grid_master`.

**Database**
- **PostgreSQL `databank`** — the store; tables `weather`, `air_quality`, `grid_master`.
- **upsert** — `INSERT … ON CONFLICT (cell_id, valid_time, data_type) DO UPDATE`.
- **fact / dimension** — measurement tables (`weather`/`air_quality`) vs the location
  dimension (`grid_master`).
- **TIMESTAMPTZ** — Postgres timestamp-with-time-zone (stored UTC).
