"""Pipeline orchestration — the shared logic behind every entry point.

Sources: Open-Meteo **weather** + Open-Meteo **air quality**. (IMD removed.)

Primary stage:
    run_combined  -- trailing HISTORY + FORECAST for both sources, merged into ONE
                     accumulating CSV per source (data/datasets/{weather,air_quality}.csv),
                     deduped on (cell_id, time, data_type). `data_type` flags each row
                     'historical' (observed/ERA5) vs 'forecast' (predicted).
Building blocks / utilities:
    run_daily     -- trailing-window history only -> data/daily/
    run_forecast  -- forecast only -> data/forecast/
    run_historical-- arbitrary date range, chunked, resumable -> data/historical/
    backfill      -- bulk history -> parquet (resumable manifest)
    build_master  -- grid -> state/district/pincode master dimension
    run_all       -- run_combined + build_master  (the recurring whole pipeline)
"""
from __future__ import annotations

import os
import uuid
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from openmeteo_requests.Client import OpenMeteoRequestsError

from src.config import Config, REPO_ROOT
from src.lineage import RunContext
# NOTE: geo enrichment (state/district/pincode) no longer runs on the data tables.
# Location is the single source of truth in grid_master (see src/master/build_master.py).
from src.openmeteo import fetch as om_fetch
from src.airquality import fetch as aq_fetch

# earliest date each source has data for (historical requests are clipped/skipped)
MIN_DATE = {"openmeteo": "1940-01-01", "airquality": "2022-08-01"}
ALL_SOURCES = ("openmeteo", "airquality")

# the single maintained dataset file per source
DATASET_NAME = {"openmeteo": "weather", "airquality": "air_quality"}
DATASET_KEYS = ["cell_id", "time", "data_type"]   # dedup key for accumulate-mode


# --- helpers ----------------------------------------------------------
def write_csv_safe(df: pd.DataFrame, path: Path) -> Path:
    """Write via temp file + atomic replace. If the target is locked (open in
    Excel), write a `__locked_retry` sidecar and warn instead of crashing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    try:
        os.replace(tmp, path)
        return path
    except PermissionError:
        alt = path.with_name(path.stem + "__locked_retry" + path.suffix)
        os.replace(tmp, alt)
        print(f"  [warn] {path.name} is locked (open in Excel?). Wrote {alt.name} instead.")
        return alt


def month_chunks(start: str, end: str, n: int):
    """Yield (chunk_start, chunk_end) ISO strings spanning [start, end] in n-month steps."""
    cur, end_d = date.fromisoformat(start), date.fromisoformat(end)
    while cur <= end_d:
        y = cur.year + (cur.month - 1 + n) // 12
        m = (cur.month - 1 + n) % 12 + 1
        nxt = date(y, m, 1)
        yield cur.isoformat(), min(nxt - timedelta(days=1), end_d).isoformat()
        cur = nxt


def _trailing_window(cfg: Config) -> tuple[str, str]:
    inc = cfg._raw.get("incremental", {})
    end = date.today() - timedelta(days=int(inc.get("lag_days", 5)))
    start = end - timedelta(days=int(inc.get("trailing_days", 3)) - 1)
    return start.isoformat(), end.isoformat()


def _run_id(cfg: Config, source: str) -> str:
    return (cfg.openmeteo if source == "openmeteo" else cfg.airquality)["pipeline_run_id"]


def _new_contexts(cfg: Config, data_type: str = "historical") -> tuple[str, dict]:
    """One shared insertion identity per run; per-source pipeline_run_id from config."""
    insertion_id = str(uuid.uuid4())
    inserted_at = pd.Timestamp.now(tz="UTC")
    ctx = {s: RunContext.create(_run_id(cfg, s), insertion_id, inserted_at, data_type)
           for s in ALL_SOURCES}
    return insertion_id, ctx


def _fetch_history(source, cfg, cs, ce, ctx):
    return (om_fetch if source == "openmeteo" else aq_fetch).fetch_frame(cfg, cs, ce, ctx)


def _fetch_forecast(source, cfg, ctx, days):
    return (om_fetch if source == "openmeteo" else aq_fetch).forecast_frame(cfg, ctx, days)


def _dataset_path(source: str) -> Path:
    return REPO_ROOT / "data" / "datasets" / f"{DATASET_NAME[source]}.csv"


def _sink(df, source: str, cfg: Config):
    """Persist a fetched frame to the configured target. Returns (destination, total rows)."""
    target = cfg.storage.get("target", "csv")
    if target == "postgres":
        from src.db import loader
        table = cfg.database["weather_table"] if source == "openmeteo" else cfg.database["airquality_table"]
        return table, loader.upsert(df, table, cfg)
    path = _dataset_path(source)
    return path.name, upsert_dataset(df, path)


def db_init(cfg: Config | None = None) -> None:
    """Create the databank database and the weather / air_quality tables."""
    from src.db import schema
    schema.init(cfg or Config.load())


def db_harden(cfg: Config | None = None) -> None:
    """Apply constraints/FK/ENUM, BRIN indexes, least-privilege roles, etl_run_log."""
    from src.db import hardening
    hardening.harden(cfg or Config.load())


def upsert_dataset(new_df: pd.DataFrame, path: Path, keys=DATASET_KEYS) -> int:
    """Append new_df to the maintained dataset and de-duplicate on `keys`, keeping the
    most recently fetched row (latest inserted_at). Returns the total row count."""
    if path.exists():
        old = pd.read_csv(path, parse_dates=["time", "inserted_at"], dtype={"pincode": "string"})
        combined = pd.concat([old, new_df], ignore_index=True)
    else:
        combined = new_df.copy()
    combined = (combined
                .sort_values("inserted_at")
                .drop_duplicates(subset=keys, keep="last")
                .sort_values(["cell_id", "time"])
                .reset_index(drop=True))
    write_csv_safe(combined, path)
    return len(combined)


# --- PRIMARY stage: combined history + forecast -> single dataset per source ----
def run_combined(cfg: Config | None = None, days_weather=None, days_aq=None) -> None:
    """Fetch trailing HISTORY + FORECAST for weather & air quality and merge each into
    its single accumulating dataset file (data/datasets/weather.csv, air_quality.csv).

    Each run: history rows flagged data_type='historical' (ERA5, ~5-day lag),
    forecast rows flagged 'forecast' (today + N ahead). Rows are appended and
    de-duped on (cell_id, time, data_type), so the dataset grows and the ERA5 gap
    fills in over successive runs. Idempotent on re-run.
    """
    from src.db import runlog
    cfg = cfg or Config.load()
    insertion_id = str(uuid.uuid4())
    inserted_at = pd.Timestamp.now(tz="UTC")
    cs, ce = _trailing_window(cfg)
    fdays = {"openmeteo": days_weather, "airquality": days_aq}
    to_pg = cfg.storage.get("target") == "postgres"

    def _record(source, phase, ws, we, rows, status, error=None):
        if to_pg:
            runlog.log(cfg, insertion_id=insertion_id, command="run", source=source,
                       phase=phase, window_start=ws, window_end=we,
                       rows_written=rows, status=status, error=error)

    print("=" * 72)
    print(f"COMBINED RUN  history={cs}..{ce}  +forecast  insertion_id={insertion_id}")
    print("=" * 72)

    # Phase 1: HISTORY (cheapest, most important) -> persist each source immediately,
    # so a later forecast failure (e.g. rate limit) never loses this work.
    for source in ALL_SOURCES:
        ctx = RunContext.create(_run_id(cfg, source), insertion_id, inserted_at, "historical")
        try:
            hist = _fetch_history(source, cfg, cs, ce, ctx)
        except OpenMeteoRequestsError as exc:
            _record(source, "historical", cs, ce, 0, "skipped", str(exc)[:300])
            print(f"[{source}] historical SKIPPED ({exc}). Re-run when quota resets.")
            continue
        dest, total = _sink(hist, source, cfg)
        _record(source, "historical", cs, ce, len(hist), "success")
        print(f"[{source}] +{len(hist):,} historical -> {dest} (now {total:,} rows)")

    # Phase 2: FORECAST -> persist; tolerate a quota/rate-limit failure gracefully.
    fc_start = date.today().isoformat()
    for source in ALL_SOURCES:
        ctx = RunContext.create(_run_id(cfg, source), insertion_id, inserted_at, "forecast")
        try:
            fc = _fetch_forecast(source, cfg, ctx, fdays[source])
        except OpenMeteoRequestsError as exc:
            _record(source, "forecast", fc_start, None, 0, "skipped", str(exc)[:300])
            print(f"[{source}] forecast SKIPPED ({exc}). History is already saved; "
                  f"re-run when quota resets to add forecast rows.")
            continue
        dest, total = _sink(fc, source, cfg)
        _record(source, "forecast", fc_start, None, len(fc), "success")
        print(f"[{source}] +{len(fc):,} forecast -> {dest} (now {total:,} rows)")
    print("\nDONE.")


# --- building-block stages -------------------------------------------
def run_daily(cfg: Config | None = None) -> None:
    """Trailing-window HISTORY only -> data/daily/<source>/."""
    cfg = cfg or Config.load()
    cs, ce = _trailing_window(cfg)
    insertion_id, ctx = _new_contexts(cfg, data_type="historical")
    print(f"DAILY RUN  window={cs}..{ce}  insertion_id={insertion_id}")
    for source in ALL_SOURCES:
        df = _fetch_history(source, cfg, cs, ce, ctx[source])
        out = write_csv_safe(df, cfg.path("daily_dir", source, f"{source}_{cs}_{ce}.csv"))
        print(f"[{source}] {len(df):,} rows -> {out}")
    print("\nDONE.")


def run_forecast(cfg: Config | None = None, days_weather=None, days_aq=None) -> None:
    """FORECAST only (weather up to 16 d, air quality up to 7 d) -> data/forecast/."""
    cfg = cfg or Config.load()
    insertion_id, ctx = _new_contexts(cfg, data_type="forecast")
    run_date = date.today().isoformat()
    print(f"FORECAST RUN  issued={run_date}  insertion_id={insertion_id}")
    days = {"openmeteo": days_weather, "airquality": days_aq}
    for source in ALL_SOURCES:
        df = _fetch_forecast(source, cfg, ctx[source], days[source])
        out = write_csv_safe(df, REPO_ROOT / "data" / "forecast" / source / f"{source}_forecast_{run_date}.csv")
        print(f"[{source}] {len(df):,} rows -> {out}")
    print("\nDONE.")


def run_historical(
    cfg: Config | None = None,
    start_date: str = "2025-01-01",
    end_date: str = "2025-03-31",
    sources=ALL_SOURCES,
    chunk_months: int = 1,
    skip_existing: bool = True,
) -> None:
    """Pull an arbitrary date range, chunked by month, resumable -> data/historical/."""
    cfg = cfg or Config.load()
    insertion_id, ctx = _new_contexts(cfg, data_type="historical")
    chunks = list(month_chunks(start_date, end_date, chunk_months))

    print(f"HISTORICAL PULL  {start_date}..{end_date}  sources={list(sources)}  "
          f"{len(chunks)} chunk(s) x {chunk_months}mo  insertion_id={insertion_id}")

    for source in sources:
        out_root = REPO_ROOT / "data" / "historical" / source
        for cs, ce in chunks:
            if ce < MIN_DATE[source]:
                print(f"[{source}] {cs}..{ce}: before earliest data ({MIN_DATE[source]}) -> skip")
                continue
            cs_eff = max(cs, MIN_DATE[source])
            out = out_root / f"{source}_{cs_eff}_{ce}.csv"
            if skip_existing and out.exists():
                print(f"[{source}] {cs_eff}..{ce}: exists -> skip")
                continue
            df = _fetch_history(source, cfg, cs_eff, ce, ctx[source])
            write_csv_safe(df, out)
            print(f"[{source}] {cs_eff}..{ce}: {len(df):,} rows -> {out}")
    print("\nDONE.")


def backfill(cfg: Config | None = None, source: str = "openmeteo") -> None:
    """Bulk historical weather -> parquet (resumable). Uses the config.time window."""
    cfg = cfg or Config.load()
    if source == "openmeteo":
        om_fetch.run(cfg)
    else:
        raise ValueError(f"backfill supports only 'openmeteo' (got {source})")


def build_master(cfg: Config | None = None) -> None:
    from src.master import build_master as _bm
    _bm.run(cfg or Config.load())


def run_all(cfg: Config | None = None) -> None:
    """The recurring whole pipeline: combined history+forecast datasets, then master.

    NOTE on free-tier quota: history + forecast in one run can approach Open-Meteo's
    hourly limit (5,000 weighted calls/hr). Lower `forecast_days` or use a paid api_key
    if you hit it. The cost-aware client paces under the per-minute limit automatically.
    """
    cfg = cfg or Config.load()
    print("\n########## [1/2] MASTER (location dimension) ##########")
    build_master(cfg)        # populate grid_master first so the FK is satisfied
    print("\n########## [2/2] COMBINED (history + forecast) ##########")
    run_combined(cfg)
