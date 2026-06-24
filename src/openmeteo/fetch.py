"""Chunked, resumable Open-Meteo archive fetch over the India grid.

Strategy
--------
* Split the grid into batches of `cells_per_request` cells.
* Each batch -> one HTTP call (multi-location) -> one parquet file.
* A JSON manifest records completed batches so an interrupted run
  (or one that hit the daily rate limit) resumes exactly where it stopped.

Output: data/raw/openmeteo/<region>/<start>_<end>/batch_<n>.parquet
        long/tidy schema: [cell_id, lat, lon, time, <variable...>]
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from datetime import date

from src.config import Config
from src.grid import build_grid, grid_summary
from src.lineage import RunContext
from src.openmeteo.client import ThrottledOpenMeteo, estimate_cost


def _window_tag(cfg: Config) -> str:
    return f"{cfg.time['start_date']}_{cfg.time['end_date']}"


def _out_dir(cfg: Config) -> Path:
    return cfg.path("raw_dir", "openmeteo", cfg.region["name"], _window_tag(cfg))


def _manifest_path(out_dir: Path) -> Path:
    return out_dir / "_manifest.json"


def _load_manifest(out_dir: Path) -> dict:
    mp = _manifest_path(out_dir)
    if mp.exists():
        return json.loads(mp.read_text())
    return {"done_batches": []}


def _save_manifest(out_dir: Path, manifest: dict):
    _manifest_path(out_dir).write_text(json.dumps(manifest, indent=2))


def _parse_responses(responses, batch_df: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    """Flatten flatbuffer responses (one per cell) into a tidy DataFrame.

    Columns:
      cell_id                 -- stable grid-cell id (the cross-source join key)
      src_lat, src_lon        -- the ACTUAL source-cell coords the API returned,
                                 full precision (NOT rounded). The requested grid
                                 lat/lon are intentionally NOT stored.
    """
    frames = []
    # responses come back in the same order as the coords we sent
    for resp, (_, cell) in zip(responses, batch_df.iterrows()):
        hourly = resp.Hourly()
        times = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
        data = {"time": times}
        for i, var in enumerate(variables):
            data[var] = hourly.Variables(i).ValuesAsNumpy()
        df = pd.DataFrame(data)
        df.insert(0, "cell_id", int(cell["cell_id"]))
        # store ONLY the actual source coordinates (full precision, no rounding)
        df.insert(1, "src_lat", resp.Latitude())
        df.insert(2, "src_lon", resp.Longitude())
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def fetch_frame(
    cfg: Config,
    start_date: str,
    end_date: str,
    run_ctx: RunContext,
) -> pd.DataFrame:
    """Fetch [start_date, end_date] over the whole grid into ONE stamped frame.

    Used by the daily incremental pull (small windows). No per-batch files /
    manifest -- returns the combined DataFrame with lineage columns appended.
    """
    om_cfg = cfg.openmeteo
    variables = om_cfg["hourly_variables"]
    grid = build_grid(cfg.region)

    batch_size = int(om_cfg["cells_per_request"])
    batches = [grid.iloc[i : i + batch_size] for i in range(0, len(grid), batch_size)]
    n_days = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1

    print(grid_summary(grid, cfg.region))
    print(f"[open-meteo] window={start_date}..{end_date} ({n_days}d)  "
          f"variables={len(variables)}  batches={len(batches)}")

    client = ThrottledOpenMeteo(om_cfg)
    base_params = {
        "start_date": start_date,
        "end_date": end_date,
        "hourly": variables,
        "timezone": cfg.time["timezone"],
    }

    frames = []
    for batch in tqdm(batches, desc="open-meteo daily"):
        cost = estimate_cost(len(batch), len(variables), n_days)
        responses = client.fetch(
            batch["lat"].tolist(), batch["lon"].tolist(), base_params, cost
        )
        frames.append(_parse_responses(responses, batch, variables))

    df = pd.concat(frames, ignore_index=True)
    return run_ctx.stamp(df)


def forecast_frame(
    cfg: Config,
    run_ctx: RunContext,
    forecast_days: int | None = None,
) -> pd.DataFrame:
    """Fetch the weather FORECAST (next N days) over the grid into ONE stamped frame.

    Uses the forecast host (api.open-meteo.com/v1/forecast) with `forecast_days`
    instead of start/end dates. Same variables, parser, and lineage as the archive.
    """
    om_cfg = cfg.openmeteo
    variables = om_cfg["hourly_variables"]
    fdays = int(forecast_days or om_cfg.get("forecast_days", 16))
    url = om_cfg.get("forecast_url", "https://api.open-meteo.com/v1/forecast")
    grid = build_grid(cfg.region)

    batch_size = int(om_cfg["cells_per_request"])
    batches = [grid.iloc[i : i + batch_size] for i in range(0, len(grid), batch_size)]

    print(grid_summary(grid, cfg.region))
    print(f"[open-meteo FORECAST] +{fdays}d  variables={len(variables)}  batches={len(batches)}")

    # point the throttled client at the forecast host
    client = ThrottledOpenMeteo({**om_cfg, "archive_url": url})
    base_params = {"forecast_days": fdays, "hourly": variables, "timezone": cfg.time["timezone"]}

    frames = []
    for batch in tqdm(batches, desc="open-meteo forecast"):
        cost = estimate_cost(len(batch), len(variables), fdays)
        responses = client.fetch(
            batch["lat"].tolist(), batch["lon"].tolist(), base_params, cost
        )
        frames.append(_parse_responses(responses, batch, variables))

    df = pd.concat(frames, ignore_index=True)
    return run_ctx.stamp(df)


def run(cfg: Config | None = None) -> Path:
    cfg = cfg or Config.load()
    om_cfg = cfg.openmeteo
    variables = om_cfg["hourly_variables"]

    grid = build_grid(cfg.region)
    out_dir = _out_dir(cfg)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = _load_manifest(out_dir)
    done = set(manifest["done_batches"])

    batch_size = int(om_cfg["cells_per_request"])
    batches = [grid.iloc[i : i + batch_size] for i in range(0, len(grid), batch_size)]

    n_days = (date.fromisoformat(cfg.time["end_date"])
              - date.fromisoformat(cfg.time["start_date"])).days + 1
    total_cost = estimate_cost(len(grid), len(variables), n_days)

    print(grid_summary(grid, cfg.region))
    print(f"window={_window_tag(cfg)} ({n_days}d)  variables={len(variables)}  "
          f"batches={len(batches)} ({batch_size} cells each)  already_done={len(done)}")
    print(f"estimated cost ~= {total_cost:,.0f} weighted calls "
          f"(free tier: 10,000/day, 300,000/month)")

    client = ThrottledOpenMeteo(om_cfg)
    base_params = {
        "start_date": cfg.time["start_date"],
        "end_date": cfg.time["end_date"],
        "hourly": variables,
        "timezone": cfg.time["timezone"],
    }

    for bi, batch in enumerate(tqdm(batches, desc="open-meteo batches")):
        if bi in done:
            continue
        batch_cost = estimate_cost(len(batch), len(variables), n_days)
        responses = client.fetch(
            batch["lat"].tolist(), batch["lon"].tolist(), base_params, batch_cost
        )
        df = _parse_responses(responses, batch, variables)
        df.to_parquet(out_dir / f"batch_{bi:04d}.parquet", index=False)
        manifest["done_batches"].append(bi)
        _save_manifest(out_dir, manifest)

    print(f"DONE -> {out_dir}")
    return out_dir


if __name__ == "__main__":
    run()
