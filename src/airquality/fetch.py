"""Open-Meteo Air Quality fetch over the India grid (AQI + pollutants).

Same machinery as the weather pull -- it reuses the throttled client, the
cost estimate, and the flatbuffer parser from `src.openmeteo` -- but points at
the air-quality endpoint with its own variable set and pipeline_run_id.

For India the data comes from CAMS GLOBAL (0.4 deg, ~45 km); the global archive
starts Aug 2022. Open-Meteo offers no CPCB/India AQI, so `us_aqi` is the headline
index and raw pollutant concentrations are included for downstream CPCB derivation.

Output (daily): data/daily/airquality/airquality_<start>_<end>.csv
Schema: [cell_id, lat, lon, time, <aq variables...>, <lineage...>]
"""
from __future__ import annotations

from datetime import date

import pandas as pd
from tqdm import tqdm

from src.config import Config
from src.grid import build_grid, grid_summary
from src.lineage import RunContext
from src.openmeteo.client import ThrottledOpenMeteo, estimate_cost
from src.openmeteo.fetch import _parse_responses


def fetch_frame(
    cfg: Config,
    start_date: str,
    end_date: str,
    run_ctx: RunContext,
) -> pd.DataFrame:
    """Fetch [start_date, end_date] air quality over the grid into ONE stamped frame."""
    aq_cfg = cfg.airquality
    variables = aq_cfg["hourly_variables"]
    grid = build_grid(cfg.region)

    batch_size = int(aq_cfg["cells_per_request"])
    batches = [grid.iloc[i : i + batch_size] for i in range(0, len(grid), batch_size)]
    n_days = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1

    print(grid_summary(grid, cfg.region))
    print(f"[air-quality] window={start_date}..{end_date} ({n_days}d)  "
          f"variables={len(variables)}  batches={len(batches)}")

    client = ThrottledOpenMeteo(aq_cfg)  # archive_url -> air-quality endpoint
    base_params = {
        "start_date": start_date,
        "end_date": end_date,
        "hourly": variables,
        "timezone": cfg.time["timezone"],
    }

    frames = []
    for batch in tqdm(batches, desc="air-quality daily"):
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
    """Fetch the air-quality FORECAST (next N days, max 7) over the grid.

    The air-quality host serves both archive and forecast, so we reuse the same
    URL but pass `forecast_days` instead of start/end dates.
    """
    aq_cfg = cfg.airquality
    variables = aq_cfg["hourly_variables"]
    fdays = min(int(forecast_days or aq_cfg.get("forecast_days", 7)), 7)
    grid = build_grid(cfg.region)

    batch_size = int(aq_cfg["cells_per_request"])
    batches = [grid.iloc[i : i + batch_size] for i in range(0, len(grid), batch_size)]

    print(grid_summary(grid, cfg.region))
    print(f"[air-quality FORECAST] +{fdays}d  variables={len(variables)}  batches={len(batches)}")

    client = ThrottledOpenMeteo(aq_cfg)  # same host serves forecast
    base_params = {"forecast_days": fdays, "hourly": variables, "timezone": cfg.time["timezone"]}

    frames = []
    for batch in tqdm(batches, desc="air-quality forecast"):
        cost = estimate_cost(len(batch), len(variables), fdays)
        responses = client.fetch(
            batch["lat"].tolist(), batch["lon"].tolist(), base_params, cost
        )
        frames.append(_parse_responses(responses, batch, variables))

    df = pd.concat(frames, ignore_index=True)
    return run_ctx.stamp(df)


if __name__ == "__main__":
    cfg = Config.load()
    ctx = RunContext.create(cfg.airquality["pipeline_run_id"])
    df = fetch_frame(cfg, cfg.time["start_date"], cfg.time["end_date"], ctx)
    print(df.head())
