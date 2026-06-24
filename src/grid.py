"""Generate the India grid the whole pipeline pulls against.

A regular lat/lon mesh over the configured bounding box. With `india_only`
set, cells are filtered by point-in-polygon against GADM India boundaries
(optionally buffered to keep coastal/border cells), so the pipeline fetches
only Indian cells -- no blank state/district/pincode and ~70% fewer API calls.
`cell_id` is assigned on the FULL bbox grid and preserved through filtering,
so it stays a stable identifier regardless of the india_only toggle.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import numpy as np
import pandas as pd


@dataclass(frozen=True)
class GridCell:
    cell_id: int
    lat: float
    lon: float


def _full_grid(lat_min, lat_max, lon_min, lon_max, res) -> pd.DataFrame:
    lats = np.round(np.arange(lat_min, lat_max + 1e-9, res), 4)
    lons = np.round(np.arange(lon_min, lon_max + 1e-9, res), 4)
    mesh_lat, mesh_lon = np.meshgrid(lats, lons, indexing="ij")
    df = pd.DataFrame({"lat": mesh_lat.ravel(), "lon": mesh_lon.ravel()})
    df.insert(0, "cell_id", range(len(df)))
    return df


@lru_cache(maxsize=8)
def _india_cell_ids(lat_min, lat_max, lon_min, lon_max, res, buffer) -> frozenset:
    """cell_ids (on the full bbox grid) whose centre falls inside India (+buffer).
    Memoised so GADM is read once per process."""
    import geopandas as gpd
    from src.geo.enrich import GADM_PATH, GADM_LAYER

    g = _full_grid(lat_min, lat_max, lon_min, lon_max, res)
    india = gpd.read_file(GADM_PATH, layer=GADM_LAYER).geometry.union_all()
    if buffer and buffer > 0:
        india = india.buffer(buffer)
    pts = gpd.GeoSeries(gpd.points_from_xy(g["lon"], g["lat"]), crs="EPSG:4326")
    return frozenset(g["cell_id"][pts.within(india).to_numpy()].tolist())


def build_grid(region: dict) -> pd.DataFrame:
    """DataFrame[cell_id, lat, lon] for the bbox; filtered to India if india_only."""
    res = float(region["resolution_deg"])
    df = _full_grid(region["lat_min"], region["lat_max"],
                    region["lon_min"], region["lon_max"], res)
    if region.get("india_only"):
        keep = _india_cell_ids(
            float(region["lat_min"]), float(region["lat_max"]),
            float(region["lon_min"]), float(region["lon_max"]),
            res, float(region.get("coast_buffer_deg", 0.0)),
        )
        df = df[df["cell_id"].isin(keep)].reset_index(drop=True)
    return df


def grid_summary(df: pd.DataFrame, region: dict) -> str:
    return (
        f"{region['name']} grid @ {region['resolution_deg']} deg: "
        f"{len(df)} cells "
        f"(lat {df.lat.min()}..{df.lat.max()}, lon {df.lon.min()}..{df.lon.max()})"
    )


if __name__ == "__main__":
    from src.config import Config

    cfg = Config.load()
    g = build_grid(cfg.region)
    print(grid_summary(g, cfg.region))
    print(g.head())
