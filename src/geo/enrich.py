"""Reverse-geocode grid coordinates -> state, district, pincode.

  * state, district : exact point-in-polygon against GADM 4.1 India admin level-2.
  * pincode         : NEAREST pincode centroid from the GeoNames India postal
                      dump (no open pincode polygons exist, so nearest-centroid
                      is the standard approach).

Enrichment is done on the ACTUAL coordinates the API returned (src_lat/src_lon
for Open-Meteo; lat/lon for IMD, which are already the true grid coords).

Caveat: at a coarse grid (e.g. 1 deg ~111 km) a single cell spans many pincodes,
so `pincode` is the nearest post-office centroid to the cell point -- indicative,
not a claim that the whole cell shares that pincode. Finer grids make it meaningful.

Reference files (under data/reference/, see requirements.txt for download cmds):
  gadm41_IND.gpkg   -- GADM 4.1 India boundaries
  IN.txt            -- GeoNames postal dump (tab-separated)
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from src.config import REPO_ROOT
from src.lineage import LINEAGE_COLUMNS

GADM_PATH = REPO_ROOT / "data" / "reference" / "gadm41_IND.gpkg"
GADM_LAYER = "ADM_ADM_2"
PINCODE_PATH = REPO_ROOT / "data" / "reference" / "IN.txt"
_PIN_COLS = ["country", "pincode", "place", "state", "state_code", "district",
             "admin2_code", "admin3", "admin3_code", "lat", "lon", "accuracy"]

# Beyond this distance (deg) from the nearest pincode centroid, leave pincode blank
# (e.g. ocean cells). ~0.6 deg ~= 65 km.
_MAX_PINCODE_DIST_DEG = 0.6


class GeoEnricher:
    """Loads GADM polygons + pincode KD-tree ONCE; reuse across all sources."""

    def __init__(self):
        import geopandas as gpd

        self._gpd = gpd
        if not GADM_PATH.exists():
            raise FileNotFoundError(f"GADM India boundaries missing at {GADM_PATH}")
        if not PINCODE_PATH.exists():
            raise FileNotFoundError(f"GeoNames pincode dump missing at {PINCODE_PATH}")

        self._adm = gpd.read_file(GADM_PATH, layer=GADM_LAYER)[["NAME_1", "NAME_2", "geometry"]]
        pins = pd.read_csv(PINCODE_PATH, sep="\t", header=None, names=_PIN_COLS,
                           dtype={"pincode": str}).dropna(subset=["lat", "lon"])
        self._pin_codes = pins["pincode"].to_numpy()
        self._pin_tree = cKDTree(pins[["lat", "lon"]].to_numpy())

    def enrich(self, df: pd.DataFrame, lat_col: str, lon_col: str) -> pd.DataFrame:
        """Return `df` with state, district, pincode merged on (lat_col, lon_col).

        Computed on the UNIQUE coordinate pairs, then merged back -- so it scales
        to row-level hourly data cheaply.
        """
        gpd = self._gpd
        uniq = df[[lat_col, lon_col]].drop_duplicates().reset_index(drop=True)

        # state + district via point-in-polygon
        pts = gpd.GeoDataFrame(
            uniq.copy(),
            geometry=gpd.points_from_xy(uniq[lon_col], uniq[lat_col]),
            crs="EPSG:4326",
        )
        joined = gpd.sjoin(pts, self._adm, how="left", predicate="within")
        joined = joined[~joined.index.duplicated(keep="first")]  # boundary ties
        uniq["state"] = joined["NAME_1"].to_numpy()
        uniq["district"] = joined["NAME_2"].to_numpy()

        # pincode via nearest centroid (guarded by distance + in-India)
        dist, idx = self._pin_tree.query(uniq[[lat_col, lon_col]].to_numpy(), k=1)
        in_india = uniq["state"].notna().to_numpy()
        ok = in_india & (dist <= _MAX_PINCODE_DIST_DEG)
        uniq["pincode"] = np.where(ok, self._pin_codes[idx], None)

        return df.merge(uniq, on=[lat_col, lon_col], how="left")


def enrich_with_geo(enricher: "GeoEnricher", df: pd.DataFrame,
                    lat_col: str, lon_col: str) -> pd.DataFrame:
    """Add state/district/pincode, then keep the lineage columns last."""
    df = enricher.enrich(df, lat_col, lon_col)
    ordered = [c for c in df.columns if c not in LINEAGE_COLUMNS] \
        + [c for c in LINEAGE_COLUMNS if c in df.columns]
    return df[ordered]
