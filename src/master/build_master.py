"""Build the single source of truth for location: `grid_master`.

ONE master keyed by `cell_id`, mapping each cell to its **canonical grid point**
(`latitude`, `longitude`) and location (`state`, `district`, `pincode`). The grid
point is the same for both weather and air quality (it's what we request), so this
is a single canonical mapping shared across sources.

Data tables (`weather`, `air_quality`) hold only `cell_id` + `src_lat/src_lon`
(provenance) + measurements; they JOIN to grid_master on `cell_id` for location.

Outputs: data/master/grid_master.csv + DB table databank.grid_master.
"""
from __future__ import annotations

import pandas as pd
from psycopg2.extras import execute_values

from src.config import Config, REPO_ROOT
from src.geo.enrich import GeoEnricher
from src.grid import build_grid
from src.db.connection import connect

_COLS = ["cell_id", "latitude", "longitude", "resolution_deg",
         "in_india", "state", "district", "pincode"]


def _write_db_master(conn, df: pd.DataFrame) -> None:
    # Upsert (not drop+create) so the FK weather/air_quality -> grid_master survives.
    sub = df[_COLS].astype(object).where(df[_COLS].notna(), None)
    rows = list(sub.itertuples(index=False, name=None))
    cols = ", ".join(f'"{c}"' for c in _COLS)
    updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in _COLS if c != "cell_id")
    with conn.cursor() as cur:
        cur.execute('CREATE TABLE IF NOT EXISTS "grid_master" ('
                    'cell_id INTEGER PRIMARY KEY, latitude REAL, longitude REAL, '
                    'resolution_deg REAL, in_india BOOLEAN, '
                    'state TEXT, district TEXT, pincode TEXT)')
        execute_values(cur, f'INSERT INTO "grid_master" ({cols}) VALUES %s '
                            f'ON CONFLICT (cell_id) DO UPDATE SET {updates}', rows)
    conn.commit()


def run(cfg: Config | None = None):
    cfg = cfg or Config.load()

    g = build_grid(cfg.region).rename(columns={"lat": "latitude", "lon": "longitude"})
    m = GeoEnricher().enrich(g, "latitude", "longitude")   # enrich on canonical grid point
    m["in_india"] = m["state"].notna()
    m["resolution_deg"] = float(cfg.region["resolution_deg"])
    m = m[_COLS]

    out_dir = REPO_ROOT / "data" / "master"
    out_dir.mkdir(parents=True, exist_ok=True)
    m.to_csv(out_dir / "grid_master.csv", index=False)

    conn = connect(cfg, dbname=cfg.database["dbname"])
    _write_db_master(conn, m)
    conn.close()

    print(f"[master] grid_master: {len(m)} cells (single source of truth) "
          f"-> grid_master.csv + DB table 'grid_master'")
    return out_dir / "grid_master.csv"


if __name__ == "__main__":
    run()
