"""Upsert a fetched DataFrame into a databank table (accumulate via ON CONFLICT)."""
from __future__ import annotations

import pandas as pd
from psycopg2.extras import execute_values

from src.config import Config
from src.db.connection import connect

PK = ("cell_id", "valid_time", "data_type")


def upsert(df: pd.DataFrame, table: str, cfg: Config | None = None) -> int:
    """Insert rows; on PK conflict update all non-key columns. Returns table row count."""
    if df is None or df.empty:
        return 0
    cfg = cfg or Config.load()

    df = df.rename(columns={"time": "valid_time"})
    cols = list(df.columns)

    # pandas NaN / NA / NaT -> None so they become SQL NULL
    obj = df.astype(object)
    obj = obj.where(obj.notna(), None)
    rows = list(obj.itertuples(index=False, name=None))

    collist = ", ".join(f'"{c}"' for c in cols)
    updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in PK)
    sql = (f'INSERT INTO "{table}" ({collist}) VALUES %s '
           f'ON CONFLICT (cell_id, valid_time, data_type) DO UPDATE SET {updates}')

    conn = connect(cfg)
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        with conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "{table}"')
            total = cur.fetchone()[0]
    finally:
        conn.close()
    return total
