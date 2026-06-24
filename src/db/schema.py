"""Create the `databank` database and the wide `weather` / `air_quality` tables.

DDL is generated from the config variable lists, so the tables always match what
the pipeline fetches. Primary key (cell_id, valid_time, data_type) mirrors the
CSV dedup key, so ON CONFLICT upserts give the same accumulate semantics.
"""
from __future__ import annotations

from src.config import Config
from src.db.connection import connect

# measurement variables that are whole numbers -> SMALLINT; everything else REAL
_SMALLINT_VARS = {"weather_code", "is_day"}


def _measurement_type(col: str) -> str:
    return "SMALLINT" if col in _SMALLINT_VARS else "REAL"


def table_ddl(table: str, variables: list[str]) -> str:
    var_cols = ",\n  ".join(f'"{v}" {_measurement_type(v)}' for v in variables)
    return f'''
CREATE TABLE IF NOT EXISTS "{table}" (
  cell_id         INTEGER     NOT NULL,
  src_lat         REAL,
  src_lon         REAL,
  valid_time      TIMESTAMPTZ NOT NULL,
  {var_cols},
  data_type       TEXT        NOT NULL,
  pipeline_run_id TEXT,
  insertion_id    UUID,
  inserted_at     TIMESTAMPTZ,
  PRIMARY KEY (cell_id, valid_time, data_type)
);'''


def create_database(cfg: Config | None = None) -> None:
    cfg = cfg or Config.load()
    name = cfg.database["dbname"]
    conn = connect(cfg, dbname="postgres", admin=True)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
        if cur.fetchone():
            print(f"[db] database '{name}' already exists")
        else:
            cur.execute(f'CREATE DATABASE "{name}"')
            print(f"[db] created database '{name}'")
    conn.close()


def create_tables(cfg: Config | None = None) -> None:
    cfg = cfg or Config.load()
    wt, at = cfg.database["weather_table"], cfg.database["airquality_table"]
    conn = connect(cfg, admin=True)   # DDL/bootstrap via admin; ownership moves to app in db-harden
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(table_ddl(wt, cfg.openmeteo["hourly_variables"]))
        cur.execute(table_ddl(at, cfg.airquality["hourly_variables"]))
    conn.close()
    print(f"[db] ensured tables: '{wt}' ({len(cfg.openmeteo['hourly_variables'])} vars), "
          f"'{at}' ({len(cfg.airquality['hourly_variables'])} vars)")


def init(cfg: Config | None = None) -> None:
    cfg = cfg or Config.load()
    create_database(cfg)
    create_tables(cfg)
