"""Production hardening for the databank schema (idempotent; run as the admin role).

Applies, to the existing tables:
  * ENUM `data_kind` for data_type (controlled vocabulary)
  * NOT NULL on key/lineage columns
  * CHECK constraints grounded in the data's physical ranges
       (NOTE: us_aqi/pm2_5 are lower-bound-only — they legitimately exceed 500)
  * FK cell_id -> grid_master(cell_id)  (referential integrity)
  * BRIN index on valid_time  (append-only time-series; tiny & ideal)
  * etl_run_log table  (observability)
  * least-privilege roles: databank_app (owns objects) + databank_reader (SELECT)
"""
from __future__ import annotations

import os

import psycopg2
from psycopg2 import errors as pgerr

from src.config import Config
from src.db.connection import connect, _secret

ENUM_NAME = "data_kind"

# (name, expr) CHECK constraints per table. NULLs pass CHECKs automatically.
_CHECKS = {
    "weather": [
        ("ck_weather_lat", "src_lat BETWEEN 6 AND 38"),
        ("ck_weather_lon", "src_lon BETWEEN 67 AND 98"),
        ("ck_weather_rh", "relative_humidity_2m BETWEEN 0 AND 100"),
        ("ck_weather_cloud", "cloud_cover BETWEEN 0 AND 100"),
        ("ck_weather_wdir10", "wind_direction_10m BETWEEN 0 AND 360"),
        ("ck_weather_precip", "precipitation >= 0"),
        ("ck_weather_ws10", "wind_speed_10m >= 0"),
    ],
    "air_quality": [
        ("ck_aq_lat", "src_lat BETWEEN 6 AND 38"),
        ("ck_aq_lon", "src_lon BETWEEN 67 AND 98"),
        ("ck_aq_pm25", "pm2_5 >= 0"),
        ("ck_aq_pm10", "pm10 >= 0"),
        ("ck_aq_usaqi", "us_aqi >= 0"),   # lower bound only (dust events exceed 500)
        ("ck_aq_ozone", "ozone >= 0"),
    ],
}
_NOT_NULL = ["src_lat", "src_lon", "pipeline_run_id", "inserted_at"]


def _try(cur, sql, label):
    """Run a statement; treat 'already exists' as success (idempotent)."""
    try:
        cur.execute(sql)
        print(f"   + {label}")
    except (pgerr.DuplicateObject, pgerr.DuplicateTable, pgerr.DuplicateColumn,
            pgerr.UniqueViolation) as e:
        print(f"   = {label} (exists)")


def _create_roles(cur, cfg):
    app_pw = _secret(cfg.database["password_env"])
    rdr_pw = _secret(cfg.database["reader_password_env"])
    app, rdr = cfg.database["user"], cfg.database["reader_user"]
    for role, pw in [(app, app_pw), (rdr, rdr_pw)]:
        cur.execute(f"""
            DO $$ BEGIN
              IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='{role}') THEN
                 CREATE ROLE {role} LOGIN PASSWORD '{pw}';
              ELSE
                 ALTER ROLE {role} LOGIN PASSWORD '{pw}';
              END IF;
            END $$;""")
    print(f"   + roles: {app} (app), {rdr} (reader)")


def _create_etl_log(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_run_log (
          run_id        BIGSERIAL PRIMARY KEY,
          insertion_id  UUID,
          command       TEXT,
          source        TEXT,
          phase         TEXT,
          window_start  DATE,
          window_end    DATE,
          rows_written  BIGINT,
          status        TEXT,
          error         TEXT,
          started_at    TIMESTAMPTZ DEFAULT now(),
          finished_at   TIMESTAMPTZ
        )""")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_etl_started ON etl_run_log USING brin (started_at)")
    print("   + etl_run_log")


def _ensure_grid_master(cur):
    """Create an empty grid_master if missing, so the FK can reference it on a
    fresh DB. `main.py master` populates it afterwards."""
    cur.execute('CREATE TABLE IF NOT EXISTS "grid_master" ('
                'cell_id INTEGER PRIMARY KEY, latitude REAL, longitude REAL, '
                'resolution_deg REAL, in_india BOOLEAN, '
                'state TEXT, district TEXT, pincode TEXT)')
    print("   + grid_master (ensured)")


def _enum_and_constraints(cur, cfg):
    tables = [cfg.database["weather_table"], cfg.database["airquality_table"]]
    # ENUM type
    cur.execute(f"""
        DO $$ BEGIN
          IF NOT EXISTS (SELECT FROM pg_type WHERE typname='{ENUM_NAME}') THEN
             CREATE TYPE {ENUM_NAME} AS ENUM ('historical','forecast');
          END IF;
        END $$;""")
    print(f"   + enum {ENUM_NAME}")
    for t in tables:
        # convert data_type TEXT -> data_kind if not already
        cur.execute("SELECT udt_name FROM information_schema.columns "
                    "WHERE table_name=%s AND column_name='data_type'", (t,))
        row = cur.fetchone()
        if row and row[0] != ENUM_NAME:
            cur.execute(f'ALTER TABLE "{t}" ALTER COLUMN data_type TYPE {ENUM_NAME} '
                        f'USING data_type::{ENUM_NAME}')
            print(f"   + {t}.data_type -> {ENUM_NAME}")
        # NOT NULL
        for col in _NOT_NULL:
            _try(cur, f'ALTER TABLE "{t}" ALTER COLUMN {col} SET NOT NULL', f"{t}.{col} NOT NULL")
        # CHECK constraints
        for name, expr in _CHECKS[t]:
            _try(cur, f'ALTER TABLE "{t}" ADD CONSTRAINT {name} CHECK ({expr})', f"{t} {name}")
        # FK -> grid_master
        _try(cur, f'ALTER TABLE "{t}" ADD CONSTRAINT fk_{t}_cell '
                  f'FOREIGN KEY (cell_id) REFERENCES grid_master(cell_id)', f"{t} FK cell_id")
        # BRIN on valid_time
        _try(cur, f'CREATE INDEX IF NOT EXISTS ix_{t}_valid_time ON "{t}" USING brin (valid_time)',
             f"{t} BRIN(valid_time)")


def _ownership_and_grants(cur, cfg):
    app, rdr = cfg.database["user"], cfg.database["reader_user"]
    db = cfg.database["dbname"]
    objs = [cfg.database["weather_table"], cfg.database["airquality_table"],
            "grid_master", "etl_run_log"]
    cur.execute(f"GRANT CREATE, USAGE ON SCHEMA public TO {app}")
    for o in objs:
        _try(cur, f'ALTER TABLE "{o}" OWNER TO {app}', f"own {o} -> {app}")
    _try(cur, f"ALTER TYPE {ENUM_NAME} OWNER TO {app}", f"own type -> {app}")
    _try(cur, f"ALTER SEQUENCE etl_run_log_run_id_seq OWNER TO {app}", "own seq -> app")
    # read-only role
    cur.execute(f"GRANT CONNECT ON DATABASE {db} TO {rdr}")
    cur.execute(f"GRANT USAGE ON SCHEMA public TO {rdr}")
    cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {rdr}")
    cur.execute(f"ALTER DEFAULT PRIVILEGES FOR ROLE {app} IN SCHEMA public "
                f"GRANT SELECT ON TABLES TO {rdr}")
    print(f"   + ownership -> {app}; SELECT -> {rdr}")


def harden(cfg: Config | None = None) -> None:
    cfg = cfg or Config.load()
    conn = connect(cfg, admin=True)   # superuser: role creation + ownership
    conn.autocommit = True
    with conn.cursor() as cur:
        print("[harden] roles ...");        _create_roles(cur, cfg)
        print("[harden] etl_run_log ...");   _create_etl_log(cur)
        print("[harden] grid_master ...");   _ensure_grid_master(cur)
        print("[harden] enum + constraints + indexes ..."); _enum_and_constraints(cur, cfg)
        print("[harden] ownership + grants ..."); _ownership_and_grants(cur, cfg)
    conn.close()
    print("[harden] done.")
