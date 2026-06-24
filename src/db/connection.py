"""PostgreSQL connection helpers.

Two principals (least privilege):
  * app role   (database.user)        -- owns the databank objects; used for all
                                         normal pipeline ops (DML + DDL on its tables).
  * admin role (database.admin_user)  -- superuser; used ONLY to bootstrap roles /
                                         create the database.

Passwords come from .env / env vars, never from config. TLS via `sslmode`.
"""
from __future__ import annotations

import os

import psycopg2

from src.config import REPO_ROOT, Config


def load_dotenv() -> None:
    """Minimal .env loader. Splits on the FIRST '=' so a value may contain '#'
    (e.g. a password starting with '#'); only whole comment lines are skipped."""
    env = REPO_ROOT / ".env"
    if not env.exists():
        return
    for raw in env.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip())


def _secret(var: str) -> str:
    load_dotenv()
    pw = os.environ.get(var)
    if not pw or pw == "your_postgres_password_here":
        raise RuntimeError(f"secret not set — put `{var}=...` in .env at the repo root.")
    return pw


def connect(cfg: Config | None = None, dbname: str | None = None, admin: bool = False):
    """Connect as the app role (default) or the admin role (`admin=True`)."""
    cfg = cfg or Config.load()
    db = cfg.database
    if admin:
        user = db.get("admin_user", "postgres")
        pw = _secret(db.get("admin_password_env", "DATABANK_PASSWORD"))
    else:
        user = db["user"]
        pw = _secret(db.get("password_env", "DATABANK_PASSWORD"))
    return psycopg2.connect(
        host=db["host"],
        port=int(db["port"]),
        dbname=dbname or db["dbname"],
        user=user,
        password=pw,
        sslmode=db.get("sslmode", "prefer"),
        connect_timeout=10,
    )
