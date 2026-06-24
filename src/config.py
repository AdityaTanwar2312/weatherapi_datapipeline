"""Config loader. Single source of truth = config.yaml at repo root."""
from __future__ import annotations

from pathlib import Path
from datetime import date
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


class Config:
    def __init__(self, raw: dict):
        self._raw = raw

    @classmethod
    def load(cls, path: str | Path | None = None) -> "Config":
        path = Path(path) if path else REPO_ROOT / "config.yaml"
        with open(path, "r", encoding="utf-8") as fh:
            return cls(yaml.safe_load(fh))

    # convenient typed accessors -------------------------------------
    @property
    def region(self) -> dict:
        return self._raw["region"]

    @property
    def time(self) -> dict:
        return self._raw["time"]

    @property
    def openmeteo(self) -> dict:
        return self._raw["openmeteo"]

    @property
    def airquality(self) -> dict:
        return self._raw["airquality"]

    @property
    def imd(self) -> dict:
        return self._raw["imd"]

    @property
    def storage(self) -> dict:
        return self._raw["storage"]

    @property
    def database(self) -> dict:
        return self._raw["database"]

    def years(self) -> list[int]:
        """Calendar years spanned by the configured window (for imdlib)."""
        y0 = date.fromisoformat(self.time["start_date"]).year
        y1 = date.fromisoformat(self.time["end_date"]).year
        return list(range(y0, y1 + 1))

    def path(self, key: str, *parts: str) -> Path:
        """Resolve a storage dir (raw_dir/processed_dir) under the repo root."""
        base = REPO_ROOT / self.storage[key]
        p = base.joinpath(*parts)
        p.parent.mkdir(parents=True, exist_ok=True) if p.suffix else p.mkdir(parents=True, exist_ok=True)
        return p
