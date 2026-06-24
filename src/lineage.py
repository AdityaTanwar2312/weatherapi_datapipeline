"""Row-lineage stamping shared by every source pull.

Four audit fields get attached to every output row:

  * data_type        -- 'historical' (daily/historical/backfill = observed/reanalysis)
                        or 'forecast' (predicted). Critical so DB rows are never
                        ambiguous about whether a value is observed or predicted.
  * pipeline_run_id  -- FIXED, from config (per API/source). Stable across runs;
                        used to tell different pipelines/APIs apart later.
  * insertion_id     -- RANDOM UUID generated once per pipeline run. Identifies
                        this specific insertion/execution event.
  * inserted_at      -- UTC timestamp the rows were produced/written.

`insertion_id` + `inserted_at` are generated ONCE per run and shared across all
rows, so the whole run is traceable to a single id.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass

import pandas as pd

# lineage columns, in the order they are appended (kept last in every output)
LINEAGE_COLUMNS = ["data_type", "pipeline_run_id", "insertion_id", "inserted_at"]


@dataclass
class RunContext:
    pipeline_run_id: str
    insertion_id: str
    inserted_at: pd.Timestamp
    data_type: str = "historical"   # 'historical' (observed) | 'forecast' (predicted)

    @classmethod
    def create(
        cls,
        pipeline_run_id: str,
        insertion_id: str | None = None,
        inserted_at: pd.Timestamp | None = None,
        data_type: str = "historical",
    ) -> "RunContext":
        return cls(
            pipeline_run_id=pipeline_run_id,
            insertion_id=insertion_id or str(uuid.uuid4()),
            inserted_at=inserted_at if inserted_at is not None else pd.Timestamp.now(tz="UTC"),
            data_type=data_type,
        )

    def stamp(self, df: pd.DataFrame) -> pd.DataFrame:
        """Append the lineage columns (incl. data_type) to a copy of `df`."""
        out = df.copy()
        out["data_type"] = self.data_type
        out["pipeline_run_id"] = self.pipeline_run_id
        out["insertion_id"] = self.insertion_id
        out["inserted_at"] = self.inserted_at
        return out
