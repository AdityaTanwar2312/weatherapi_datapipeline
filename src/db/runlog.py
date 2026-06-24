"""Write ETL run records to etl_run_log (observability). Best-effort: logging
failures never break the pipeline."""
from __future__ import annotations

from src.config import Config
from src.db.connection import connect


def log(cfg: Config, *, insertion_id, command, source, phase,
        window_start, window_end, rows_written, status, error=None) -> None:
    try:
        conn = connect(cfg)
        with conn, conn.cursor() as cur:
            cur.execute(
                'INSERT INTO etl_run_log '
                '(insertion_id, command, source, phase, window_start, window_end, '
                ' rows_written, status, error, finished_at) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, now())',
                (str(insertion_id), command, source, phase,
                 window_start, window_end, rows_written, status, error))
        conn.close()
    except Exception as exc:   # never let observability break the run
        print(f"  [runlog] skipped ({type(exc).__name__}: {str(exc)[:80]})")
