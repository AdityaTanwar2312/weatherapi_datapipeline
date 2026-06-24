"""Single entry point for the India weather + air-quality data pipeline.

    python main.py <command> [options]

Sources: Open-Meteo weather + Open-Meteo air quality. (IMD has been removed.)

Commands
--------
  run                           HISTORY + FORECAST -> single accumulating dataset per
                                source (data/datasets/{weather,air_quality}.csv).  <- primary
  daily                         trailing-window history only -> data/daily/
  forecast                      forecast only (weather 16d, AQ 7d) -> data/forecast/
  historical <start> <end>      pull a date range (chunked, resumable) -> data/historical/
  backfill                      bulk weather history -> parquet (resumable)
  master                        build the lat/lon -> state/district/pincode master
  all                           run + master  (the recurring whole pipeline)

Examples
--------
  python main.py run                       # today's history + forecast into the datasets
  python main.py run --days 7 --aq-days 7   # cap forecast horizon (rate-limit friendly)
  python main.py historical 2025-01-01 2025-03-31
  python main.py all

Dates are automatic (anchored on today). Everything else (grid, variables,
forecast horizon, throttle, pipeline ids) lives in config.yaml.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import Config
from src import pipeline


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="main.py", description="India weather + air-quality pipeline")
    sub = p.add_subparsers(dest="command", required=True)

    r = sub.add_parser("run", help="history + forecast -> single accumulating dataset per source")
    r.add_argument("--days", type=int, default=None, help="weather forecast horizon (max 16)")
    r.add_argument("--aq-days", type=int, default=None, help="air-quality forecast horizon (max 7)")

    sub.add_parser("daily", help="trailing-window history only -> data/daily/")

    f = sub.add_parser("forecast", help="forecast only -> data/forecast/")
    f.add_argument("--days", type=int, default=None, help="weather forecast horizon (max 16)")
    f.add_argument("--aq-days", type=int, default=None, help="air-quality forecast horizon (max 7)")

    h = sub.add_parser("historical", help="pull a date range (chunked, resumable) -> CSV")
    h.add_argument("start", help="start date YYYY-MM-DD (inclusive)")
    h.add_argument("end", help="end date YYYY-MM-DD (inclusive)")
    h.add_argument("--sources", nargs="+", default=list(pipeline.ALL_SOURCES),
                   choices=list(pipeline.ALL_SOURCES), help="which sources to pull")
    h.add_argument("--chunk-months", type=int, default=1, help="split range into N-month chunks")
    h.add_argument("--no-skip-existing", action="store_true", help="re-fetch chunks already written")

    b = sub.add_parser("backfill", help="bulk weather history -> parquet (uses config.time window)")
    b.add_argument("--source", default="openmeteo", choices=["openmeteo"])

    sub.add_parser("master", help="build the lat/lon -> state/district/pincode master")
    sub.add_parser("db-init", help="create the databank database + weather/air_quality tables")
    sub.add_parser("db-harden", help="constraints/FK/ENUM, indexes, least-privilege roles, etl_run_log")
    sub.add_parser("all", help="run + master (the recurring whole pipeline)")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    cfg = Config.load()

    if args.command == "run":
        pipeline.run_combined(cfg, days_weather=args.days, days_aq=args.aq_days)
    elif args.command == "daily":
        pipeline.run_daily(cfg)
    elif args.command == "forecast":
        pipeline.run_forecast(cfg, days_weather=args.days, days_aq=args.aq_days)
    elif args.command == "historical":
        pipeline.run_historical(
            cfg, start_date=args.start, end_date=args.end, sources=args.sources,
            chunk_months=args.chunk_months, skip_existing=not args.no_skip_existing,
        )
    elif args.command == "backfill":
        pipeline.backfill(cfg, source=args.source)
    elif args.command == "master":
        pipeline.build_master(cfg)
    elif args.command == "db-init":
        pipeline.db_init(cfg)
    elif args.command == "db-harden":
        pipeline.db_harden(cfg)
    elif args.command == "all":
        pipeline.run_all(cfg)


if __name__ == "__main__":
    main()
