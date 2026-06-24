"""Cost-aware, cached, retrying Open-Meteo client wrapper.

Open-Meteo's free limits (600/min, 5k/hr, 10k/day) are charged in
*weighted cost units*, NOT raw HTTP requests. A request's cost grows with
(#variables / 14) x (#days / 14) per location -- so a 100-cell batch with
25 variables can burn ~hundreds of units in one shot.

This wrapper therefore:
  * estimates each batch's cost before sending,
  * paces requests against a rolling 60s budget,
  * catches "limit exceeded" errors and backs off 65s then retries,
  * caches responses on disk so re-runs are free & idempotent.
"""
from __future__ import annotations

import time
from collections import deque

import openmeteo_requests
import requests_cache
from retry_requests import retry
from openmeteo_requests.Client import OpenMeteoRequestsError

from src.config import REPO_ROOT


def estimate_cost(n_cells: int, n_vars: int, n_days: int) -> float:
    """Open-Meteo weighted-call estimate. Baseline 14 vars x 14 days = 1.0/cell."""
    per_cell = max(n_vars / 14.0, 1.0) * max(n_days / 14.0, 1.0)
    return n_cells * per_cell


class ThrottledOpenMeteo:
    def __init__(self, cfg_openmeteo: dict):
        self.cfg = cfg_openmeteo
        self.url = cfg_openmeteo["archive_url"]
        # Stay safely under 600/min. Reactive backoff handles the rest.
        self.minute_budget = float(cfg_openmeteo.get("minute_cost_budget", 500.0))
        self.backoff_seconds = float(cfg_openmeteo.get("rate_limit_backoff_s", 65.0))
        self.max_retries = int(cfg_openmeteo.get("max_retries", 5))
        self._window: deque[tuple[float, float]] = deque()  # (timestamp, cost)

        cache_path = str(REPO_ROOT / ".openmeteo_cache")
        cache_session = requests_cache.CachedSession(cache_path, expire_after=-1)
        # retry-requests handles transient 5xx/connection errors
        retry_session = retry(cache_session, retries=3, backoff_factor=0.5)
        self._client = openmeteo_requests.Client(session=retry_session)

    # --- rolling 60s cost budget ------------------------------------
    def _spent_last_minute(self) -> float:
        cutoff = time.monotonic() - 60.0
        while self._window and self._window[0][0] < cutoff:
            self._window.popleft()
        return sum(c for _, c in self._window)

    def _pace(self, cost: float):
        """Block until sending `cost` keeps us under the minute budget."""
        while self._spent_last_minute() + cost > self.minute_budget and self._window:
            oldest_ts = self._window[0][0]
            sleep_for = max(0.0, 60.0 - (time.monotonic() - oldest_ts)) + 0.5
            time.sleep(sleep_for)
        self._window.append((time.monotonic(), cost))

    def fetch(self, lats: list[float], lons: list[float], params: dict, cost: float):
        """One archive call for a batch of coords, with pacing + backoff."""
        self._pace(cost)
        full_params = {"latitude": lats, "longitude": lons, **params}
        if self.cfg.get("api_key"):
            full_params["apikey"] = self.cfg["api_key"]

        for attempt in range(self.max_retries):
            try:
                return self._client.weather_api(self.url, params=full_params)
            except OpenMeteoRequestsError as exc:
                reason = str(exc).lower()
                # Only the per-MINUTE limit is worth waiting out (~60 s). Hourly /
                # daily / monthly limits won't clear in seconds -> fail fast so the
                # caller can skip gracefully instead of stalling for minutes.
                if "minutely" in reason and attempt < self.max_retries - 1:
                    wait = self.backoff_seconds
                    print(f"  [rate-limit/minute] sleeping {wait:.0f}s "
                          f"(attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(wait)
                    self._window.clear()
                    continue
                raise  # hourly/daily limit or any other error -> propagate immediately
        raise RuntimeError("exhausted retries against Open-Meteo")
