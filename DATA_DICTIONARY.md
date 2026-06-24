# Data Dictionary — India Weather & Air-Quality (PostgreSQL `databank`)

Every column in the three tables: what it signifies, its values, and units.

| Table | Source | Columns | Grain |
|---|---|---|---|
| **`weather`** | Open-Meteo (ERA5 historical + forecast) | 54 | one row per **cell × hour** |
| **`air_quality`** | Open-Meteo (CAMS historical + forecast) | 25 | one row per **cell × hour** |
| **`grid_master`** | derived (GADM + GeoNames) | 8 | one row per **cell** — the **location mapping** |

Scope: **India only**, hourly, on a **0.5°** grid (~1,109 cells; `resolution_deg`
in `config.yaml`). Measurement values are stored at **full float precision (never
rounded)**. Location lives **only** in `grid_master` — the data tables join to it on
`cell_id`.

---

## 1. Data-table identity & coordinate columns (`weather`, `air_quality`)

| Column | Type / Unit | Meaning | Values |
|---|---|---|---|
| `cell_id` | integer | **The join key** — to `grid_master` and across the two sources. Assigned on the full bbox grid, preserved after the India filter (non-contiguous). | e.g. `231`, `465` |
| `src_lat` | degrees (°N) | **Actual source-cell** latitude the API returned (real ERA5/CAMS cell), full precision. The only latitude stored on data rows. | e.g. `28.506149` |
| `src_lon` | degrees (°E) | **Actual source-cell** longitude, full precision. | e.g. `76.996582` |
| `valid_time` | timestamptz (UTC) | The hour the measurement/forecast refers to. Hourly. Stored UTC; convert to IST (+05:30) for local. | `2026-06-12 18:30:00+00` |

> The requested grid `lat/lon` and the location columns (`state/district/pincode`) are
> **not** on the data rows. Location comes from `grid_master` (§3) via `cell_id`.
> Because weather (ERA5) and air quality (CAMS) snap to different native grids, the
> same `cell_id` has **different** `src_lat/src_lon` in each table — join on
> `cell_id`, never on coordinates.

---

## 2. Lineage / audit columns (`weather`, `air_quality`)

| Column | Type | Meaning | Values |
|---|---|---|---|
| `data_type` | text | observed vs predicted flag | `historical` (ERA5/CAMS) · `forecast` |
| `pipeline_run_id` | text | fixed per-source id (from config) | `openmeteo_daily_v1` · `airquality_daily_v1` |
| `insertion_id` | uuid | random id, one per pipeline run | `9d04d194-…` |
| `inserted_at` | timestamptz (UTC) | when the row was fetched/written (audit) | `2026-06-18 …+00` |

Primary key / dedup: `(cell_id, valid_time, data_type)` — re-runs upsert (keep latest `inserted_at`).

---

## 3. `grid_master` — the single source of truth for location

One row per `cell_id`, built from the **canonical grid point** (shared by both
sources) and enriched once. Join any data row to it on `cell_id`.

| Column | Type / Unit | Meaning |
|---|---|---|
| `cell_id` | integer (PK) | grid-cell id (matches the data tables) |
| `latitude` | degrees (°N) | **canonical** grid-point latitude (the requested point) |
| `longitude` | degrees (°E) | **canonical** grid-point longitude |
| `resolution_deg` | degrees | grid spacing (0.5) |
| `in_india` | boolean | whether the cell is inside India's polygons |
| `state` | text | Indian state / UT (GADM `NAME_1`) |
| `district` | text | Indian district (GADM `NAME_2`) |
| `pincode` | text | nearest PIN code centroid (GeoNames) |

Example join:
```sql
SELECT w.cell_id, m.state, m.district, w.valid_time, w.temperature_2m, w.data_type
FROM weather w JOIN grid_master m USING (cell_id)
WHERE m.state = 'Delhi' AND w.data_type = 'forecast';
```

---

## 4. Weather measurement columns (`weather`, 46 variables)

Open-Meteo default units. "avg" = hourly mean; "instant" = value at the timestamp.

### Temperature & humidity
| Column | Unit | Meaning |
|---|---|---|
| `temperature_2m` | °C | air temperature at 2 m |
| `dew_point_2m` | °C | dew-point temperature at 2 m |
| `apparent_temperature` | °C | "feels-like" temperature |
| `wet_bulb_temperature_2m` | °C | wet-bulb temperature at 2 m |
| `relative_humidity_2m` | % | relative humidity at 2 m |

### Pressure
| Column | Unit | Meaning |
|---|---|---|
| `pressure_msl` | hPa | mean-sea-level pressure |
| `surface_pressure` | hPa | surface pressure (elevation-dependent) |

### Precipitation & sky
| Column | Unit | Meaning |
|---|---|---|
| `precipitation` | mm | total precip in the hour (rain + showers + snow w.e.) |
| `rain` | mm | liquid rain in the hour |
| `showers` | mm | convective showers in the hour |
| `snowfall` | cm | snowfall in the hour |
| `snow_depth` | m | snow depth on the ground |
| `weather_code` | WMO code (int) | 0 clear; 1–3 partly cloudy; 45/48 fog; 51–67 drizzle/rain; 71–77 snow; 80–82 showers; 95–99 thunderstorm |
| `cloud_cover` | % | total cloud cover |
| `cloud_cover_low` | % | low cloud (< ~3 km) |
| `cloud_cover_mid` | % | mid cloud (~3–8 km) |
| `cloud_cover_high` | % | high cloud (> ~8 km) |

### Atmospheric moisture & boundary layer
| Column | Unit | Meaning |
|---|---|---|
| `et0_fao_evapotranspiration` | mm | reference evapotranspiration (FAO-56) |
| `vapour_pressure_deficit` | kPa | air dryness (plant water stress) |
| `total_column_integrated_water_vapour` | kg/m² | precipitable water |
| `boundary_layer_height` | m | planetary boundary-layer height |

### Wind
| Column | Unit | Meaning |
|---|---|---|
| `wind_speed_10m` | km/h | wind speed at 10 m |
| `wind_speed_100m` | km/h | wind speed at 100 m |
| `wind_direction_10m` | ° | wind direction at 10 m (0=N, 90=E) |
| `wind_direction_100m` | ° | wind direction at 100 m |
| `wind_gusts_10m` | km/h | max gust at 10 m in the hour |

### Soil temperature / moisture (4 layers each)
| Column | Unit | Meaning |
|---|---|---|
| `soil_temperature_0_to_7cm` … `_7_to_28cm` … `_28_to_100cm` … `_100_to_255cm` | °C | soil temperature by depth layer |
| `soil_moisture_0_to_7cm` … `_7_to_28cm` … `_28_to_100cm` … `_100_to_255cm` | m³/m³ | volumetric soil moisture by depth layer |

### Solar radiation (hourly avg + instantaneous)
| Column | Unit | Meaning |
|---|---|---|
| `shortwave_radiation` / `_instant` | W/m² | global horizontal irradiance (GHI) |
| `direct_radiation` / `_instant` | W/m² | direct (beam) radiation |
| `diffuse_radiation` / `_instant` | W/m² | diffuse (scattered) radiation |
| `direct_normal_irradiance` / `_instant` | W/m² | direct normal irradiance (DNI) |
| `terrestrial_radiation` / `_instant` | W/m² | top-of-atmosphere radiation |

### Other
| Column | Unit | Meaning |
|---|---|---|
| `is_day` | 0/1 | 1 if daylight at that hour |
| `sunshine_duration` | seconds | sunshine seconds in the hour (0–3600) |

---

## 5. Air-quality measurement columns (`air_quality`, 17 variables)

Source: CAMS via Open-Meteo (global product over India, 0.4°/~0.1° served).

### Pollutant concentrations
| Column | Unit | Meaning |
|---|---|---|
| `pm10`, `pm2_5` | µg/m³ | coarse / fine particulate matter |
| `carbon_monoxide`, `nitrogen_dioxide`, `sulphur_dioxide`, `ozone` | µg/m³ | CO / NO₂ / SO₂ / O₃ |
| `dust` | µg/m³ | mineral dust |
| `aerosol_optical_depth` | — | column aerosol optical depth |
| `uv_index` | index | UV index |

### Air Quality Indices
No India/CPCB AQI exists in Open-Meteo; `us_aqi` is the headline index, and the raw
pollutants above let a CPCB AQI be derived later.

| Column | Unit | Meaning |
|---|---|---|
| `us_aqi` | index (0–500+) | overall US-EPA AQI (max of sub-indices) |
| `us_aqi_pm2_5`, `us_aqi_pm10`, `us_aqi_nitrogen_dioxide`, `us_aqi_ozone`, `us_aqi_sulphur_dioxide`, `us_aqi_carbon_monoxide` | index | per-pollutant US sub-indices |
| `european_aqi` | index (0–100+) | overall European AQI |

---

## 6. Notes

- **Location is normalized:** state/district/pincode + canonical lat/lon live only in
  `grid_master`. Join `weather`/`air_quality` to it on `cell_id`.
- **Two coordinate ideas:** `grid_master.latitude/longitude` = the *canonical* grid
  point (same for both sources); `src_lat/src_lon` on data rows = each source's *actual*
  cell (provenance). They differ by up to ~16 km for weather (ERA5 snapping).
- **Units** are Open-Meteo metric defaults (°C, mm, km/h, hPa, W/m², µg/m³).
- **Blanks/NaN:** `state/district/pincode` blank only for rare border cells just
  outside India; some weather fields are physically ~0/NaN (snow over most of India,
  soil over water); `ammonia` is excluded from air quality (all-NaN over India).
- **Native resolution:** ERA5 0.25°, CAMS 0.4° — sampling finer adds no new information.
