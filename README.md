# AtmosLens

AtmosLens is a HoloViz ecosystem application built using the libraries surfaced through [`holoviz/holoviz`](https://github.com/holoviz/holoviz), with implementation centered on Lumen, Panel, HoloViews, GeoViews, hvPlot, Datashader, Param, and Colorcet over xarray-backed air-quality data. It turns a real forecast cube into recommendations such as when to run, when to ventilate, and which commute departure window minimizes exposure, and supports typed global place search, compact default controls, and professional-grade advanced controls that stay in sync automatically.

![AtmosLens app preview](assets/atmoslens-preview.svg)

## Why this exists

Air-quality forecasts are easy to find and hard to act on. Most people still have to translate a map, a number, and a time series into a decision on their own.

AtmosLens closes that gap:

- pick a location
- pick a health profile
- pick an activity
- pick a pollutant and time horizon
- get a clear verdict, a best window, a map, a timeline, and a route-exposure comparison

This repo is intentionally scoped as a strong March 31 artifact: something a HoloViz mentor can open, run, understand quickly, and recognize as a natural bridge toward native Lumen + xarray support.

The strategic purpose is explicit: AtmosLens is the first vertical slice and public proof-of-need for the official HoloViz GSoC 2026 project **Lumen + Xarray Integration**, which the HoloViz project list describes as a high-priority 350-hour effort focused on native xarray support, an `XarraySource`, and explicit query semantics in Lumen. Sources: [HoloViz 2026 GSoC wiki](https://github.com/holoviz/holoviz/wiki/2026-GSoC-Project-List), [GSoC timeline](https://developers.google.com/open-source/gsoc/timeline).

## What the app shows

- **Activity Safety Advisor**: `Good`, `Caution`, or `Avoid`, plus the best time window and a short explanation.
- **Interactive Pollution Map**: xarray-backed gridded data rendered with GeoViews + hvPlot.
- **24-hour Forecast Timeline**: threshold bands and the highlighted best window.
- **Decision Matrix**: compares profiles and activities side by side at the same location to show that the recommendation engine generalizes beyond a single query.
- **Recommendation Card**: concise user-facing guidance instead of a raw forecast dump.
- **Route / Commute Exposure Window**: preset or search-driven route endpoints sampled against the same gridded forecast across multiple departure times.
- **Global Search + Region Refresh**: type a city, district, or postcode anywhere on Earth, press Enter, and refresh the forecast cube through the same xarray pipeline.
- **Professional Controls**: advanced region, route, and analysis parameters stay linked automatically while remaining hidden from the default user flow.

The preview above uses the real Dublin sample cube with `Ozone` selected because it produces a more legible risk gradient than PM2.5 on the fetched March 25 forecast.

## Data resilience

AtmosLens uses a three-tier fallback chain for forecast data:

1. **Live gridded forecast** — full Open-Meteo Air Quality API grid for the configured region
2. **Point-to-grid fallback** — single-point forecast expanded to a synthetic local grid (when the grid API is rate-limited)
3. **Bundled template fallback** — bundled Dublin sample cube re-projected to the target region (when the API is completely unavailable)

This means the app never crashes on a failed fetch. It always loads something usable and tells the user what happened.

## Health guidance and WHO references

Every recommendation includes:

- **Health guidance** — plain-language advice calibrated to the decision score (e.g., "Air quality is excellent for run. No precautions needed.")
- **WHO guideline reference** — the relevant WHO air-quality guideline value for context (e.g., "WHO guideline: 100 µg/m³ (8-hour mean)")
- **Score interpretation** — human-readable label mapping the 0–100 score to severity (Excellent / Good / Moderate / Unhealthy for sensitive groups / Unhealthy / Hazardous)
- **7 activity types** — Run, Walk, Ventilate, Cycle Commute, Outdoor Dining, Children's Play, Dog Walk — each with distinct exertion multipliers and window durations
- **4 health profiles** — General, Sensitive, Asthma, Outdoor Worker — each with distinct threshold multipliers

## HoloViz stack used explicitly

- `Panel` for the application shell, widgets, cards, and layout
- `GeoViews` for map-native overlays and route rendering
- `HoloViews` for structured overlays such as threshold bands and best-window spans
- `hvPlot` for quick plotting directly from xarray / pandas objects
- `Datashader` for rasterizing the map layer through the HoloViews/GeoViews stack
- `Lumen` for `AtmosXarraySource` (a real `lumen.sources.Source` subclass) and in-app pipeline previews
- `Param` for reactive state instead of ad hoc widget wiring
- `Colorcet` for scientifically sane colormaps
- `xarray` as the canonical data model for labeled N-dimensional air-quality data

## Why xarray matters here

- The source data is a real forecast cube with `time x lat x lon` dimensions.
- Location lookups, map slices, and route sampling all come from the same labeled dataset.
- The app logic stays dimension-aware instead of flattening everything into unrelated tables.
- That makes AtmosLens a credible motivating artifact for upstream Lumen work on first-class xarray sources and transforms.

## GSoC 2026 — Lumen + Xarray Integration prototype

AtmosLens directly prototypes the first steps of the [HoloViz GSoC 2026 "Lumen + Xarray Integration" project](https://github.com/holoviz/holoviz/wiki/2026-GSoC-Project-List) (HIGH priority, 350 hours):

> *"Explore Lumen Source abstractions, prototype a minimal XarraySource, evaluate xarray-sql query translation approaches."*

### `AtmosXarraySource` — real `lumen.sources.Source` subclass

[`src/atmoslens/xarray_source.py`](src/atmoslens/xarray_source.py) contains `AtmosXarraySource`, a concrete `lumen.sources.Source` subclass that wraps the live `xarray.Dataset` and answers queries via **coordinate operations** — not row predicates:

```python
from atmoslens.xarray_source import AtmosXarraySource
import lumen.sources

assert issubclass(AtmosXarraySource, lumen.sources.Source)  # ✅ real Source subclass

source = AtmosXarraySource(dataset=ds)

# Tables = xarray data variables
source.get_tables()          # ['pm2_5', 'nitrogen_dioxide', 'ozone', 'european_aqi']

# Schema exposes coordinate ranges (not column types) — the design gap made explicit
source.get_schema('pm2_5')   # {'dims': ['time', 'lat', 'lon'], 'coords': {'time': {'start': ..., 'end': ..., 'n': 48}, ...}}

# get() operates on labeled axes, not row filters
df = source.get('pm2_5', lat_min=53.25, lat_max=53.45, time_start='2026-03-26T06:00')
```

### SQL via DuckDB — xarray-sql integration

[`src/atmoslens/sql_bridge.py`](src/atmoslens/sql_bridge.py) demonstrates SQL-like querying on xarray data via DuckDB, matching the GSoC spec's *"integration with xarray-sql or similar mechanisms"*:

```sql
-- Executed against a DuckDB in-memory table registered from an xarray slice:
SELECT time, lat, lon, pm2_5
FROM forecast
WHERE time BETWEEN '2026-03-26T00:00' AND '2026-03-27T00:00'
  AND lat BETWEEN 53.242 AND 53.367
  AND pm2_5 > 3.6
ORDER BY pm2_5 DESC
LIMIT 20
```

### Design gap made explicit

| Today (AtmosXarraySource prototype) | Needed in upstream Lumen |
|--------------------------------------|--------------------------|
| `get()` returns `pd.DataFrame` (flattened after slicing) | Pipeline stages pass `xr.DataArray` natively |
| `get_schema()` returns coord ranges as a dict | Lumen planner understands N-dimensional schemas |
| SQL via DuckDB over a flattened slice | `xarray-sql` in the Source layer |
| Coordinate queries via keyword args | Declarative transform spec for xarray ops |

The **Lumen Bridge** tab in the running app shows all of this interactively: `AtmosXarraySource` summary, visual pipeline steps, the live DuckDB SQL query, and the design gap narrative.

![Lumen bridge preview](assets/lumen-bridge-preview.svg)

## Quickstart

### Option 1: Pixi

```bash
pixi install
pixi run start
```

### Option 2: pip + venv

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/panel serve app.py --autoreload --show
```

### Refresh the sample dataset

The repo includes `data/sample_forecast.nc`. To regenerate it from the Open-Meteo air-quality API:

```bash
.venv/bin/atmoslens-fetch --output data/sample_forecast.nc
```

Inside the app, the default flow is:

- type a decision point and press Enter
- review the refreshed recommendation, map, timeline, and decision matrix
- optionally resolve a commute origin and destination
- open `Professional Controls` only if you need to override the default geometry or analysis settings

Route searches auto-fit a local corridor, and `Load Route Corridor Forecast` refreshes the commute cube after manual edits. The fetch layer also retries short upstream failures and surfaces rate-limit errors clearly when the public API is busy.

## Data provenance

- Source: Open-Meteo Air Quality API
- Domain: `cams_europe`
- Region: Dublin commuter belt
- Grid: `48 x 9 x 11` (`time x lat x lon`)
- Variables: `pm2_5`, `nitrogen_dioxide`, `ozone`, `european_aqi`

The fetch path intentionally builds a small regular grid around a real metro region, writes it to NetCDF, and then treats that file as the canonical xarray source for the app.

## Repo layout

- [`app.py`](app.py): Panel entrypoint
- [`ENGINEERING_SPEC.md`](ENGINEERING_SPEC.md): mentor-facing March 31 engineering plan and project framing
- [`pyproject.toml`](pyproject.toml): pip-friendly project definition
- [`pixi.toml`](pixi.toml): conda-forge / Pixi environment
- [`src/atmoslens/datasets.py`](src/atmoslens/datasets.py): real-data fetch and dataset normalization
- [`src/atmoslens/config.py`](src/atmoslens/config.py): app framing, defaults, and ecosystem links
- [`src/atmoslens/profiles.py`](src/atmoslens/profiles.py): health profiles, activities, thresholds
- [`src/atmoslens/scoring.py`](src/atmoslens/scoring.py): best-window scoring and verdict logic
- [`src/atmoslens/exposure.py`](src/atmoslens/exposure.py): route sampling and departure-time exposure ranking
- [`src/atmoslens/recommendations.py`](src/atmoslens/recommendations.py): user-facing recommendation assembly
- [`src/atmoslens/plotting.py`](src/atmoslens/plotting.py): GeoViews / HoloViews / hvPlot visual layer
- [`src/atmoslens/lumen_support.py`](src/atmoslens/lumen_support.py): Lumen `Pipeline` helpers over AtmosLens outputs
- [`src/atmoslens/state.py`](src/atmoslens/state.py): Param-based reactive state
- [`src/atmoslens/views.py`](src/atmoslens/views.py): app layout and cards
- [`src/atmoslens/xarray_source.py`](src/atmoslens/xarray_source.py): `AtmosXarraySource(lumen.sources.Source)` — GSoC prototype XarraySource
- [`src/atmoslens/sql_bridge.py`](src/atmoslens/sql_bridge.py): DuckDB SQL querying on xarray slices (xarray-sql integration demo)
- [`src/atmoslens/lumen_bridge.py`](src/atmoslens/lumen_bridge.py): xarray schema introspection and query serialisation
- [`notebooks/`](notebooks): exploration, logic validation, bridge prototyping
- [`tests/`](tests): focused tests around thresholds, recommendations, exposure, and bridge serialization

## Tests

```bash
.venv/bin/pytest
```

Current local status:

- `250 passed` on Python `3.12.12`
- includes 26 dedicated tests for `AtmosXarraySource` and `sql_bridge` (Lumen Source subclass, DuckDB queries, xarray pipeline)
- app object verified by importing `build_app()` and constructing the `FastListTemplate`
- end-to-end smoke test: search, fetch, activity, route, matrix, bridge, lumen pipelines, map frame, XarraySource queries

## Demo framing for HoloViz / GSoC

AtmosLens is not trying to be a complete air-quality platform. It is a convincing HoloViz artifact that shows:

- real xarray-backed scientific data (live Open-Meteo forecast, NetCDF, `time × lat × lon` grid)
- visible use of the full HoloViz ecosystem: Panel, HoloViews, GeoViews, hvPlot, Datashader, Lumen, Param, Colorcet
- a non-trivial decision layer: health profiles, activity types, threshold scoring, route exposure ranking
- scenario-level reasoning across multiple health profiles and activities at the same location
- **a working `AtmosXarraySource(lumen.sources.Source)` prototype** — the exact first step of the GSoC project
- **DuckDB SQL querying on xarray slices** — the xarray-sql integration the GSoC spec describes
- **explicit pipeline steps** rendered visually in the Lumen Bridge tab
