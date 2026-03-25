# AtmosLens Engineering Spec

AtmosLens is a HoloViz ecosystem application built using the libraries surfaced through the `holoviz/holoviz` umbrella repository, with implementation centered on Lumen, Panel, HoloViews, GeoViews, hvPlot, Datashader, Param, and Colorcet over xarray-backed air-quality data.

Its strategic purpose is to act as a vertical slice and public proof-of-need for the official HoloViz GSoC 2026 project **Lumen + Xarray Integration**:

- HoloViz umbrella repo: <https://github.com/holoviz/holoviz>
- HoloViz 2026 GSoC project list: <https://github.com/holoviz/holoviz/wiki/2026-GSoC-Project-List>
- GSoC timeline: <https://developers.google.com/open-source/gsoc/timeline>

## March 31 Goal

By the March 31, 2026 contributor deadline, a mentor should be able to:

- open the repo
- understand the problem in under a minute
- run the app in one or two commands
- see visible use of multiple HoloViz libraries from the umbrella repo
- see real xarray-backed gridded scientific data
- understand why the app naturally motivates first-class xarray support in Lumen

## MVP Feature Spine

The spine of AtmosLens is:

1. Activity Safety Advisor
2. Interactive Pollution Map
3. Forecast Timeline
4. Recommendation Card
5. Route Exposure Window
6. Lumen-oriented xarray bridge

Everything else is secondary polish.

## HoloViz Stack

The intended stack is deliberate:

- `Panel` for the app shell and widgets
- `GeoViews` for geographic overlays
- `HoloViews` for composable plot structure
- `hvPlot` for quick interactive plotting from pandas and xarray
- `Datashader` for map rasterization
- `Param` for reactive state
- `Colorcet` for perceptually sane colormaps
- `Lumen` for pipeline-oriented output previews and the upstream framing

## Data and Domain Model

The canonical scientific object is an `xarray.Dataset` with dimensions:

- `time`
- `lat`
- `lon`

Supported variables currently include:

- `pm2_5`
- `nitrogen_dioxide`
- `ozone`
- `european_aqi`

Application logic stays out of plotting code and is modeled through:

- health profiles
- activity types
- threshold adjustment rules
- time-window scoring
- route exposure ranking

## Lumen + xarray Bridge

The architectural point of AtmosLens is not only to show an app, but to make the missing abstraction visible.

`src/atmoslens/lumen_bridge.py` should continue to represent:

- dataset schema introspection
- explicit transform steps
- query-like request structures
- serialized workflow state

That gives a concrete path toward an eventual `XarraySource` in Lumen, matching the first steps named in the official HoloViz GSoC project list.

## Done Definition

AtmosLens is in good shape for the deadline if:

- the app runs locally
- the activity advisor is convincing
- the map, timeline, and route views all use the same xarray cube
- the route feature feels real, not decorative
- the repo tells a clean HoloViz and Lumen+xarray story
- the bridge layer makes the upstream project direction obvious
