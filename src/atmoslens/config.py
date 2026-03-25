from __future__ import annotations

APP_NAME = "AtmosLens"
APP_TAGLINE = "Air Quality Decision Copilot"
APP_DESCRIPTION = (
    "A HoloViz ecosystem application built using the libraries surfaced through the holoviz/holoviz "
    "umbrella repository, with implementation centered on Lumen, Panel, HoloViews, GeoViews, hvPlot, "
    "Datashader, Param, and Colorcet over xarray-backed air-quality data."
)

HOLOVIZ_UMBRELLA_REPO = "https://github.com/holoviz/holoviz"
HOLOVIZ_GSOC_WIKI = "https://github.com/holoviz/holoviz/wiki/2026-GSoC-Project-List"
GSOC_TIMELINE_URL = "https://developers.google.com/open-source/gsoc/timeline"

DEFAULT_REGION_PRESET = "Dublin Metro"
DEFAULT_LOCATION_PRESET = "Dublin Docklands"
DEFAULT_ROUTE_PRESET = "Sandyford to Docklands"

DEFAULT_PROFILE = "Sensitive"
DEFAULT_ACTIVITY = "Run"
DEFAULT_POLLUTANT = "ozone"
DEFAULT_ADVISOR_MODE = "Any hour in horizon"
DEFAULT_HORIZON_HOURS = 24

PROFILE_OPTIONS = ["General", "Sensitive", "Asthma", "Outdoor Worker"]
ACTIVITY_OPTIONS = ["Run", "Walk", "Ventilate", "Cycle Commute", "Outdoor Dining", "Children's Play", "Dog Walk"]
ADVISOR_MODES = ["Any hour in horizon", "Morning", "Afternoon", "Evening", "Overnight"]
HORIZON_OPTIONS = [12, 24, 36, 48]

MAX_COMMUTE_DISTANCE_KM = 160.0
