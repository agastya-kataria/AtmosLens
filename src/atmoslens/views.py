from __future__ import annotations

import json

import panel as pn

from atmoslens.lumen_support import build_activity_pipeline, build_route_pipeline, pipeline_summary_spec
from atmoslens.plotting import build_pollution_map, build_route_plot, build_timeline_plot
from atmoslens.profiles import pollutant_meta
from atmoslens.state import AtmosLensState

pn.extension(
    "tabulator",
    design="material",
    sizing_mode="stretch_width",
    notifications=True,
)

APP_CSS = """
body {
  background: linear-gradient(180deg, #fffaf0 0%, #f8fafc 55%, #f1f5f9 100%);
}
.atmoslens-note code {
  background: rgba(15, 23, 42, 0.08);
  padding: 0.1rem 0.35rem;
  border-radius: 999px;
}
.atmoslens-card {
  background: linear-gradient(160deg, rgba(255,255,255,0.96), rgba(255,248,240,0.92));
  border: 1px solid rgba(15,23,42,0.08);
  border-radius: 18px;
  box-shadow: 0 14px 36px rgba(15, 23, 42, 0.08);
}
"""

if APP_CSS not in pn.config.raw_css:
    pn.config.raw_css.append(APP_CSS)


def _card_html(title: str, body: str, *, accent: str, eyebrow: str) -> str:
    return f"""
    <div class="atmoslens-card" style="border-left: 6px solid {accent}; padding: 1.1rem 1.2rem;">
      <div style="font-size:0.72rem; letter-spacing:0.12em; text-transform:uppercase; color:#64748b; margin-bottom:0.35rem;">{eyebrow}</div>
      <div style="font-size:1.1rem; font-weight:700; color:#0f172a; margin-bottom:0.45rem;">{title}</div>
      <div style="font-size:0.95rem; line-height:1.5; color:#334155;">{body}</div>
    </div>
    """


def _error_panel(title: str, message: str):
    return pn.pane.Alert(
        f"**{title}**\n\n{message}",
        alert_type="danger",
        sizing_mode="stretch_width",
    )


def _format_value(value: float) -> str:
    magnitude = abs(float(value))
    if magnitude >= 100:
        return f"{value:.0f}"
    if magnitude >= 10:
        return f"{value:.1f}"
    if magnitude >= 1:
        return f"{value:.2f}"
    return f"{value:.3f}"


def render_recommendation_card(state: AtmosLensState):
    try:
        result = state.activity_result()
    except Exception as exc:  # noqa: BLE001
        return _error_panel("Activity Safety Advisor", str(exc))

    verdict_colors = {"Good": "#0f766e", "Caution": "#d97706", "Avoid": "#dc2626"}
    meta = pollutant_meta(state.pollutant)
    body = (
        f"<div style='display:inline-block; margin-bottom:0.5rem; padding:0.25rem 0.65rem; "
        f"border-radius:999px; background:{verdict_colors[result.recommendation.verdict]}; color:white; "
        f"font-weight:700;'>{result.recommendation.verdict}</div>"
        f"<p style='margin:0 0 0.75rem 0;'>{result.recommendation.explanation}</p>"
        f"<div><strong>Decision point:</strong> {state.location_name} ({state.location_lat:.3f}, {state.location_lon:.3f})</div>"
        f"<div><strong>Best window:</strong> {result.recommendation.best_window_label}</div>"
        f"<div><strong>Current {meta['label']}:</strong> {_format_value(result.recommendation.current_value)} {result.recommendation.unit}</div>"
        f"<div><strong>Decision score:</strong> {result.recommendation.score:.1f} / 100</div>"
    )
    return pn.pane.HTML(
        _card_html(
            result.recommendation.headline,
            body,
            accent=verdict_colors[result.recommendation.verdict],
            eyebrow="Activity Safety Advisor",
        ),
        min_height=270,
    )


def render_snapshot_cards(state: AtmosLensState):
    timestamp = state.current_timestamp()
    cards = []

    try:
        activity = state.activity_result()
        meta = pollutant_meta(state.pollutant)
        cards.append(
            pn.pane.HTML(
                _card_html(
                    f"{state.location_name} at {timestamp:%H:%M}",
                    f"<strong>{_format_value(activity.recommendation.current_value)} {meta['unit']}</strong> projected {meta['label']}.",
                    accent="#0f766e",
                    eyebrow="Map Snapshot",
                ),
                min_height=130,
            )
        )
    except Exception as exc:  # noqa: BLE001
        cards.append(_error_panel("Map Snapshot", str(exc)))

    try:
        route = state.route_result()
        cards.append(
            pn.pane.HTML(
                _card_html(
                    route.recommendation.headline,
                    f"{route.recommendation.best_window_label}<br>{route.recommendation.explanation}",
                    accent="#0f172a",
                    eyebrow="Commute Exposure",
                ),
                min_height=130,
            )
        )
    except Exception as exc:  # noqa: BLE001
        cards.append(_error_panel("Commute Exposure", str(exc)))

    return pn.Row(*cards, sizing_mode="stretch_width")


def render_map_panel(state: AtmosLensState):
    try:
        frame = state.current_map_frame()
        plot = build_pollution_map(
            frame,
            state.pollutant,
            state.current_timestamp(),
            location=state.current_location(),
            route=state.current_route(),
        )
        meta = pollutant_meta(state.pollutant)
        slice_min = float(frame.min())
        slice_max = float(frame.max())
        note = pn.pane.Markdown(
            (
                f"**Spatial view.** GeoViews + hvPlot build the geographic layer, Datashader rasterizes the xarray slice, "
                f"and the same cube is sampled again for the route overlay. Current cube: `{state.summary()['region_name']}`. "
                f"Current map slice ranges from `{_format_value(slice_min)}` to `{_format_value(slice_max)}` {meta['unit']}`, "
                f"with the color scale clipped to the 5th-95th percentile so global maps stay readable."
            ),
            css_classes=["atmoslens-note"],
        )
        return pn.Column(note, plot)
    except Exception as exc:  # noqa: BLE001
        return _error_panel("Pollution Map", str(exc))


def render_timeline_panel(state: AtmosLensState):
    try:
        result = state.activity_result()
        plot = build_timeline_plot(result, state.pollutant, state.profile, state.activity)
        note = pn.pane.Markdown(
            (
                f"**Temporal view.** HoloViews overlays the forecast curve, threshold bands, and "
                f"the best `{state.activity}` window across the selected `{state.horizon_hours}`-hour horizon."
            ),
            css_classes=["atmoslens-note"],
        )
        return pn.Column(note, plot)
    except Exception as exc:  # noqa: BLE001
        return _error_panel("Forecast Timeline", str(exc))


def render_commute_panel(state: AtmosLensState):
    try:
        result = state.route_result()
        plot = build_route_plot(result, state.pollutant, state.profile, "Cycle Commute")
        note = pn.pane.Markdown(
            (
                f"**Route feature.** AtmosLens samples `{state.route_name}` from "
                f"`{state.route_start_name}` to `{state.route_end_name}` against the gridded forecast and ranks each departure hour."
            ),
            css_classes=["atmoslens-note"],
        )
        return pn.Column(note, plot)
    except Exception as exc:  # noqa: BLE001
        return _error_panel("Commute Window", str(exc))


def render_bridge_panel(state: AtmosLensState):
    try:
        schema = state.bridge_schema()
        query_spec = state.bridge_query_spec()
        activity_pipeline = build_activity_pipeline(state.activity_result())
        route_pipeline = build_route_pipeline(state.route_result())
    except Exception as exc:  # noqa: BLE001
        return _error_panel("Lumen Bridge", str(exc))

    explanation = pn.pane.Markdown(
        """
        **Why this points upstream to Lumen**

        The dataset is xarray-native, but the application logic is already expressed as explicit transform steps:
        `select_location`, `select_time_range`, `aggregate_hourly_windows`, `score_exposure`, `recommend_activity`.
        That is the exact shape of an `XarraySource` + transform pipeline discussion in Lumen.
        """,
        css_classes=["atmoslens-note"],
    )
    lumen_note = pn.pane.Markdown(
        """
        **Actual Lumen usage inside AtmosLens**

        AtmosLens uses real `lumen.Pipeline` objects backed by `InMemorySource` tables for the activity and route outputs.
        The strategic gap is not "how do I build a pipeline" but "how do I make the xarray cube itself a first-class Lumen source."
        """,
        css_classes=["atmoslens-note"],
    )
    return pn.Column(
        explanation,
        pn.Row(
            pn.pane.JSON(schema, depth=3),
            pn.pane.JSON(query_spec, depth=3),
        ),
        lumen_note,
        pn.Row(
            pn.Column(
                pn.pane.Markdown("**Lumen activity pipeline**", css_classes=["atmoslens-note"]),
                pn.widgets.Tabulator(activity_pipeline.data, disabled=True, pagination="local", page_size=6),
                pn.pane.JSON(pipeline_summary_spec(activity_pipeline), depth=4),
            ),
            pn.Column(
                pn.pane.Markdown("**Lumen route pipeline**", css_classes=["atmoslens-note"]),
                pn.widgets.Tabulator(route_pipeline.data, disabled=True, pagination="local", page_size=6),
                pn.pane.JSON(pipeline_summary_spec(route_pipeline), depth=4),
            ),
        ),
        pn.pane.Markdown(
            f"```json\n{json.dumps(query_spec, indent=2, default=str)}\n```",
            height=340,
            css_classes=["atmoslens-note"],
        ),
    )


def _summary_pane(state: AtmosLensState):
    summary = state.summary()
    return pn.pane.Markdown(
        (
            f"**Loaded cube**\n\n"
            f"- Region: `{summary['region_name']}`\n"
            f"- Target search region: `{state.region_name}`\n"
            f"- Times: `{summary['time_start']}` to `{summary['time_end']}`\n"
            f"- Bounds: lat `{summary['lat_min']:.3f}` to `{summary['lat_max']:.3f}`, "
            f"lon `{summary['lon_min']:.3f}` to `{summary['lon_max']:.3f}`\n"
            f"- Grid: `{summary['dims']['time']} x {summary['dims']['lat']} x {summary['dims']['lon']}`\n"
            f"- Pollutants: `{', '.join(summary['pollutants'])}`\n\n"
            f"**Status**\n\n{state.status_message}"
        ),
        css_classes=["atmoslens-note"],
    )


def build_sidebar(state: AtmosLensState):
    refresh_button = pn.widgets.Button(
        name="Refresh Forecast Cube",
        button_type="primary",
        icon="refresh",
        sizing_mode="stretch_width",
    )

    def _refresh(_):
        try:
            pn.state.notifications.info("Fetching a new xarray forecast cube...")
            state.refresh_dataset()
            pn.state.notifications.success(state.status_message)
        except Exception as exc:  # noqa: BLE001
            state.status_message = f"Refresh failed: {exc}"
            pn.state.notifications.error(state.status_message)

    refresh_button.on_click(_refresh)

    summary = pn.bind(lambda _: _summary_pane(state), state.param.dataset_revision)

    location_search = pn.widgets.TextInput(
        name="Search place",
        placeholder="Type any city, district, or postcode",
    )
    location_search_button = pn.widgets.Button(name="Resolve place", button_type="primary", icon="map-search")
    location_matches = pn.widgets.RadioBoxGroup(name="Location matches", options={}, visible=False)
    location_search_note = pn.pane.Markdown("", css_classes=["atmoslens-note"])
    location_select_guard = {"active": False}

    def _search_location(_):
        try:
            labels = state.search_location(location_search.value)
            location_matches.options = {label: index for index, label in enumerate(labels)}
            location_select_guard["active"] = True
            location_matches.value = 0 if labels else None
            location_select_guard["active"] = False
            location_matches.visible = bool(labels)
            pn.state.notifications.info("Fetching a live forecast cube for the searched place...")
            state.refresh_dataset()
            location_search_note.object = (
                "Top geocoding matches are shown below. The first result is selected automatically, the forecast cube is refreshed immediately, "
                "and you can click another match if the query was ambiguous."
            )
            pn.state.notifications.success(f"Resolved {state.location_name} and loaded a live forecast cube for that area.")
        except Exception as exc:  # noqa: BLE001
            location_select_guard["active"] = False
            location_matches.options = {}
            location_matches.visible = False
            location_search_note.object = f"**Search error**\n\n{exc}"
            pn.state.notifications.error(str(exc))

    def _select_location(event):
        if location_select_guard["active"] or event.new is None:
            return
        try:
            state.apply_location_search_result(int(event.new))
            pn.state.notifications.info(f"Using {state.location_name} as the decision point and refreshing the forecast cube...")
            state.refresh_dataset()
            pn.state.notifications.success(f"Loaded a live forecast cube for {state.location_name}.")
        except Exception as exc:  # noqa: BLE001
            pn.state.notifications.error(str(exc))

    location_search_button.on_click(_search_location)
    location_matches.param.watch(_select_location, "value")

    route_start_search = pn.widgets.TextInput(
        name="Search route start",
        placeholder="Type a commute origin city, district, or postcode",
    )
    route_start_button = pn.widgets.Button(name="Resolve start", button_type="primary", icon="route")
    route_start_matches = pn.widgets.RadioBoxGroup(name="Route start matches", options={}, visible=False)
    route_start_note = pn.pane.Markdown("", css_classes=["atmoslens-note"])
    route_start_select_guard = {"active": False}

    def _search_route_start(_):
        try:
            labels = state.search_route_start(route_start_search.value)
            route_start_matches.options = {label: index for index, label in enumerate(labels)}
            route_start_select_guard["active"] = True
            route_start_matches.value = 0 if labels else None
            route_start_select_guard["active"] = False
            route_start_matches.visible = bool(labels)
            route_start_note.object = (
                "The top match was applied to the route start and selected below. Search the route end next, then load the corridor forecast."
            )
            pn.state.notifications.success(f"Resolved route start as {state.route_start_name}.")
        except Exception as exc:  # noqa: BLE001
            route_start_select_guard["active"] = False
            route_start_matches.options = {}
            route_start_matches.visible = False
            route_start_note.object = f"**Start search error**\n\n{exc}"
            pn.state.notifications.error(str(exc))

    def _select_route_start(event):
        if route_start_select_guard["active"] or event.new is None:
            return
        try:
            state.apply_route_start_search_result(int(event.new))
        except Exception as exc:  # noqa: BLE001
            pn.state.notifications.error(str(exc))

    route_start_button.on_click(_search_route_start)
    route_start_matches.param.watch(_select_route_start, "value")

    route_end_search = pn.widgets.TextInput(
        name="Search route end",
        placeholder="Type a commute destination city, district, or postcode",
    )
    route_end_button = pn.widgets.Button(name="Resolve end", button_type="primary", icon="route-2")
    route_end_matches = pn.widgets.RadioBoxGroup(name="Route end matches", options={}, visible=False)
    route_end_note = pn.pane.Markdown("", css_classes=["atmoslens-note"])
    route_end_select_guard = {"active": False}

    def _search_route_end(_):
        try:
            labels = state.search_route_end(route_end_search.value)
            route_end_matches.options = {label: index for index, label in enumerate(labels)}
            route_end_select_guard["active"] = True
            route_end_matches.value = 0 if labels else None
            route_end_select_guard["active"] = False
            route_end_matches.visible = bool(labels)
            route_end_note.object = (
                "The top match was applied to the route end and selected below. Load the corridor forecast to score departure times on that route."
            )
            pn.state.notifications.success(f"Resolved route end as {state.route_end_name}.")
        except Exception as exc:  # noqa: BLE001
            route_end_select_guard["active"] = False
            route_end_matches.options = {}
            route_end_matches.visible = False
            route_end_note.object = f"**End search error**\n\n{exc}"
            pn.state.notifications.error(str(exc))

    def _select_route_end(event):
        if route_end_select_guard["active"] or event.new is None:
            return
        try:
            state.apply_route_end_search_result(int(event.new))
        except Exception as exc:  # noqa: BLE001
            pn.state.notifications.error(str(exc))

    route_end_button.on_click(_search_route_end)
    route_end_matches.param.watch(_select_route_end, "value")
    route_refresh_button = pn.widgets.Button(
        name="Load Route Corridor Forecast",
        button_type="primary",
        icon="navigation",
        sizing_mode="stretch_width",
    )

    def _refresh_route(_):
        try:
            pn.state.notifications.info("Fetching a corridor forecast for the current route geometry...")
            state.refresh_dataset()
            pn.state.notifications.success(f"Loaded a route corridor forecast for {state.route_name}.")
        except Exception as exc:  # noqa: BLE001
            pn.state.notifications.error(str(exc))

    route_refresh_button.on_click(_refresh_route)

    region_controls = pn.Param(
        state,
        parameters=[
            "region_preset",
            "region_center_lat",
            "region_center_lon",
            "region_lat_span",
            "region_lon_span",
            "forecast_grid_lat",
            "forecast_grid_lon",
            "forecast_domain",
            "forecast_timezone",
        ],
        widgets={
            "region_preset": pn.widgets.Select,
            "region_center_lat": pn.widgets.FloatInput,
            "region_center_lon": pn.widgets.FloatInput,
            "region_lat_span": pn.widgets.FloatInput,
            "region_lon_span": pn.widgets.FloatInput,
            "forecast_grid_lat": pn.widgets.IntSlider,
            "forecast_grid_lon": pn.widgets.IntSlider,
            "forecast_domain": pn.widgets.Select,
            "forecast_timezone": pn.widgets.TextInput,
        },
        show_name=False,
    )

    location_controls = pn.Param(
        state,
        parameters=["location_name", "location_lat", "location_lon"],
        widgets={
            "location_name": pn.widgets.TextInput,
            "location_lat": pn.widgets.FloatInput,
            "location_lon": pn.widgets.FloatInput,
        },
        show_name=False,
    )

    route_controls = pn.Param(
        state,
        parameters=[
            "route_name",
            "route_start_name",
            "route_start_lat",
            "route_start_lon",
            "route_end_name",
            "route_end_lat",
            "route_end_lon",
            "route_duration_minutes",
        ],
        widgets={
            "route_name": pn.widgets.TextInput,
            "route_start_name": pn.widgets.TextInput,
            "route_start_lat": pn.widgets.FloatInput,
            "route_start_lon": pn.widgets.FloatInput,
            "route_end_name": pn.widgets.TextInput,
            "route_end_lat": pn.widgets.FloatInput,
            "route_end_lon": pn.widgets.FloatInput,
            "route_duration_minutes": pn.widgets.IntSlider,
        },
        show_name=False,
    )

    analysis_controls = pn.Param(
        state,
        parameters=["profile", "activity", "pollutant", "advisor_mode", "horizon_hours", "map_hour_index"],
        widgets={
            "profile": pn.widgets.RadioButtonGroup,
            "activity": pn.widgets.Select,
            "pollutant": pn.widgets.Select,
            "advisor_mode": pn.widgets.Select,
            "horizon_hours": pn.widgets.RadioButtonGroup,
            "map_hour_index": pn.widgets.IntSlider,
        },
        show_name=False,
    )

    guidance = pn.pane.Markdown(
        """
        ## AtmosLens

        Type a place into the search bars below to resolve a decision point or commute anywhere in the world.
        AtmosLens geocodes the search, recenters the forecast region, and can load a new xarray-backed forecast cube for that area.
        """,
        css_classes=["atmoslens-note"],
    )

    return pn.Column(
        guidance,
        pn.Card(region_controls, refresh_button, title="Forecast Region", collapsed=False),
        pn.Card(
            pn.Column(location_search, location_search_button, location_matches, location_search_note, location_controls),
            title="Decision Point Search",
            collapsed=False,
        ),
        pn.Card(
            pn.Column(
                route_start_search,
                route_start_button,
                route_start_matches,
                route_start_note,
                route_end_search,
                route_end_button,
                route_end_matches,
                route_end_note,
                route_refresh_button,
                route_controls,
            ),
            title="Commute Route Search",
            collapsed=True,
        ),
        pn.Card(analysis_controls, title="Decision Controls", collapsed=True),
        summary,
    )


def build_app(state: AtmosLensState | None = None):
    state = state or AtmosLensState()
    template = pn.template.FastListTemplate(
        title="AtmosLens",
        accent_base_color="#0f766e",
        header_background="#0f172a",
        theme_toggle=False,
        sidebar_width=380,
    )
    template.sidebar.append(build_sidebar(state))

    recommendation = pn.bind(
        lambda *_: render_recommendation_card(state),
        state.param.location_name,
        state.param.location_lat,
        state.param.location_lon,
        state.param.profile,
        state.param.activity,
        state.param.pollutant,
        state.param.advisor_mode,
        state.param.horizon_hours,
        state.param.dataset_revision,
    )
    snapshots = pn.bind(
        lambda *_: render_snapshot_cards(state),
        state.param.location_name,
        state.param.location_lat,
        state.param.location_lon,
        state.param.route_name,
        state.param.route_start_lat,
        state.param.route_start_lon,
        state.param.route_end_lat,
        state.param.route_end_lon,
        state.param.profile,
        state.param.activity,
        state.param.pollutant,
        state.param.map_hour_index,
        state.param.dataset_revision,
    )
    map_panel = pn.bind(
        lambda *_: render_map_panel(state),
        state.param.location_name,
        state.param.location_lat,
        state.param.location_lon,
        state.param.route_name,
        state.param.route_start_lat,
        state.param.route_start_lon,
        state.param.route_end_lat,
        state.param.route_end_lon,
        state.param.pollutant,
        state.param.map_hour_index,
        state.param.dataset_revision,
    )
    timeline_panel = pn.bind(
        lambda *_: render_timeline_panel(state),
        state.param.location_name,
        state.param.location_lat,
        state.param.location_lon,
        state.param.profile,
        state.param.activity,
        state.param.pollutant,
        state.param.advisor_mode,
        state.param.horizon_hours,
        state.param.dataset_revision,
    )
    commute_panel = pn.bind(
        lambda *_: render_commute_panel(state),
        state.param.route_name,
        state.param.route_start_lat,
        state.param.route_start_lon,
        state.param.route_end_lat,
        state.param.route_end_lon,
        state.param.route_duration_minutes,
        state.param.profile,
        state.param.pollutant,
        state.param.horizon_hours,
        state.param.dataset_revision,
    )
    bridge_panel = pn.bind(
        lambda *_: render_bridge_panel(state),
        state.param.location_name,
        state.param.location_lat,
        state.param.location_lon,
        state.param.route_name,
        state.param.route_start_lat,
        state.param.route_start_lon,
        state.param.route_end_lat,
        state.param.route_end_lon,
        state.param.profile,
        state.param.activity,
        state.param.pollutant,
        state.param.advisor_mode,
        state.param.horizon_hours,
        state.param.dataset_revision,
    )

    intro = pn.pane.Markdown(
        """
        # AtmosLens — Air Quality Decision Copilot

        A HoloViz app that turns **xarray-backed air-quality forecasts** into personal decisions:
        when to run, when to ventilate, and which commute window minimizes exposure.

        The app now supports global presets plus fully editable coordinates, route endpoints, and region refreshes,
        and it now resolves places from search bars instead of forcing users through fixed dropdowns.
        """,
        css_classes=["atmoslens-note"],
    )

    template.main.extend(
        [
            intro,
            pn.Row(recommendation, sizing_mode="stretch_width"),
            snapshots,
            pn.Tabs(
                ("Pollution Map", map_panel),
                ("Forecast Timeline", timeline_panel),
                ("Commute Window", commute_panel),
                ("Lumen Bridge", bridge_panel),
                dynamic=True,
            ),
        ]
    )
    return template
