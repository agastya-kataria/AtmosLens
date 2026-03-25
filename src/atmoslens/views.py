from __future__ import annotations

import json

import panel as pn

from atmoslens.datasets import DEFAULT_ROUTES
from atmoslens.plotting import build_pollution_map, build_route_plot, build_timeline_plot
from atmoslens.profiles import pollutant_meta
from atmoslens.state import AtmosLensState

pn.extension(
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
"""

if APP_CSS not in pn.config.raw_css:
    pn.config.raw_css.append(APP_CSS)


def _card_html(title: str, body: str, *, accent: str, eyebrow: str) -> str:
    return f"""
    <div style="
        background: linear-gradient(160deg, rgba(255,255,255,0.96), rgba(255,248,240,0.92));
        border: 1px solid rgba(15,23,42,0.08);
        border-left: 6px solid {accent};
        border-radius: 18px;
        box-shadow: 0 14px 36px rgba(15, 23, 42, 0.08);
        padding: 1.1rem 1.2rem;
    ">
      <div style="font-size:0.72rem; letter-spacing:0.12em; text-transform:uppercase; color:#64748b; margin-bottom:0.35rem;">{eyebrow}</div>
      <div style="font-size:1.1rem; font-weight:700; color:#0f172a; margin-bottom:0.45rem;">{title}</div>
      <div style="font-size:0.95rem; line-height:1.5; color:#334155;">{body}</div>
    </div>
    """


def render_recommendation_card(state: AtmosLensState):
    result = state.activity_result()
    verdict_colors = {"Good": "#0f766e", "Caution": "#d97706", "Avoid": "#dc2626"}
    meta = pollutant_meta(state.pollutant)
    body = (
        f"<div style='display:inline-block; margin-bottom:0.5rem; padding:0.25rem 0.65rem; "
        f"border-radius:999px; background:{verdict_colors[result.recommendation.verdict]}; color:white; "
        f"font-weight:700;'>{result.recommendation.verdict}</div>"
        f"<p style='margin:0 0 0.75rem 0;'>{result.recommendation.explanation}</p>"
        f"<div><strong>Best window:</strong> {result.recommendation.best_window_label}</div>"
        f"<div><strong>Current {meta['label']}:</strong> {result.recommendation.current_value:.1f} {result.recommendation.unit}</div>"
    )
    return pn.pane.HTML(
        _card_html(
            result.recommendation.headline,
            body,
            accent=verdict_colors[result.recommendation.verdict],
            eyebrow="Activity Safety Advisor",
        ),
        min_height=250,
    )


def render_snapshot_cards(state: AtmosLensState):
    activity = state.activity_result()
    route = state.route_result()
    meta = pollutant_meta(state.pollutant)
    timestamp = state.current_timestamp()
    cards = [
        pn.pane.HTML(
            _card_html(
                f"{state.location} at {timestamp:%H:%M}",
                f"<strong>{activity.recommendation.current_value:.1f} {meta['unit']}</strong> projected {meta['label']}.",
                accent="#0f766e",
                eyebrow="Map Snapshot",
            ),
            min_height=130,
        ),
        pn.pane.HTML(
            _card_html(
                route.recommendation.headline,
                f"{route.recommendation.best_window_label}<br>{route.recommendation.explanation}",
                accent="#0f172a",
                eyebrow="Commute Exposure",
            ),
            min_height=130,
        ),
    ]
    return pn.Row(*cards, sizing_mode="stretch_width")


def render_map_panel(state: AtmosLensState):
    plot = build_pollution_map(
        state.current_map_frame(),
        state.pollutant,
        state.current_timestamp(),
        selected_location=state.location,
        route=DEFAULT_ROUTES[state.route],
    )
    note = pn.pane.Markdown(
        (
            f"**Spatial view.** GeoViews + hvPlot render the gridded xarray slice for "
            f"`{state.pollutant}` at `{state.current_timestamp():%Y-%m-%d %H:%M}`. "
            f"The highlighted route makes the commute feature visibly tied to the same cube."
        ),
        css_classes=["atmoslens-note"],
    )
    return pn.Column(note, plot)


def render_timeline_panel(state: AtmosLensState):
    result = state.activity_result()
    plot = build_timeline_plot(result, state.pollutant, state.profile, state.activity)
    note = pn.pane.Markdown(
        (
            f"**Temporal view.** HoloViews overlays the forecast curve, threshold bands, and "
            f"the best `{state.activity}` window that drives the advisor card."
        ),
        css_classes=["atmoslens-note"],
    )
    return pn.Column(note, plot)


def render_commute_panel(state: AtmosLensState):
    result = state.route_result()
    plot = build_route_plot(result, state.pollutant, state.profile, "Cycle Commute")
    note = pn.pane.Markdown(
        (
            f"**Route feature.** AtmosLens samples `{state.route}` against the gridded forecast "
            f"and scores every departure hour, rather than just showing an AQI label."
        ),
        css_classes=["atmoslens-note"],
    )
    return pn.Column(note, plot)


def render_bridge_panel(state: AtmosLensState):
    schema = state.bridge_schema()
    query_spec = state.bridge_query_spec()
    explanation = pn.pane.Markdown(
        """
        **Why this points upstream to Lumen**

        The dataset is xarray-native, but the application logic is already expressed as explicit transform steps:
        `select_location`, `select_time_range`, `aggregate_hourly_windows`, `score_exposure`, `recommend_activity`.
        That is the exact shape of an `XarraySource` + transform pipeline discussion in Lumen.
        """,
        css_classes=["atmoslens-note"],
    )
    return pn.Column(
        explanation,
        pn.Row(
            pn.pane.JSON(schema, depth=3),
            pn.pane.JSON(query_spec, depth=3),
        ),
        pn.pane.Markdown(
            f"```json\n{json.dumps(query_spec, indent=2, default=str)}\n```",
            height=340,
            css_classes=["atmoslens-note"],
        ),
    )


def build_sidebar(state: AtmosLensState):
    controls = pn.Param(
        state,
        parameters=["location", "profile", "activity", "pollutant", "advisor_mode", "route", "map_hour_index"],
        widgets={
            "location": pn.widgets.Select,
            "profile": pn.widgets.RadioButtonGroup,
            "activity": pn.widgets.Select,
            "pollutant": pn.widgets.Select,
            "advisor_mode": pn.widgets.Select,
            "route": pn.widgets.Select,
            "map_hour_index": pn.widgets.IntSlider,
        },
        show_name=False,
    )
    timestamp = pn.bind(
        lambda _: pn.pane.Markdown(
            f"**Map time**\n\n`{state.current_timestamp():%Y-%m-%d %H:%M}`",
            css_classes=["atmoslens-note"],
        ),
        state.param.map_hour_index,
    )
    return pn.Column(
        pn.pane.Markdown(
            """
            ## AtmosLens

            Air quality decision copilot for the next 24 hours.

            Pick a location, health profile, activity, pollutant, and route. AtmosLens turns
            the same xarray cube into a recommendation card, a geospatial map, a timeline, and
            a route-exposure view.
            """,
            css_classes=["atmoslens-note"],
        ),
        controls,
        timestamp,
    )


def build_app(state: AtmosLensState | None = None):
    state = state or AtmosLensState()
    template = pn.template.FastListTemplate(
        title="AtmosLens",
        accent_base_color="#0f766e",
        header_background="#0f172a",
        theme_toggle=False,
        sidebar_width=360,
    )
    template.sidebar.append(build_sidebar(state))

    recommendation = pn.bind(
        lambda *_: render_recommendation_card(state),
        state.param.location,
        state.param.profile,
        state.param.activity,
        state.param.pollutant,
        state.param.advisor_mode,
    )
    snapshots = pn.bind(
        lambda *_: render_snapshot_cards(state),
        state.param.location,
        state.param.route,
        state.param.profile,
        state.param.activity,
        state.param.pollutant,
        state.param.map_hour_index,
    )
    map_panel = pn.bind(
        lambda *_: render_map_panel(state),
        state.param.location,
        state.param.route,
        state.param.pollutant,
        state.param.map_hour_index,
    )
    timeline_panel = pn.bind(
        lambda *_: render_timeline_panel(state),
        state.param.location,
        state.param.profile,
        state.param.activity,
        state.param.pollutant,
        state.param.advisor_mode,
    )
    commute_panel = pn.bind(
        lambda *_: render_commute_panel(state),
        state.param.route,
        state.param.profile,
        state.param.pollutant,
    )
    bridge_panel = pn.bind(
        lambda *_: render_bridge_panel(state),
        state.param.location,
        state.param.profile,
        state.param.activity,
        state.param.pollutant,
        state.param.advisor_mode,
    )

    intro = pn.pane.Markdown(
        """
        # AtmosLens — Air Quality Decision Copilot

        A HoloViz app that turns **xarray-backed air-quality forecasts** into personal decisions:
        when to run, when to ventilate, and which commute window minimizes exposure.
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
