from __future__ import annotations

import json

import panel as pn

from atmoslens.config import APP_DESCRIPTION, APP_NAME, APP_TAGLINE, HOLOVIZ_GSOC_WIKI, HOLOVIZ_UMBRELLA_REPO
from atmoslens.lumen_support import build_activity_pipeline, build_route_pipeline, pipeline_summary_spec
from atmoslens.plotting import build_pollution_map, build_route_plot, build_scenario_matrix_plot, build_timeline_plot
from atmoslens.profiles import pollutant_meta
from atmoslens.scoring import score_interpretation, who_guideline_note
from atmoslens.state import AtmosLensState

pn.extension(
    "tabulator",
    design="material",
    sizing_mode="stretch_width",
    notifications=True,
)

APP_CSS = """
body {
  background: linear-gradient(180deg, #f8fffe 0%, #f8fafc 55%, #f1f5f9 100%);
}
.bk-Column.atmoslens-sidebar > div {
  gap: 10px;
}
.atmoslens-note code {
  background: rgba(15, 23, 42, 0.07);
  padding: 0.1rem 0.35rem;
  border-radius: 999px;
  font-size: 0.88em;
}
.atmoslens-card {
  background: linear-gradient(160deg, rgba(255,255,255,0.97), rgba(255,250,244,0.93));
  border: 1px solid rgba(15,23,42,0.07);
  border-radius: 16px;
  box-shadow: 0 8px 28px rgba(15, 23, 42, 0.06);
  transition: transform 140ms ease, box-shadow 140ms ease;
}
.atmoslens-card:hover {
  transform: translateY(-1px);
  box-shadow: 0 12px 36px rgba(15, 23, 42, 0.10);
}
.atmoslens-hero {
  background: linear-gradient(135deg, rgba(15,118,110,0.08), rgba(217,119,6,0.06));
  border: 1px solid rgba(15,23,42,0.06);
  border-radius: 18px;
  padding: 1rem 1.15rem;
  box-shadow: 0 6px 20px rgba(15, 23, 42, 0.05);
}
.atmoslens-hero strong {
  color: #0f172a;
}
.atmoslens-kicker {
  text-transform: uppercase;
  letter-spacing: 0.12em;
  font-size: 0.7rem;
  color: #64748b;
  margin-bottom: 0.25rem;
}
.atmoslens-guidance {
  background: linear-gradient(135deg, rgba(15,118,110,0.06), rgba(15,118,110,0.02));
  border-left: 3px solid rgba(15,118,110,0.3);
  border-radius: 0 10px 10px 0;
  padding: 8px 12px;
  margin: 8px 0;
  font-size: 0.88rem;
  line-height: 1.5;
  color: #334155;
}
.atmoslens-score-bar {
  height: 6px;
  border-radius: 3px;
  background: #e2e8f0;
  overflow: hidden;
  margin: 6px 0 2px 0;
}
.atmoslens-score-fill {
  height: 100%;
  border-radius: 3px;
  transition: width 300ms ease;
}
"""

if APP_CSS not in pn.config.raw_css:
    pn.config.raw_css.append(APP_CSS)


def _notify(level: str, message: str) -> None:
    notifications = getattr(pn.state, "notifications", None)
    if notifications is None:
        return
    handler = getattr(notifications, level, None)
    if handler is not None:
        try:
            handler(message)
        except Exception:
            pass


def _card_html(title: str, body: str, *, accent: str, eyebrow: str) -> str:
    return f"""
    <div class="atmoslens-card" style="border-left: 6px solid {accent}; padding: 1.1rem 1.2rem;">
      <div class="atmoslens-kicker">{eyebrow}</div>
      <div style="font-size:1.05rem; font-weight:700; color:#0f172a; margin-bottom:0.4rem;">{title}</div>
      <div style="font-size:0.92rem; line-height:1.5; color:#334155;">{body}</div>
    </div>
    """


def _hero_html(title: str, body: str) -> str:
    return f"""
    <div class="atmoslens-hero">
      <div class="atmoslens-kicker">Global Search</div>
      <div style="font-size:1.08rem; font-weight:700; color:#0f172a; margin-bottom:0.35rem;">{title}</div>
      <div style="font-size:0.9rem; line-height:1.5; color:#334155;">{body}</div>
    </div>
    """


def _workaround_html() -> str:
    return (
        "Workaround: wait 5–10 seconds and try again, or open Professional Controls, "
        "set a smaller grid like 7 × 9, then click Refresh Forecast Cube."
    )


def _error_panel(title: str, message: str):
    return pn.pane.Alert(
        f"**{title}**\n\n{message}",
        alert_type="danger",
        sizing_mode="stretch_width",
    )


def _busy_panel(title: str, message: str):
    spinner = pn.indicators.LoadingSpinner(value=True, width=28, height=28, color="primary")
    text = pn.pane.Markdown(
        f"**{title}**\n\n{message}",
        sizing_mode="stretch_width",
    )
    return pn.Row(spinner, text, sizing_mode="stretch_width")


def _state_error_panel(title: str, state: AtmosLensState, message: str):
    op = state.operational_status()
    if state.busy:
        return _busy_panel(title, state.busy_message or "Refreshing the xarray-backed forecast cube for the current selection.")
    if not op["cube_matches_target"]:
        return pn.pane.Alert(
            (
                f"**{title}**\n\n"
                f"The selection has moved to **{op['target_region']}**, but the loaded cube is still **{op['loaded_region']}**. "
                f"AtmosLens needs to refresh the xarray forecast cube for the new area before it can score this view.\n\n"
                f"{_workaround_html()}"
            ),
            alert_type="warning",
            sizing_mode="stretch_width",
        )
    if not op["location_ready"] and ("Selected location" in message or title in {"Activity Safety Advisor", "Forecast Timeline", "Lumen Bridge"}):
        return pn.pane.Alert(
            (
                f"**{title}**\n\n"
                f"The loaded cube already matches **{op['loaded_region']}**, but the decision point is outside its bounds. "
                f"Search again, edit the point, or refresh a forecast cube centered on the selected place.\n\n"
                f"{_workaround_html()}"
            ),
            alert_type="warning",
            sizing_mode="stretch_width",
        )
    if not op["route_commute_ready"]:
        return pn.pane.Alert(
            (
                f"**{title}**\n\n"
                f"The current route spans about **{op['route_distance_km']:.0f} km**, which is too large for AtmosLens' commute advisor. "
                f"Choose a destination within roughly **160 km** or analyze the destination as a place instead."
            ),
            alert_type="warning",
            sizing_mode="stretch_width",
        )
    if not op["route_ready"] and title in {"Map Snapshot", "Commute Exposure", "Commute Window"}:
        return pn.pane.Alert(
            (
                f"**{title}**\n\n"
                f"The loaded cube matches **{op['loaded_region']}**, but part of the commute corridor still falls outside it. "
                f"Refresh a corridor cube or tighten the route endpoints."
            ),
            alert_type="warning",
            sizing_mode="stretch_width",
        )
    return _error_panel(title, message)


def _format_value(value: float) -> str:
    magnitude = abs(float(value))
    if magnitude >= 100:
        return f"{value:.0f}"
    if magnitude >= 10:
        return f"{value:.1f}"
    if magnitude >= 1:
        return f"{value:.2f}"
    return f"{value:.3f}"


def _score_bar_html(score: float, label: str) -> str:
    """Render a thin score bar with colour gradient."""
    if score <= 35:
        fill_color = "#0f766e"
    elif score <= 70:
        fill_color = "#d97706"
    else:
        fill_color = "#dc2626"
    pct = min(100, max(2, score))
    return (
        f'<div style="font-size:0.82rem; color:#64748b;">{label} — {score:.0f}/100</div>'
        f'<div class="atmoslens-score-bar">'
        f'<div class="atmoslens-score-fill" style="width:{pct}%; background:{fill_color};"></div>'
        f'</div>'
    )


def render_recommendation_card(state: AtmosLensState):
    try:
        result = state.activity_result()
    except Exception as exc:  # noqa: BLE001
        return _state_error_panel("Activity Safety Advisor", state, str(exc))

    rec = result.recommendation
    verdict_colors = {"Good": "#0f766e", "Caution": "#d97706", "Avoid": "#dc2626"}
    accent = verdict_colors.get(rec.verdict, "#64748b")
    meta = pollutant_meta(state.pollutant)
    forecast_timestamp = state.localize_timestamp(state.current_timestamp())

    verdict_badge = (
        f"<div style='display:flex; align-items:center; gap:10px; margin-bottom:10px;'>"
        f"<span style='display:inline-block; padding:5px 14px; border-radius:999px; "
        f"background:{accent}; color:white; font-weight:700; font-size:1rem;'>{rec.verdict}</span>"
        f"<span style='font-size:1.05rem; font-weight:700; color:#0f172a;'>{rec.headline}</span>"
        f"</div>"
    )
    guidance_block = ""
    if rec.health_guidance:
        guidance_block = f'<div class="atmoslens-guidance">{rec.health_guidance}</div>'
    who_block = ""
    if rec.who_guideline:
        who_block = f'<div style="font-size:0.8rem; color:#94a3b8; margin-top:6px;">{rec.who_guideline}</div>'
    score_bar = _score_bar_html(rec.score, rec.score_label or score_interpretation(rec.score))

    details = (
        f"<div style='display:grid; grid-template-columns:1fr 1fr; gap:6px 16px; font-size:0.88rem; margin-top:8px;'>"
        f"<div><strong>Local time:</strong> {state.current_local_time():%a %d %b %H:%M}</div>"
        f"<div><strong>Timezone:</strong> {state.forecast_timezone}</div>"
        f"<div><strong>Forecast hour:</strong> {forecast_timestamp:%a %d %b %H:%M}</div>"
        f"<div><strong>Decision point:</strong> {state.location_name}</div>"
        f"<div><strong>Current {meta['label']}:</strong> {_format_value(rec.current_value)} {rec.unit}</div>"
        f"<div><strong>Best window:</strong> {rec.best_window_label}</div>"
        f"<div><strong>Profile:</strong> {state.profile} · {state.activity}</div>"
        f"<div><strong>Pollutant:</strong> {meta['label']} · {state.horizon_hours}h horizon</div>"
        f"</div>"
    )

    body = f"{verdict_badge}{guidance_block}{score_bar}{details}{who_block}"

    return pn.pane.HTML(
        _card_html(
            rec.headline,
            body,
            accent=accent,
            eyebrow="Activity Safety Advisor",
        ),
        min_height=300,
    )


def render_snapshot_cards(state: AtmosLensState):
    timestamp = state.localize_timestamp(state.current_timestamp())
    cards = []
    op = state.operational_status()

    try:
        activity = state.activity_result()
        meta = pollutant_meta(state.pollutant)
        rec = activity.recommendation
        snap_body = (
            f"<strong>{_format_value(rec.current_value)} {meta['unit']}</strong> projected {meta['label']}."
            f"<br><span style='font-size:0.82rem; color:#64748b;'>"
            f"Score: {rec.score:.0f}/100 ({rec.score_label or score_interpretation(rec.score)})</span>"
        )
        cards.append(
            pn.pane.HTML(
                _card_html(
                    f"{state.location_name} — {timestamp:%H:%M} local",
                    snap_body,
                    accent="#0f766e",
                    eyebrow="Current Conditions",
                ),
                min_height=130,
            )
        )
    except Exception as exc:  # noqa: BLE001
        cards.append(_state_error_panel("Map Snapshot", state, str(exc)))

    try:
        route = state.route_result()
        route_rec = route.recommendation
        route_body = (
            f"{route_rec.best_window_label}<br>{route_rec.headline}"
            f"<br><span style='font-size:0.82rem; color:#64748b;'>"
            f"Score: {route_rec.score:.0f}/100 ({route_rec.score_label or score_interpretation(route_rec.score)})</span>"
        )
        cards.append(
            pn.pane.HTML(
                _card_html(
                    route_rec.headline,
                    route_body,
                    accent="#0f172a",
                    eyebrow="Commute Exposure",
                ),
                min_height=130,
            )
        )
    except Exception as exc:  # noqa: BLE001
        cards.append(_state_error_panel("Commute Exposure", state, str(exc)))

    readiness_color = "#0f766e" if op["ready"] else "#d97706"
    readiness_title = "Forecast Ready" if op["ready"] else "Refresh Recommended"
    shift_line = ""
    if not op["cube_matches_target"] or not op["location_ready"]:
        shift_line = f"<div><strong>Selection shift:</strong> {op['selection_shift_km']:.0f} km from loaded cube centre</div>"
    readiness_body = (
        f"<div><strong>Loaded cube:</strong> {op['loaded_region']}</div>"
        f"<div><strong>Target region:</strong> {op['target_region']}</div>"
        f"<div><strong>Location in bounds:</strong> {'Yes' if op['location_ready'] else 'No'}</div>"
        f"<div><strong>Route in bounds:</strong> {'Yes' if op['route_ready'] else 'No'}</div>"
        f"{shift_line}"
        f"<div><strong>Commute corridor:</strong> {op['route_distance_km']:.0f} km</div>"
    )
    cards.append(
        pn.pane.HTML(
            _card_html(
                readiness_title,
                readiness_body,
                accent=readiness_color,
                eyebrow="Operational Status",
            ),
            min_height=130,
        )
    )

    return pn.Row(*cards, sizing_mode="stretch_width")


def render_map_panel(state: AtmosLensState):
    try:
        frame = state.current_map_frame()
        op = state.operational_status()
        plot = build_pollution_map(
            frame,
            state.pollutant,
            state.localize_timestamp(state.current_timestamp()),
            location=state.current_location(),
            timezone_label=state.forecast_timezone,
            route=state.current_route() if op["route_ready"] and op["route_commute_ready"] else None,
        )
        meta = pollutant_meta(state.pollutant)
        slice_min = float(frame.min())
        slice_max = float(frame.max())
        note = pn.pane.Markdown(
            (
                f"**Spatial view.** `GeoViews` builds the geographic overlay with Carto tiles, `HoloViews` renders the "
                f"xarray slice as a `QuadMesh` with a pollutant-specific green-to-red risk ramp from `Colorcet`, "
                f"and the commute corridor is overlaid only when the route falls within the active cube. "
                f"Current cube: `{state.summary()['region_name']}`. "
                f"Map hour shown in `{state.forecast_timezone}` local time. "
                f"Slice range: `{_format_value(slice_min)}` to `{_format_value(slice_max)}` {meta['unit']} "
                f"(colour scale clipped to 5th–95th percentile)."
            ),
            css_classes=["atmoslens-note"],
        )
        return pn.Column(note, plot)
    except Exception as exc:  # noqa: BLE001
        return _state_error_panel("Pollution Map", state, str(exc))


def render_timeline_panel(state: AtmosLensState):
    try:
        result = state.activity_result()
        plot = build_timeline_plot(result, state.pollutant, state.profile, state.activity, state.forecast_timezone)
        rec = result.recommendation
        guidance_md = ""
        if rec.health_guidance:
            guidance_md = f"\n\n> {rec.health_guidance}"
        who_md = ""
        if rec.who_guideline:
            who_md = f"\n\n*{rec.who_guideline}*"
        note = pn.pane.Markdown(
            (
                f"**Temporal view.** `HoloViews` overlays the forecast curve, threshold bands (green = Good, "
                f"amber = Caution, red = Avoid), and the best `{state.activity}` window across the `{state.horizon_hours}`h horizon "
                f"for the `{state.profile}` profile. All times in `{state.forecast_timezone}` local time."
                f"{guidance_md}{who_md}"
            ),
            css_classes=["atmoslens-note"],
        )
        return pn.Column(note, plot)
    except Exception as exc:  # noqa: BLE001
        return _state_error_panel("Forecast Timeline", state, str(exc))


def render_commute_panel(state: AtmosLensState):
    try:
        result = state.route_result()
        plot = build_route_plot(result, state.pollutant, state.profile, "Cycle Commute", state.forecast_timezone)
        rec = result.recommendation
        guidance_md = ""
        if rec.health_guidance:
            guidance_md = f"\n\n> {rec.health_guidance}"
        note = pn.pane.Markdown(
            (
                f"**Route feature.** AtmosLens samples `{state.route_name}` from "
                f"`{state.route_start_name}` to `{state.route_end_name}` against the gridded xarray forecast "
                f"and ranks each departure hour in `{state.forecast_timezone}` local time. "
                f"The route is interpolated into 32 sample points along its geometry and each candidate departure "
                f"is scored with a blended 60% mean + 40% peak exposure metric."
                f"{guidance_md}"
            ),
            css_classes=["atmoslens-note"],
        )
        return pn.Column(note, plot)
    except Exception as exc:  # noqa: BLE001
        return _state_error_panel("Commute Window", state, str(exc))


def render_matrix_panel(state: AtmosLensState):
    try:
        matrix = state.scenario_matrix()
    except Exception as exc:  # noqa: BLE001
        return _state_error_panel("Decision Matrix", state, str(exc))

    best = matrix.sort_values("score", ascending=True).iloc[0]
    worst = matrix.sort_values("score", ascending=False).iloc[0]
    plot = build_scenario_matrix_plot(matrix)
    note = pn.pane.Markdown(
        (
            f"**Decision matrix.** AtmosLens scores every health profile × every non-route activity at "
            f"`{state.location_name}` against the same xarray forecast cube. "
            f"Cleanest combination: **{best['profile']} + {best['activity']}** → {best['verdict']} "
            f"(score {best['score']:.0f}, window {best['best_window']}). "
            f"Most exposed: **{worst['profile']} + {worst['activity']}** → {worst['verdict']} "
            f"(score {worst['score']:.0f})."
        ),
        css_classes=["atmoslens-note"],
    )
    ranking = pn.widgets.Tabulator(
        matrix[["profile", "activity", "verdict", "best_window", "score"]],
        disabled=True,
        pagination="local",
        page_size=len(matrix),
    )
    return pn.Row(
        pn.Column(note, plot),
        pn.Column(
            pn.pane.Markdown("**Scenario ranking**", css_classes=["atmoslens-note"]),
            ranking,
            sizing_mode="stretch_width",
        ),
        sizing_mode="stretch_width",
    )


def render_bridge_panel(state: AtmosLensState):
    try:
        schema = state.bridge_schema()
        query_spec = state.bridge_query_spec()
        activity_pipeline = build_activity_pipeline(state.activity_result())
        route_pipeline = build_route_pipeline(state.route_result())
    except Exception as exc:  # noqa: BLE001
        return _state_error_panel("Lumen Bridge", state, str(exc))

    explanation = pn.pane.Markdown(
        """
**Why this points upstream to Lumen**

The dataset is `xarray`-native, but the application logic is already expressed as explicit transform steps:
`select_location` → `select_time_range` → `aggregate_hourly_windows` → `score_exposure` → `recommend_activity`.
That is the exact shape of an `XarraySource` + transform pipeline discussion in Lumen.
The bridge prototype in `lumen_bridge.py` introspects xarray dimensions, coordinates, and variables,
then serialises the analysis as a query-spec-like structure — the start of a native Lumen `XarraySource` story.
        """,
        css_classes=["atmoslens-note"],
    )
    lumen_note = pn.pane.Markdown(
        """
**Actual Lumen usage inside AtmosLens**

AtmosLens uses real `lumen.Pipeline` objects backed by `InMemorySource` tables for the activity and route outputs.
The strategic gap is not "how do I build a pipeline" but "how do I make the xarray cube itself a first-class Lumen source."
That is the upstream contribution this artifact motivates: native `XarraySource` and reusable xarray-native transforms in Lumen.
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
    status_text = state.busy_message if state.busy and state.busy_message else state.status_message
    mode_line = ""
    if summary.get("forecast_mode") == "live_grid":
        mode_line = "- Mode: `Live gridded forecast`\n"
    return pn.pane.Markdown(
        (
            f"**Loaded cube**\n\n"
            f"- Region: `{summary['region_name']}`\n"
            f"- Target search region: `{state.region_name}`\n"
            f"{mode_line}"
            f"- Times (local): `{summary['time_start']}` to `{summary['time_end']}`\n"
            f"- Forecast timezone: `{summary['timezone'] or state.forecast_timezone}`\n"
            f"- Bounds: lat `{summary['lat_min']:.3f}` to `{summary['lat_max']:.3f}`, "
            f"lon `{summary['lon_min']:.3f}` to `{summary['lon_max']:.3f}`\n"
            f"- Grid: `{summary['dims']['time']} × {summary['dims']['lat']} × {summary['dims']['lon']}`\n"
            f"- Pollutants: `{', '.join(summary['pollutants'])}`\n\n"
            f"**Status**\n\n{status_text}"
        ),
        css_classes=["atmoslens-note"],
    )


def _run_with_busy(state: AtmosLensState, message: str, callback):
    state.set_busy(message)
    try:
        return callback()
    finally:
        state.clear_busy()


def _local_time_pane(state: AtmosLensState):
    pane = pn.pane.Markdown("", css_classes=["atmoslens-note"])

    def _update() -> None:
        now = state.current_local_time()
        pane.object = f"**Local time now:** `{now:%a %d %b %H:%M}` in `{state.forecast_timezone}`"

    _update()
    state.param.watch(lambda *_: _update(), "forecast_timezone")
    if pn.state.curdoc is not None:
        pn.state.add_periodic_callback(_update, period=30000)
    return pane


def build_sidebar(state: AtmosLensState):
    refresh_button = pn.widgets.Button(
        name="Refresh Forecast Cube",
        button_type="default",
        icon="refresh",
        sizing_mode="stretch_width",
    )

    def _refresh(_):
        try:
            _notify("info", "Fetching a new xarray forecast cube…")
            _run_with_busy(state, "Refreshing the forecast cube for the active analysis region…", state.refresh_dataset)
            _notify("success", state.status_message)
        except Exception as exc:  # noqa: BLE001
            state.status_message = f"Refresh failed: {exc} {_workaround_html()}"
            _notify("error", state.status_message)

    refresh_button.on_click(_refresh)

    summary = pn.bind(
        lambda *_: _summary_pane(state),
        state.param.dataset_revision,
        state.param.region_preset,
        state.param.region_name,
        state.param.region_center_lat,
        state.param.region_center_lon,
        state.param.forecast_timezone,
        state.param.status_message,
        state.param.busy,
        state.param.busy_message,
    )

    hero = pn.bind(
        lambda *_: pn.pane.HTML(
            _hero_html(
                f"{state.location_name} · {state.profile} · {state.activity}",
                (
                    f"<strong>Target region:</strong> {state.region_name}<br>"
                    f"<strong>Loaded cube:</strong> {state.summary()['region_name']}<br>"
                    f"<strong>Local time:</strong> {state.current_local_time():%a %d %b %H:%M} ({state.forecast_timezone})<br>"
                    f"Type any city, district, or postcode and press <strong>Enter</strong> to refresh."
                ),
            )
        ),
        state.param.location_name,
        state.param.profile,
        state.param.activity,
        state.param.region_name,
        state.param.dataset_revision,
        state.param.busy,
        state.param.forecast_timezone,
    )

    # --- Location search ---
    location_search = pn.widgets.TextInput(
        name="Search place",
        placeholder="Type any city, district, or postcode…",
    )
    location_search_button = pn.widgets.Button(name="Analyze Place", button_type="primary", icon="search")
    location_matches = pn.widgets.Select(name="Other matches", options={}, visible=False)
    location_search_note = pn.pane.Markdown(
        "Search auto-refreshes the forecast cube. Advanced geometry edits stay local until you refresh manually.",
        css_classes=["atmoslens-note"],
    )
    location_select_guard = {"active": False}

    def _set_match_widget(widget, labels: list[str]) -> None:
        widget.options = {label: index for index, label in enumerate(labels)}
        widget.visible = len(labels) > 1

    def _search_location(_=None):
        query = (location_search.value or "").strip()
        if len(query) < 2:
            location_search_note.object = "Type at least two characters."
            return
        try:
            labels = state.search_location_matches(query)
        except Exception as exc:  # noqa: BLE001
            location_select_guard["active"] = False
            _set_match_widget(location_matches, [])
            location_search_note.object = f"**Search error**\n\n{exc}\n\n{_workaround_html()}"
            state.status_message = f"Search failed: {exc}"
            _notify("error", str(exc))
            return

        try:
            _set_match_widget(location_matches, labels)
            location_select_guard["active"] = True
            location_matches.value = 0 if labels else None
            location_select_guard["active"] = False
            _notify("info", "Fetching a live forecast cube for the searched place…")
            _run_with_busy(
                state,
                f"Refreshing the forecast cube for {query}…",
                lambda: state.load_location_search_result(0),
            )
            location_search_note.object = (
                f"Resolved **{state.location_name}**. Region, timezone, and commute route updated automatically."
            )
            _notify("success", f"Loaded a live forecast cube for {state.location_name}.")
        except Exception as exc:  # noqa: BLE001
            location_select_guard["active"] = False
            location_search_note.object = (
                f"**Search paused**\n\n{exc}\n\nThe current forecast cube was kept unchanged.\n\n{_workaround_html()}"
            )
            state.status_message = f"Search paused: {exc}"
            _notify("warning", str(exc))

    def _search_location_enter(event):
        if event.new <= event.old:
            return
        _search_location()

    def _select_location(event):
        if location_select_guard["active"] or event.new is None:
            return
        try:
            _notify("info", "Refreshing the forecast cube for the selected place…")
            _run_with_busy(
                state,
                "Refreshing the forecast cube for the selected place…",
                lambda: state.load_location_search_result(int(event.new)),
            )
            location_search_note.object = (
                f"Resolved **{state.location_name}**. Forecast cube refreshed automatically."
            )
            _notify("success", f"Loaded a live forecast cube for {state.location_name}.")
        except Exception as exc:  # noqa: BLE001
            location_search_note.object = (
                f"**Selection paused**\n\n{exc}\n\nThe current forecast cube was kept unchanged.\n\n{_workaround_html()}"
            )
            state.status_message = f"Selection paused: {exc}"
            _notify("warning", str(exc))

    location_search_button.on_click(_search_location)
    location_search.param.watch(_search_location_enter, "enter_pressed")
    location_matches.param.watch(_select_location, "value")

    # --- Route search ---
    route_start_search = pn.widgets.TextInput(
        name="Search route start",
        placeholder="Origin city, district, or postcode",
    )
    route_start_button = pn.widgets.Button(name="Resolve Start", button_type="primary", icon="route")
    route_start_matches = pn.widgets.Select(name="Start matches", options={}, visible=False)
    route_start_note = pn.pane.Markdown("", css_classes=["atmoslens-note"])
    route_start_select_guard = {"active": False}

    def _search_route_start(_=None):
        query = (route_start_search.value or "").strip()
        if len(query) < 2:
            route_start_note.object = "Type at least two characters."
            return
        try:
            labels = _run_with_busy(
                state,
                "Resolving the route origin…",
                lambda: state.search_route_start(query),
            )
        except Exception as exc:  # noqa: BLE001
            route_start_select_guard["active"] = False
            _set_match_widget(route_start_matches, [])
            route_start_note.object = f"**Start search error**\n\n{exc}"
            state.status_message = f"Route start search failed: {exc}"
            _notify("error", str(exc))
            return

        _set_match_widget(route_start_matches, labels)
        route_start_select_guard["active"] = True
        route_start_matches.value = 0 if labels else None
        route_start_select_guard["active"] = False
        route_start_note.object = (
            f"Using **{state.route_start_name}** as route start. Search the destination next."
        )
        _notify("success", f"Resolved route start: {state.route_start_name}.")

    def _search_route_start_enter(event):
        if event.new <= event.old:
            return
        _search_route_start()

    def _select_route_start(event):
        if route_start_select_guard["active"] or event.new is None:
            return
        try:
            _run_with_busy(
                state,
                "Applying the selected route origin…",
                lambda: state.apply_route_start_search_result(int(event.new)),
            )
            route_start_note.object = f"Using **{state.route_start_name}** as the route start."
        except Exception as exc:  # noqa: BLE001
            route_start_note.object = f"**Start selection error**\n\n{exc}"
            state.status_message = f"Route start selection failed: {exc}"
            _notify("error", str(exc))

    route_start_button.on_click(_search_route_start)
    route_start_search.param.watch(_search_route_start_enter, "enter_pressed")
    route_start_matches.param.watch(_select_route_start, "value")

    route_end_search = pn.widgets.TextInput(
        name="Search route end",
        placeholder="Destination city, district, or postcode",
    )
    route_end_button = pn.widgets.Button(name="Resolve End", button_type="primary", icon="route-2")
    route_end_matches = pn.widgets.Select(name="End matches", options={}, visible=False)
    route_end_note = pn.pane.Markdown("", css_classes=["atmoslens-note"])
    route_end_select_guard = {"active": False}

    def _search_route_end(_=None):
        query = (route_end_search.value or "").strip()
        if len(query) < 2:
            route_end_note.object = "Type at least two characters."
            return
        try:
            labels = state.search_route_end_matches(query)
        except Exception as exc:  # noqa: BLE001
            route_end_select_guard["active"] = False
            _set_match_widget(route_end_matches, [])
            route_end_note.object = f"**End search error**\n\n{exc}\n\n{_workaround_html()}"
            state.status_message = f"Route end search failed: {exc}"
            _notify("error", str(exc))
            return

        try:
            _set_match_widget(route_end_matches, labels)
            route_end_select_guard["active"] = True
            route_end_matches.value = 0 if labels else None
            route_end_select_guard["active"] = False
            _notify("info", "Refreshing the route corridor forecast…")
            _run_with_busy(
                state,
                "Refreshing the forecast cube for the route corridor…",
                lambda: state.load_route_end_search_result(0),
            )
            route_end_note.object = (
                f"Using **{state.route_end_name}** as destination. Corridor forecast refreshed automatically."
            )
            _notify("success", f"Loaded corridor forecast for {state.route_name}.")
        except Exception as exc:  # noqa: BLE001
            route_end_select_guard["active"] = False
            route_end_note.object = (
                f"**End search paused**\n\n{exc}\n\nThe current forecast cube was kept unchanged.\n\n{_workaround_html()}"
            )
            state.status_message = f"Route end search paused: {exc}"
            _notify("warning", str(exc))

    def _search_route_end_enter(event):
        if event.new <= event.old:
            return
        _search_route_end()

    def _select_route_end(event):
        if route_end_select_guard["active"] or event.new is None:
            return
        try:
            _run_with_busy(
                state,
                "Applying the selected route destination…",
                lambda: state.load_route_end_search_result(int(event.new)),
            )
            route_end_note.object = (
                f"Using **{state.route_end_name}** as destination. Corridor forecast refreshed automatically."
            )
            _notify("success", f"Loaded corridor forecast for {state.route_name}.")
        except Exception as exc:  # noqa: BLE001
            route_end_note.object = (
                f"**End selection paused**\n\n{exc}\n\nThe current forecast cube was kept unchanged.\n\n{_workaround_html()}"
            )
            state.status_message = f"Route end selection paused: {exc}"
            _notify("warning", str(exc))

    route_end_button.on_click(_search_route_end)
    route_end_search.param.watch(_search_route_end_enter, "enter_pressed")
    route_end_matches.param.watch(_select_route_end, "value")

    route_refresh_button = pn.widgets.Button(
        name="Load Route Corridor Forecast",
        button_type="default",
        icon="navigation",
        sizing_mode="stretch_width",
    )

    def _refresh_route(_):
        try:
            if not state.route_commute_ready():
                message = (
                    f"The current route spans about {state.route_distance_km():.0f} km, which is too large for the commute advisor. "
                    "Pick a closer destination before loading a corridor forecast."
                )
                state.status_message = message
                _notify("warning", message)
                return
            _notify("info", "Fetching a corridor forecast for the current route geometry…")
            _run_with_busy(state, "Refreshing the forecast cube for the active commute corridor…", state.refresh_dataset)
            _notify("success", f"Loaded a route corridor forecast for {state.route_name}.")
        except Exception as exc:  # noqa: BLE001
            state.status_message = f"Route corridor refresh failed: {exc} {_workaround_html()}"
            _notify("error", str(exc))

    route_refresh_button.on_click(_refresh_route)

    # --- Controls ---
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
            "route_start_name",
            "route_start_lat",
            "route_start_lon",
            "route_end_name",
            "route_end_lat",
            "route_end_lon",
            "route_duration_minutes",
        ],
        widgets={
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

    quick_controls = pn.Param(
        state,
        parameters=["profile", "activity", "pollutant", "horizon_hours"],
        widgets={
            "profile": pn.widgets.RadioButtonGroup,
            "activity": pn.widgets.Select,
            "pollutant": pn.widgets.Select,
            "horizon_hours": pn.widgets.RadioButtonGroup,
        },
        show_name=False,
    )

    advanced_analysis_controls = pn.Param(
        state,
        parameters=["advisor_mode", "map_hour_index", "auto_sync_controls"],
        widgets={
            "advisor_mode": pn.widgets.Select,
            "map_hour_index": pn.widgets.IntSlider,
            "auto_sync_controls": pn.widgets.Checkbox,
        },
        show_name=False,
    )

    return pn.Column(
        hero,
        _local_time_pane(state),
        pn.Card(
            pn.Column(
                pn.Row(location_search, location_search_button),
                location_matches,
                location_search_note,
                quick_controls,
            ),
            title="Quick Start",
            collapsed=False,
        ),
        pn.Card(
            pn.Column(
                pn.pane.Markdown(
                    "Search a start and end point anywhere in the world. "
                    "The corridor forecast refreshes automatically when the end point is resolved.",
                    css_classes=["atmoslens-note"],
                ),
                route_start_search,
                route_start_button,
                route_start_matches,
                route_start_note,
                route_end_search,
                route_end_button,
                route_end_matches,
                route_end_note,
            ),
            title="Commute Route Search",
            collapsed=True,
        ),
        pn.Card(
            pn.Column(
                pn.pane.Markdown(
                    "Advanced changes keep related fields in sync automatically. "
                    "Refresh the cube after manual geometry edits.",
                    css_classes=["atmoslens-note"],
                ),
                refresh_button,
                route_refresh_button,
                advanced_analysis_controls,
                region_controls,
                location_controls,
                route_controls,
            ),
            title="Professional Controls",
            collapsed=True,
        ),
        pn.Card(summary, title="Dataset Status", collapsed=True),
        css_classes=["atmoslens-sidebar"],
    )


def build_app(state: AtmosLensState | None = None):
    state = state or AtmosLensState()
    template = pn.template.FastListTemplate(
        title=APP_NAME,
        accent_base_color="#0f766e",
        header_background="#0f172a",
        theme_toggle=False,
        sidebar_width=390,
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
        state.param.busy,
        state.param.busy_message,
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
        state.param.busy,
        state.param.busy_message,
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
        state.param.busy,
        state.param.busy_message,
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
        state.param.busy,
        state.param.busy_message,
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
        state.param.busy,
        state.param.busy_message,
    )
    matrix_panel = pn.bind(
        lambda *_: render_matrix_panel(state),
        state.param.location_name,
        state.param.location_lat,
        state.param.location_lon,
        state.param.pollutant,
        state.param.advisor_mode,
        state.param.horizon_hours,
        state.param.dataset_revision,
        state.param.busy,
        state.param.busy_message,
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
        state.param.busy,
        state.param.busy_message,
    )

    intro = pn.pane.Markdown(
        f"""
# {APP_NAME} — {APP_TAGLINE}

A **[HoloViz]({HOLOVIZ_UMBRELLA_REPO})** ecosystem application that turns **xarray-backed air-quality forecasts**
into personal decisions: when to run, when to ventilate, and which commute window minimizes exposure.

Type any city on Earth into the sidebar search, press Enter, and the app refreshes the xarray forecast cube,
recalculates recommendations across all profiles and activities, and updates the map, timeline, and route analysis.
The **[Lumen Bridge]({HOLOVIZ_GSOC_WIKI})** tab shows how the analysis naturally maps to an upstream `XarraySource`.
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
                ("Decision Matrix", matrix_panel),
                ("Lumen Bridge", bridge_panel),
                dynamic=True,
            ),
        ]
    )
    return template
