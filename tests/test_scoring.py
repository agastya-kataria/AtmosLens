from __future__ import annotations

import pandas as pd

from atmoslens.scoring import apply_mode_filter, classify_verdict, evaluate_windows


def test_apply_mode_filter_limits_to_requested_daypart():
    series = pd.Series(
        range(24),
        index=pd.date_range("2026-03-25 00:00", periods=24, freq="h"),
    )

    filtered = apply_mode_filter(series, "Morning", horizon_hours=24)

    assert list(filtered.index.hour) == [5, 6, 7, 8, 9, 10]


def test_evaluate_windows_returns_scored_labels():
    series = pd.Series(
        [12.0, 8.0, 6.0, 5.0, 7.0, 9.0],
        index=pd.date_range("2026-03-25 00:00", periods=6, freq="h"),
    )

    windows = evaluate_windows(
        series,
        pollutant="pm2_5",
        profile_name="General",
        activity_name="Walk",
        mode="Any hour in horizon",
        horizon_hours=6,
    )

    assert not windows.empty
    assert {"score", "verdict", "label"}.issubset(windows.columns)
    assert windows.sort_values("score", ascending=True).iloc[0]["label"] == "03:00–04:00"


def test_classify_verdict_thresholds_are_stable():
    assert classify_verdict(20) == "Good"
    assert classify_verdict(50) == "Caution"
    assert classify_verdict(85) == "Avoid"
