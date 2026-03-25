from __future__ import annotations

from atmoslens.profiles import adjusted_thresholds


def test_sensitive_run_thresholds_are_more_conservative_than_general_walk():
    general_walk = adjusted_thresholds("pm2_5", "General", "Walk")
    sensitive_run = adjusted_thresholds("pm2_5", "Sensitive", "Run")

    assert sensitive_run["good"] < general_walk["good"]
    assert sensitive_run["caution"] < general_walk["caution"]

