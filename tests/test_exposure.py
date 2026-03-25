from __future__ import annotations

from atmoslens.datasets import DEFAULT_ROUTES
from atmoslens.exposure import rank_route_departures


def test_route_departure_ranking_prefers_cleaner_early_window(sample_dataset):
    route = DEFAULT_ROUTES["Sandyford to Docklands"]
    ranking = rank_route_departures(
        sample_dataset,
        route,
        "pm2_5",
        "Sensitive",
        "Cycle Commute",
        horizon_hours=10,
    )

    best = ranking.sort_values("score", ascending=True).iloc[0]
    eight_am = ranking[ranking["departure"].dt.hour == 8].iloc[0]

    assert best["departure"].hour in {2, 3, 4}
    assert best["mean_value"] < eight_am["mean_value"]
    assert best["score"] < eight_am["score"]

