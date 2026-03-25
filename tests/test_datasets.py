from __future__ import annotations

from atmoslens.datasets import _query_variants, _search_score
from atmoslens.models import LocationDefinition
from atmoslens.state import AtmosLensState


def test_query_variants_expand_multiword_place_strings():
    variants = _query_variants("Shibuya Tokyo")

    assert variants[0] == "Shibuya Tokyo"
    assert "Shibuya, Tokyo" in variants
    assert "Shibuya" in variants
    assert "Tokyo" in variants


def test_search_score_prefers_results_matching_more_query_tokens():
    query_tokens = ["shibuya", "tokyo"]
    district_result = {
        "name": "Shibuya",
        "admin1": "Tokyo",
        "country": "Japan",
        "feature_code": "PPL",
    }
    city_result = {
        "name": "Tokyo",
        "admin1": "Tokyo",
        "country": "Japan",
        "feature_code": "PPLC",
    }

    assert _search_score(district_result, query_tokens, 0) > _search_score(city_result, query_tokens, 0)


def test_search_score_prefers_results_near_the_reference_location():
    query_tokens = ["tao"]
    nearby_result = {
        "name": "Koh Tao",
        "admin1": "Surat Thani",
        "country": "Thailand",
        "country_code": "TH",
        "feature_code": "PPL",
        "latitude": 10.0981,
        "longitude": 99.8406,
        "population": 1500,
    }
    distant_result = {
        "name": "Tao",
        "admin1": "New Caledonia",
        "country": "New Caledonia",
        "country_code": "NC",
        "feature_code": "PPL",
        "latitude": -20.55,
        "longitude": 164.81,
        "population": 1500,
    }

    assert _search_score(
        nearby_result,
        query_tokens,
        0,
        reference=(7.8804, 98.3923),
        country_bias="Thailand",
    ) > _search_score(
        distant_result,
        query_tokens,
        0,
        reference=(7.8804, 98.3923),
        country_bias="Thailand",
    )


def test_applying_location_search_result_seeds_a_local_route(sample_dataset):
    state = AtmosLensState(dataset=sample_dataset)
    state._location_search_results = [
        LocationDefinition(
            "Tokyo, Japan",
            35.6895,
            139.6917,
            "Asia/Tokyo",
            "Japan",
        )
    ]

    state.apply_location_search_result(0)

    assert state.route_name == "Approach to Tokyo, Japan"
    assert state.route_end_name == "Tokyo, Japan"
    assert state.route_end_lat == state.location_lat
    assert state.route_end_lon == state.location_lon


def test_applying_location_search_result_updates_region_preset_display(sample_dataset):
    state = AtmosLensState(dataset=sample_dataset)
    state._location_search_results = [
        LocationDefinition(
            "Sonipat, Haryana, India",
            28.9948,
            77.0194,
            "Asia/Kolkata",
            "India",
        )
    ]

    state.apply_location_search_result(0)

    assert state.region_preset == "Delhi NCR"
    assert state.forecast_timezone == "Asia/Kolkata"
