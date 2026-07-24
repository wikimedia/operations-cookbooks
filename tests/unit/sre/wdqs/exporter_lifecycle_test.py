"""Unit tests for WDQS Blazegraph exporter lifecycle handling."""

import importlib

import pytest


data_transfer = importlib.import_module("cookbooks.sre.wdqs.data-transfer")

EXPECTED_START_ORDER = {
    "categories": [
        "wdqs-categories",
        "prometheus-blazegraph-exporter-wdqs-categories",
    ],
    "wikidata": [
        "wdqs-blazegraph",
        "prometheus-blazegraph-exporter-wdqs-blazegraph",
        "wdqs-updater",
    ],
    "wikidata_full": [
        "wdqs-blazegraph",
        "prometheus-blazegraph-exporter-wdqs-blazegraph",
        "wdqs-updater",
    ],
    "wikidata_main": [
        "wdqs-blazegraph",
        "prometheus-blazegraph-exporter-wdqs-blazegraph",
        "wdqs-updater",
    ],
    "scholarly_articles": [
        "wdqs-blazegraph",
        "prometheus-blazegraph-exporter-wdqs-blazegraph",
        "wdqs-updater",
    ],
    "commons": [
        "wcqs-blazegraph",
        "prometheus-blazegraph-exporter-wcqs-blazegraph",
        "wcqs-updater",
    ],
}


@pytest.mark.parametrize("instance_name", EXPECTED_START_ORDER)
def test_data_transfer_service_start_order(instance_name):
    """Start Blazegraph, then its exporter, then the updater."""
    services = data_transfer.BLAZEGRAPH_INSTANCES[instance_name]["services"]

    assert list(reversed(services)) == EXPECTED_START_ORDER[instance_name]
