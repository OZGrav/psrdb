import json
from psrdb.scripts.ingest_obs import get_id, get_id_from_listing, get_calibration


class MockResponse:
    def __init__(self, content):
        self.content = json.dumps(content)


def test_get_id():
    response = MockResponse({"data": {"createPulsars": {"pulsars": {"id": 1}}}})
    assert get_id(response, "pulsars") == 1


def test_get_id_from_listing():
    response = MockResponse({"data": {"allPulsars": {"edges": [{"node": {"id": 1}}]}}})
    assert get_id_from_listing(response, "pulsar") == 1


def test_get_id_from_listing_with_listing():
    response = MockResponse({"data": {"customList": {"edges": [{"node": {"id": 1}}]}}})
    assert get_id_from_listing(response, "pulsar", "customList") == 1


def test_get_id_from_listing_no_edges():
    response = MockResponse({"data": {"customList": {"edges": []}}})
    assert get_id_from_listing(response, "pulsar", "customList") is None


def test_get_calibration():
    assert get_calibration("2023-01-01-12:44:50") is False
