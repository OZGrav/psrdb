import json
import logging
from psrdb.scripts.ingest_obs import get_id


class MockResponse:
    def __init__(self, content):
        self.content = json.dumps(content)


def test_get_id():
    logger = logging.getLogger(__name__)
    tests = [
        ("calibration", {"data": {"createCalibration": {"calibration": {"id": 1}}}}, 1),
        ("observation", {"data": {"createObservation": {"observation": {"id": 1}}}}, 1),
    ]
    for table, response_data, expected in tests:
        response = MockResponse(response_data)
        assert get_id(response, table, logger) == expected
