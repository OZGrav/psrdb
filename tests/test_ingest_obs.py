import json
import logging
from psrdb.utils.other import get_graphql_id, get_rest_api_id


class MockResponse:
    def __init__(self, content):
        self.content = json.dumps(content)
        self.data = json.dumps(content)


def test_get_graphql_id():
    logger = logging.getLogger(__name__)
    tests = [
        ("calibration", {"data": {"createCalibration": {"calibration": {"id": 1}}}}, 1),
        ("observation", {"data": {"createObservation": {"observation": {"id": 1}}}}, 1),
        ("ephemeris",   {"data": {"createEphemeris":   {"ephemeris":   {"id": 1}}}}, 1),
        ('pipelineRun', {'data': {'createPipelineRun': {'pipelineRun': {'id': 1}}}}, 1),
    ]
    for table, response_data, expected in tests:
        response = MockResponse(response_data)
        assert get_graphql_id(response, table, logger) == expected


def test_get_rest_api_id():
    logger = logging.getLogger(__name__)
    tests = [
        (
            {
                'text': "POST API and you have uploaded a template file to Template id: 1",
                'success': True,
                'created': True,
                'errors': None,
                'id' : 1,
            },
            1
        ),
    ]
    for response_data, expected in tests:
        response = MockResponse(response_data)
        assert get_rest_api_id(response, logger) == expected
