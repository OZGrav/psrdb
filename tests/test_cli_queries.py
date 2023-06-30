import json
import pytest

from psrdb.tables.pulsar import Pulsar
from psrdb.utils.other import to_camel_case


class MockResponse:
    def __init__(self, content):
        self.content = json.dumps(content)
        self.data = json.dumps(content)
        self.status_code = 200


def test_pulsar_response_parse(capsys):
    # Create a mock response
    pulsar = Pulsar(None, None)
    pulsar.print_stdout = True
    table_name = "pulsar"
    mock_responses = [
        MockResponse({'data': {to_camel_case(f'create_{table_name}'): {'pulsar': {'id': '1'}}}}),
        MockResponse({'data': {to_camel_case(f'update_{table_name}'): {'pulsar': {'id': '1'}}}}),
        MockResponse({'data': {to_camel_case(f'delete_{table_name}'): {'pulsar': {'id': '1'}}}}),
    ]

    for mock_response in mock_responses:
        # Parse the response
        pulsar.parse_mutation_response(mock_response, table_name)

        # Assert against the captured output
        captured = capsys.readouterr()
        assert captured.out == "1\n"