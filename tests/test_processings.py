import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.processings import Processings as CliProcessings


def test_cli_processing_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.location = None
    args.parent = None
    args.utc_start = None
    args.observation = None

    t = CliProcessings(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allProcessings":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_processing_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_processings")

    observation = baker.make("dataportal.Observations")
    pipeline = baker.make("dataportal.Pipelines")
    parent = baker.make("dataportal.Processings")

    args.subcommand = "create"
    args.observation = observation.id
    args.pipeline = pipeline.id
    args.parent = parent.id
    args.embargo_end = "2000-01-01T00:00:00+00:00"
    args.location = "updated"
    args.job_state = '{"foo": "bar"}'
    args.job_output = '{"foo": "bar"}'
    args.results = '{"foo": "bar"}'

    t = CliProcessings(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createProcessing":{"processing":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_processing_update_with_token(client, creator, args, jwt_token, debug_log):
    assert creator.has_perm("dataportal.add_processings")

    # first create a record
    processing = baker.make("dataportal.Processings")

    observation = baker.make("dataportal.Observations")
    pipeline = baker.make("dataportal.Pipelines")
    parent = baker.make("dataportal.Processings")

    # then update the record we just created
    args.subcommand = "update"
    args.id = processing.id
    args.observation = observation.id
    args.pipeline = pipeline.id
    args.parent = parent.id
    args.embargo_end = "2000-01-01T00:00:01+00:00"
    args.location = "updated"
    args.job_state = '{"foo": "updated"}'
    args.job_output = '{"foo": "updated"}'
    args.results = '{"foo": "updated"}'

    t = CliProcessings(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updateProcessing":{"processing":{'
        + '"id":"'
        + str(processing.id)
        + '",'
        + '"observation":{"id":"'
        + t.encode_table_id("Observations", args.observation)
        + '"},'
        + '"pipeline":{"id":"'
        + t.encode_table_id("Pipelines", args.pipeline)
        + '"},'
        + '"parent":{"id":"'
        + t.encode_table_id("Processings", args.parent)
        + '"},'
        + '"location":"updated",'
        + '"embargoEnd":"2000-01-01T00:00:01+00:00",'
        + "\"jobState\":\"{'foo': 'updated'}\","
        + "\"jobOutput\":\"{'foo': 'updated'}\","
        + "\"results\":\"{'foo': 'updated'}\"}}}}"
    )

    assert response.content == expected_content.encode("utf-8")
