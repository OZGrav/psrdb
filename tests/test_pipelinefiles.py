import re
import os
from datetime import datetime
from model_bakery import baker
from dataportal.storage import get_pipeline_upload_location
from cli.tests.helpers import *
from cli.tables.pipelinefiles import Pipelinefiles as CliPipelinefiles


def test_cli_pipelinefile_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.file_type = None
    args.processing_id = None

    t = CliPipelinefiles(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allPipelinefiles":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_pipelinefile_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_pipelinefiles")

    processing = baker.make(
        "dataportal.Processings",
        observation__telescope__name="test",
        observation__target__name="test",
        observation__instrument_config__beam="test",
        pipeline__name="test",
    )

    args.subcommand = "create"
    args.processing_id = processing.id
    args.file = __file__
    args.file_type = "test"

    t = CliPipelinefiles(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createPipelinefile":{"pipelinefile":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_pipelinefile_update_with_token(client, creator, args, jwt_token, debug_log):
    assert creator.has_perm("dataportal.add_pipelinefiles")

    # first create a record
    pipelinefile = baker.make(
        "dataportal.Pipelinefiles",
        file_type="updated",
        processing__observation__telescope__name="test",
        processing__observation__target__name="test",
        processing__observation__instrument_config__beam="test",
        processing__observation__utc_start=datetime.strptime("2020-10-10-10:10:10+00:00", "%Y-%m-%d-%H:%M:%S%z"),
        processing__pipeline__name="test",
    )

    # then update the record we just created
    args.subcommand = "update"
    args.id = pipelinefile.id
    args.file = __file__
    args.file_type = "updated"
    args.processing_id = pipelinefile.processing_id

    t = CliPipelinefiles(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updatePipelinefile":{"pipelinefile":{'
        + '"id":"'
        + str(pipelinefile.id)
        + '",'
        + '"file":"'
        + get_pipeline_upload_location(pipelinefile, os.path.basename(args.file))
        + '",'
        + '"fileType":"'
        + args.file_type
        + '",'
        + '"processing":{"id":"'
        + t.encode_table_id("Processings", args.processing_id)
        + '"}}}}}'
    )

    assert response.content == expected_content.encode("utf-8")
