import re
from model_bakery import baker
from datetime import datetime
from dataportal.storage import get_upload_location
from cli.tests.helpers import *
from cli.tables.pipelineimages import Pipelineimages as CliPipelineimages


def test_cli_pipelineimage_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.processing_id = None

    t = CliPipelineimages(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allPipelineimages":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_pipelineimage_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_pipelineimages")

    processing = baker.make(
        "dataportal.Processings",
        observation__telescope__name="test",
        observation__target__name="test",
        observation__instrument_config__beam="test",
    )

    args.subcommand = "create"
    args.processing_id = processing.id
    args.image = __file__
    args.image_type = "test"
    args.rank = 99

    t = CliPipelineimages(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createPipelineimage":{"pipelineimage":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_pipelineimage_update_with_token(client, creator, args, jwt_token, debug_log):
    assert creator.has_perm("dataportal.add_pipelineimages")

    # first create a record
    pipelineimage = baker.make(
        "dataportal.Pipelineimages",
        image_type="updated",
        processing__observation__telescope__name="test",
        processing__observation__target__name="test",
        processing__observation__instrument_config__beam="test",
        processing__observation__utc_start=datetime.strptime("2020-10-10-10:10:10+00:00", "%Y-%m-%d-%H:%M:%S%z"),
    )

    # then update the record we just created
    args.subcommand = "update"
    args.id = pipelineimage.id
    args.image = __file__
    args.image_type = "updated"
    args.rank = 100
    args.processing_id = pipelineimage.processing_id

    t = CliPipelineimages(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updatePipelineimage":{"pipelineimage":{'
        + '"id":"'
        + str(pipelineimage.id)
        + '",'
        + '"image":"'
        + get_upload_location(pipelineimage, "updated.png")
        + '",'
        + '"imageType":"'
        + args.image_type
        + '",'
        + '"processing":{"id":"'
        + t.encode_table_id("Processings", args.processing_id)
        + '"},'
        + '"rank":'
        + str(args.rank)
        + "}}}}"
    )

    assert response.content == expected_content.encode("utf-8")
