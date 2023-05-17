import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.processingcollections import Processingcollections as CliProcessingcollections


def test_cli_processingcollection_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.processing = None

    t = CliProcessingcollections(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allProcessingcollections":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_processingcollection_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_processingcollections")

    processing = baker.make("dataportal.Processings")
    collection = baker.make("dataportal.Collections")

    args.subcommand = "create"
    args.processing = processing.id
    args.collection = collection.id

    t = CliProcessingcollections(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createProcessingcollection":{"processingcollection":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_processingcollection_update_with_token(client, creator, args, jwt_token, debug_log):
    assert creator.has_perm("dataportal.add_processingcollections")

    # first create a record
    processingcollection = baker.make("dataportal.Processingcollections")

    processing = baker.make("dataportal.Processings")
    collection = baker.make("dataportal.Collections")

    # then update the record we just created
    args.subcommand = "update"
    args.id = processingcollection.id
    args.processing = processing.id
    args.collection = collection.id

    t = CliProcessingcollections(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updateProcessingcollection":{"processingcollection":{'
        + '"id":"'
        + str(processingcollection.id)
        + '",'
        + '"processing":{"id":"'
        + t.encode_table_id("Processings", args.processing)
        + '"},'
        + '"collection":{"id":"'
        + t.encode_table_id("Collections", args.collection)
        + '"}}}}}'
    )

    assert response.content == expected_content.encode("utf-8")
