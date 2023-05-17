import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.collections import Collections as CliCollections


def test_cli_collection_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.name = None

    t = CliCollections(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allCollections":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_collection_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_collections")

    args.subcommand = "create"
    args.name = "updated"
    args.description = "updated"

    t = CliCollections(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createCollection":{"collection":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_collection_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_collections")

    # first create a record
    collection = baker.make("dataportal.Collections")

    # then update the record we just created
    args.subcommand = "update"
    args.id = collection.id
    args.name = "updated"
    args.description = "updated"

    t = CliCollections(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        b'{"data":{"updateCollection":{"collection":{"id":"'
        + str(collection.id).encode("utf-8")
        + b'","name":"updated","description":"updated"}}}}'
    )
    assert response.content == expected_content
