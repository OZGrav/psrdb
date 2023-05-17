import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.basebandings import Basebandings as CliBasebandings


def test_cli_basebanding_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.processing = None

    t = CliBasebandings(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allBasebandings":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_basebanding_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_basebandings")
    processing = baker.make("dataportal.Processings")

    args.subcommand = "create"
    args.processing = processing.id

    t = CliBasebandings(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createBasebanding":{"basebanding":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_basebanding_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_basebandings")

    # first create a record
    basebanding = baker.make("dataportal.Basebandings")

    # create a new key for the update
    processing = baker.make("dataportal.Processings")

    # then update the record we just created
    args.subcommand = "update"
    args.id = basebanding.id
    args.processing = processing.id

    t = CliBasebandings(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        b'{"data":{"updateBasebanding":{"basebanding":{"id":"' + str(basebanding.id).encode("utf-8") + b'"}}}}'
    )
    assert response.content == expected_content
