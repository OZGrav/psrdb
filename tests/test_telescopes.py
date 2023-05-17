import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.telescopes import Telescopes as CliTelescopes


def test_cli_telescope_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.name = None

    t = CliTelescopes(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allTelescopes":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_telescope_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_telescopes")

    args.subcommand = "create"
    args.name = "updated"

    t = CliTelescopes(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createTelescope":{"telescope":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_telescope_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_telescopes")

    # first create a record
    telescope = baker.make("dataportal.Telescopes")

    # then update the record we just created
    args.subcommand = "update"
    args.id = telescope.id
    args.name = "updated"

    t = CliTelescopes(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        b'{"data":{"updateTelescope":{"telescope":{"id":"'
        + str(telescope.id).encode("utf-8")
        + b'","name":"updated"}}}}'
    )
    assert response.content == expected_content
