import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.targets import Targets as CliTargets


def test_cli_target_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.name = None

    t = CliTargets(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allTargets":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_target_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_targets")

    args.subcommand = "create"
    args.name = "updated"
    args.raj = "updated"
    args.decj = "updated"

    t = CliTargets(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createTarget":{"target":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_target_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_targets")

    # first create a record
    target = baker.make("dataportal.Targets")

    # then update the record we just created
    args.subcommand = "update"
    args.id = target.id
    args.name = "updated"
    args.raj = "updated"
    args.decj = "updated"

    t = CliTargets(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        b'{"data":{"updateTarget":{"target":{"id":"'
        + str(target.id).encode("utf-8")
        + b'","name":"updated","raj":"updated","decj":"updated"}}}}'
    )
    assert response.content == expected_content
