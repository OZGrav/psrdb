import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.pulsars import Pulsars as CliPulsars


def test_cli_pulsar_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.jname = None

    t = CliPulsars(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allPulsars":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_pulsar_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_pulsars")

    args.subcommand = "create"
    args.jname = "updated"
    args.state = "updated"
    args.comment = "updated"

    t = CliPulsars(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createPulsar":{"pulsar":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_pulsar_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_pulsars")

    # first create a record
    pulsar = baker.make("dataportal.Pulsars")

    # then update the record we just created
    args.subcommand = "update"
    args.id = pulsar.id
    args.jname = "updated"
    args.state = "updated"
    args.comment = "updated"

    t = CliPulsars(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        b'{"data":{"updatePulsar":{"pulsar":{"id":"'
        + str(pulsar.id).encode("utf-8")
        + b'","jname":"updated","state":"updated","comment":"updated"}}}}'
    )
    assert response.content == expected_content
