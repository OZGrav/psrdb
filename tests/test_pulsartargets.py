import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.pulsartargets import Pulsartargets as CliPulsartargets


def test_cli_pulsartarget_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.pulsar = None
    args.target = None
    args.target_name = None
    args.pulsar = None
    args.pulsar_jname = None

    t = CliPulsartargets(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allPulsartargets":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_pulsartarget_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_pulsartargets")

    pulsar = baker.make("dataportal.Pulsars")
    target = baker.make("dataportal.Targets")

    args.subcommand = "create"
    args.pulsar = pulsar.id
    args.target = target.id

    t = CliPulsartargets(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createPulsartarget":{"pulsartarget":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_pulsartarget_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_pulsartargets")

    # first create a record
    pulsartarget = baker.make("dataportal.Pulsartargets")
    pulsar = baker.make("dataportal.Pulsars")
    target = baker.make("dataportal.Targets")

    # then update the record we just created
    args.subcommand = "update"
    args.id = pulsartarget.id
    args.pulsar = pulsar.id
    args.target = target.id

    t = CliPulsartargets(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updatePulsartarget":{"pulsartarget":{'
        + '"id":"'
        + str(pulsartarget.id)
        + '",'
        + '"pulsar":{"id":"'
        + t.encode_table_id("Pulsars", args.pulsar)
        + '"},'
        + '"target":{"id":"'
        + t.encode_table_id("Targets", args.target)
        + '"}}}}}'
    )

    assert response.content == expected_content.encode("utf-8")
