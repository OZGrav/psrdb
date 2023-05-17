import re
import datetime
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.templates import Templates as CliTemplates


def test_cli_template_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.pulsar = None
    args.frequency = 11.11
    args.bandwidth = 22.22
    args.created_at = "2000-01-01T00:00:00+0000"
    args.created_by = "updated"
    args.location = "/location/on/disk"
    args.method = "method"
    args.type = "type"
    args.comment = "comment"

    t = CliTemplates(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allTemplates":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_template_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_templates")

    pulsar = baker.make("dataportal.Pulsars")

    args.subcommand = "create"
    args.pulsar = pulsar.id
    args.frequency = 33.33
    args.bandwidth = 44.44
    args.created_at = "2000-01-01T00:00:00+0000"
    args.created_by = "updated"
    args.location = "/location/on/disk"
    args.method = "method"
    args.type = "type"
    args.comment = "comment"

    t = CliTemplates(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createTemplate":{"template":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_template_update_with_token(client, creator, args, jwt_token, debug_log):
    assert creator.has_perm("dataportal.add_templates")

    # first create a record
    template = baker.make("dataportal.Templates")
    pulsar = baker.make("dataportal.Pulsars")

    # then update the record we just created
    args.subcommand = "update"
    args.id = template.id
    args.pulsar = pulsar.id
    args.frequency = 55.66
    args.bandwidth = 77.77
    args.created_at = "2000-01-01T00:00:01+00:00"
    args.created_by = "updated"
    args.location = "/location/on/disk/updated"
    args.method = "updated"
    args.type = "updated"
    args.comment = "updated"

    t = CliTemplates(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updateTemplate":{"template":{'
        + '"id":"'
        + str(template.id)
        + '",'
        + '"pulsar":{"id":"'
        + t.encode_table_id("Pulsars", args.pulsar)
        + '"},'
        + '"frequency":'
        + str(args.frequency)
        + ","
        + '"bandwidth":'
        + str(args.bandwidth)
        + ","
        + '"createdAt":"'
        + args.created_at
        + '",'
        + '"createdBy":"'
        + args.created_by
        + '",'
        + '"location":"'
        + args.location
        + '",'
        + '"method":"'
        + args.method
        + '",'
        + '"type":"'
        + args.type
        + '",'
        + '"comment":"'
        + args.comment
        + '"}}}}'
    )

    assert response.content == expected_content.encode("utf-8")


def test_cli_template_delete_with_token(client, creator, args, jwt_token, debug_log):

    assert creator.has_perm("dataportal.add_templates")

    # first create a record
    template = baker.make("dataportal.Templates")

    # then update the record we just created
    args.subcommand = "delete"
    args.id = template.id

    t = CliTemplates(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = '{"data":{"deleteTemplate":{"ok":true}}}'

    if response.content != expected_content.encode("utf-8"):
        print(response.content)
        print(expected_content.encode("utf-8"))

    assert response.content == expected_content.encode("utf-8")
