import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.programs import Programs as CliPrograms


def test_cli_program_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.telescope = None
    args.name = None

    t = CliPrograms(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allPrograms":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_program_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_programs")

    telescope = baker.make("dataportal.Telescopes")

    args.subcommand = "create"
    args.telescope = telescope.id
    args.name = "name"

    t = CliPrograms(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createProgram":{"program":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_program_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_programs")

    # first create a record
    program = baker.make("dataportal.Programs")
    telescope = baker.make("dataportal.Telescopes")

    # then update the record we just created
    args.subcommand = "update"
    args.id = program.id
    args.telescope = telescope.id
    args.name = "updated"

    t = CliPrograms(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updateProgram":{"program":{'
        + '"id":"'
        + str(program.id)
        + '",'
        + '"telescope":{"id":"'
        + t.encode_table_id("Telescopes", args.telescope)
        + '"},'
        + '"name":"'
        + args.name
        + '"}}}}'
    )

    assert response.content == expected_content.encode("utf-8")
