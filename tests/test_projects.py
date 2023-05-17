import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.projects import Projects as CliProjects


def test_cli_project_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.program = None
    args.code = None

    t = CliProjects(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allProjects":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_project_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_projects")

    program = baker.make("dataportal.Programs")

    args.subcommand = "create"
    args.program = program.id
    args.code = "updated"
    args.short = "updated"
    args.embargo_period = 1800
    args.description = "updated"

    t = CliProjects(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createProject":{"project":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_project_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_projects")

    # first create a record
    project = baker.make("dataportal.Projects")
    program = baker.make("dataportal.Programs")

    # then update the record we just created
    args.subcommand = "update"
    args.id = project.id
    args.program = program.id
    args.code = "updated"
    args.short = "updated"
    args.embargo_period = 1801
    args.description = "updated"

    t = CliProjects(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updateProject":{"project":{'
        + '"id":"'
        + str(project.id)
        + '",'
        + '"program":{"id":"'
        + t.encode_table_id("Programs", args.program)
        + '"},'
        + '"code":"'
        + args.code
        + '",'
        + '"short":"'
        + args.short
        + '",'
        + '"embargoPeriod":"'
        + str(args.embargo_period)
        + ' days, 0:00:00",'
        + '"description":"'
        + args.description
        + '"}}}}'
    )

    assert response.content == expected_content.encode("utf-8")
