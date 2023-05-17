import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.sessions import Sessions as CliSessions


def test_cli_session_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.telescope = None
    args.telescope_name = None
    args.start_lte = None
    args.start_gte = None
    args.end_lte = None
    args.end_gte = None

    t = CliSessions(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allSessions":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_session_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_sessions")

    telescope = baker.make("dataportal.Telescopes")

    args.subcommand = "create"
    args.telescope = telescope.id
    args.start = "2020-01-02T03:04:05+00:00"
    args.end = "2020-01-02T03:04:06+00:00"

    t = CliSessions(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createSession":{"session":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_session_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_sessions")

    # first create a record
    session = baker.make("dataportal.Sessions")
    telescope = baker.make("dataportal.Telescopes")

    # then update the record we just created
    args.subcommand = "update"
    args.id = session.id
    args.telescope = telescope.id
    args.start = "2020-01-02T03:04:07+00:00"
    args.end = "2020-01-02T03:04:08+00:00"

    t = CliSessions(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updateSession":{"session":{'
        + '"id":"'
        + str(session.id)
        + '",'
        + '"telescope":{"id":"'
        + t.encode_table_id("Telescopes", args.telescope)
        + '"},'
        + '"start":"'
        + args.start
        + '",'
        + '"end":"'
        + args.end
        + '"}}}}'
    )

    assert response.content == expected_content.encode("utf-8")
