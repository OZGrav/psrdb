import re
import datetime
from model_bakery import baker
from dataportal.models import Ephemerides
from cli.tests.helpers import *
from cli.util.decimal import get_decimal_from_limits
from cli.tables.ephemerides import Ephemerides as CliEphemerides


def test_cli_ephemeris_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.pulsar = None
    args.p0 = 11.11
    args.dm = 22.22
    args.rm = 33.33
    args.eph = '{"key": "value"}'

    t = CliEphemerides(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allEphemerides":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_ephemeris_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_ephemerides")

    pulsar = baker.make("dataportal.Pulsars")

    args.subcommand = "create"
    args.pulsar = pulsar.id
    args.created_at = "2000-01-01T00:00:00+0000"
    args.created_by = "updated"
    args.ephemeris = '{"FO": "1234.5"}'
    args.p0 = 12.12
    args.dm = 32.32
    args.rm = 0.0
    args.comment = "updated"
    args.valid_from = "2000-01-01T00:00:00+00:00"
    args.valid_to = "2001-01-01T00:00:01+00:00"

    t = CliEphemerides(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createEphemeris":{"ephemeris":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_ephemeris_update_with_token(client, creator, args, jwt_token, debug_log):
    assert creator.has_perm("dataportal.add_ephemerides")

    # first create a record
    ephemeris = baker.make("dataportal.Ephemerides")
    pulsar = baker.make("dataportal.Pulsars")

    # then update the record we just created
    args.subcommand = "update"
    args.id = ephemeris.id
    args.pulsar = pulsar.id
    args.created_at = "2000-01-01T00:00:01+00:00"
    args.created_by = "updated"
    args.ephemeris = '{"F0": "2345.6" }'
    args.p0 = 13.13
    args.dm = 32.32
    args.rm = 0.1
    args.comment = "updated"
    args.valid_from = "2000-01-01T00:00:01+00:00"
    args.valid_to = "2001-01-01T00:00:02+00:00"

    t = CliEphemerides(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updateEphemeris":{"ephemeris":{'
        + '"id":"'
        + str(ephemeris.id)
        + '",'
        + '"pulsar":{"id":"'
        + t.encode_table_id("Pulsars", args.pulsar)
        + '"},'
        + '"createdAt":"'
        + args.created_at
        + '",'
        + '"createdBy":"'
        + args.created_by
        + '",'
        + '"ephemeris":'
        + '"{\\"F0\\": \\"2345.6\\"}",'
        + '"p0":'
        + str(args.p0)
        + ","
        + '"dm":'
        + str(args.dm)
        + ","
        + '"rm":'
        + str(args.rm)
        + ","
        + '"comment":"'
        + args.comment
        + '",'
        + '"validFrom":"'
        + args.valid_from
        + '",'
        + '"validTo":"'
        + args.valid_to
        + '"}}}}'
    )

    print(response.content)
    print(expected_content.encode("utf-8"))
    assert response.content == expected_content.encode("utf-8")
