import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.foldings import Foldings as CliFoldings


def test_cli_folding_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.processing = None
    args.eph = None

    t = CliFoldings(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allFoldings":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


@disable_signals
def test_cli_folding_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_foldings")

    processing = baker.make("dataportal.Processings", observation__duration=10)
    eph = baker.make("dataportal.Ephemerides")

    args.subcommand = "create"
    args.processing = processing.id
    args.eph = eph.id
    args.nbin = 1024
    args.npol = 2
    args.nchan = 1024
    args.dm = 12.12
    args.tsubint = 0.00064

    t = CliFoldings(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createFolding":{"folding":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)

    assert compiled_pattern.match(response.content)


@disable_signals
def test_cli_folding_update_with_token(client, creator, args, jwt_token):

    assert creator.has_perm("dataportal.add_foldings")

    # first create a record
    folding = baker.make("dataportal.Foldings", processing__observation__duration=10)
    processing = baker.make("dataportal.Processings")
    ephemeris = baker.make("dataportal.Ephemerides")

    # then update the record we just created
    args.subcommand = "update"
    args.id = folding.id
    args.processing = processing.id
    args.eph = ephemeris.id
    args.nbin = 1025
    args.npol = 5
    args.nchan = 1023
    args.dm = 13.13
    args.tsubint = 0.1234

    t = CliFoldings(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updateFolding":{"folding":{"id":"'
        + str(folding.id)
        + '",'
        + '"processing":{"id":"'
        + t.encode_table_id("Processings", args.processing)
        + '"},'
        + '"foldingEphemeris":{"id":"'
        + t.encode_table_id("Ephemerides", args.eph)
        + '"},'
        + '"nbin":'
        + str(args.nbin)
        + ","
        + '"npol":'
        + str(args.npol)
        + ","
        + '"nchan":'
        + str(args.nchan)
        + ","
        + '"dm":'
        + str(args.dm)
        + ","
        + '"tsubint":'
        + str(args.tsubint)
        + "}}}}"
    )

    assert response.content == expected_content.encode("utf-8")
