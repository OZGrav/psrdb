import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.filterbankings import Filterbankings as CliFilterbankings


def test_cli_filterbanking_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.processing = None

    t = CliFilterbankings(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allFilterbankings":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


@disable_signals
def test_cli_filterbanking_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_filterbankings")

    processing = baker.make("dataportal.Processings")

    args.subcommand = "create"
    args.processing = processing.id
    args.nbit = 8
    args.npol = 2
    args.nchan = 1024
    args.dm = 12.12
    args.tsamp = 0.00064

    client.verbose = True
    t = CliFilterbankings(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createFilterbanking":{"filterbanking":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


@disable_signals
def test_cli_filterbanking_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_filterbankings")

    # first create a record
    filterbanking = baker.make("dataportal.Filterbankings")
    processing = baker.make("dataportal.Processings")

    # then update the record we just created
    args.subcommand = "update"
    args.id = filterbanking.id
    args.processing = processing.id
    args.nbit = 9
    args.npol = 3
    args.nchan = 1025
    args.dm = 13.13
    args.tsamp = 0.00065

    t = CliFilterbankings(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updateFilterbanking":{"filterbanking":{"id":"'
        + str(filterbanking.id)
        + '",'
        + '"processing":{"id":"'
        + t.encode_table_id("Processings", args.processing)
        + '"},'
        + '"nbit":'
        + str(args.nbit)
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
        + '"tsamp":'
        + str(args.tsamp)
        + "}}}}"
    )

    assert response.content == expected_content.encode("utf-8")
