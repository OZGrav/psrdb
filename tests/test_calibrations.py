import re
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.calibrations import Calibrations as CliCalibrations


def test_cli_calibration_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.type = None

    t = CliCalibrations(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allCalibrations":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_calibration_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_calibrations")

    args.subcommand = "create"
    args.type = "pre"
    args.location = "updated"

    t = CliCalibrations(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createCalibration":{"calibration":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_calibration_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_calibrations")

    # first create a record
    calibration = baker.make("dataportal.Calibrations")

    # then update the record we just created
    args.subcommand = "update"
    args.id = calibration.id
    args.type = "post"
    args.location = "updated"

    t = CliCalibrations(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updateCalibration":{"calibration":{'
        + '"id":"'
        + str(calibration.id)
        + '",'
        + '"calibrationType":"POST",'
        + '"location":"updated"}}}}'
    )

    assert response.content == expected_content.encode("utf-8")
