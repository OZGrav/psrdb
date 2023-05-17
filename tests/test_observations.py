import re
import logging
from model_bakery import baker
from cli.tests.helpers import *
from cli.tables.observations import Observations as CliObservations


def test_cli_observation_list_with_token(client, creator, args, jwt_token):
    args.subcommand = "list"
    args.id = None
    args.id = None
    args.target_id = None
    args.target_name = None
    args.telescope_id = None
    args.telescope_name = None
    args.project_id = None
    args.project_code = None
    args.instrumentconfig_id = None
    args.instrumentconfig_name = None
    args.utcstart_gte = None
    args.utcstart_lte = None

    t = CliObservations(client, "/graphql/", jwt_token)
    response = t.process(args)
    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"allObservations":{"edges":\\[*\\]}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_observation_create_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_observations")

    target = baker.make("dataportal.Targets")
    calibration = baker.make("dataportal.Calibrations")
    telescope = baker.make("dataportal.Telescopes")
    ic = baker.make("dataportal.Instrumentconfigs")
    project = baker.make("dataportal.Projects")

    args.subcommand = "create"
    args.target = target.id
    args.calibration = calibration.id
    args.telescope = telescope.id
    args.instrument_config = ic.id
    args.project = project.id
    args.config = "{}"
    args.utc = "2000-01-01T00:00:00+0000"
    args.duration = 2134.5
    args.nant = 64
    args.nanteff = 64
    args.suspect = "suspect"
    args.comment = "comment"

    t = CliObservations(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content_pattern = b'{"data":{"createObservation":{"observation":{"id":"\\d+"}}}}'
    compiled_pattern = re.compile(expected_content_pattern)
    assert compiled_pattern.match(response.content)


def test_cli_observation_update_with_token(client, creator, args, jwt_token):
    assert creator.has_perm("dataportal.add_observations")

    # first create a record
    observation = baker.make("dataportal.Observations")
    target = baker.make("dataportal.Targets")
    calibration = baker.make("dataportal.Calibrations")
    telescope = baker.make("dataportal.Telescopes")
    ic = baker.make("dataportal.Instrumentconfigs")
    project = baker.make("dataportal.Projects")

    # then update the record we just created
    args.subcommand = "update"
    args.id = observation.id
    args.target = target.id
    args.calibration = calibration.id
    args.telescope = telescope.id
    args.instrument_config = ic.id
    args.project = project.id
    args.config = "{}"
    args.utc = "2000-01-01T00:00:01+00:00"
    args.duration = 2134.6
    args.nant = 65
    args.nanteff = 65
    args.suspect = False
    args.comment = "updated"

    t = CliObservations(client, "/graphql/", jwt_token)
    response = t.process(args)

    assert response.status_code == 200

    expected_content = (
        '{"data":{"updateObservation":{"observation":{'
        + '"id":"'
        + str(observation.id)
        + '",'
        + '"target":{"id":"'
        + t.encode_table_id("Targets", args.target)
        + '"},'
        + '"calibration":{"id":"'
        + t.encode_table_id("Calibrations", args.calibration)
        + '"},'
        + '"telescope":{"id":"'
        + t.encode_table_id("Telescopes", args.telescope)
        + '"},'
        + '"instrumentConfig":{"id":"'
        + t.encode_table_id("Instrumentconfigs", args.instrument_config)
        + '"},'
        + '"project":{"id":"'
        + t.encode_table_id("Projects", args.project)
        + '"},'
        + '"config":"'
        + args.config
        + '",'
        + '"utcStart":"'
        + args.utc
        + '",'
        + '"duration":'
        + str(args.duration)
        + ","
        + '"nant":'
        + str(args.nant)
        + ","
        + '"nantEff":'
        + str(args.nanteff)
        + ","
        + '"suspect":'
        + str(args.suspect).lower()
        + ","
        + '"comment":"'
        + args.comment
        + '"}}}}'
    )

    assert response.content == expected_content.encode("utf-8")
