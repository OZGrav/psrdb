#!/usr/bin/env python

import json
import time
import logging
from glob import glob
from os import environ
from argparse import ArgumentParser

from psrdb.graphql_client import GraphQLClient
from psrdb.utils.other import setup_logging, decode_id, get_graphql_id, get_rest_api_id

from psrdb.tables.pipeline_image import PipelineImage
from psrdb.tables.pipeline_run import PipelineRun
from psrdb.tables.observation import Observation
from psrdb.tables.ephemeris import Ephemeris
from psrdb.tables.template import Template
from psrdb.tables.toa import Toa

logger = setup_logging(console=True, level=logging.INFO)

parser = ArgumentParser(description="Ingest Molonglo fold mode observation results, images and ToAs into PSRDB")
parser.add_argument("-t", "--token", default=environ.get("PSRDB_TOKEN"), help="JWT token")
parser.add_argument("-u", "--url", default=environ.get("PSRDB_URL", "https://pulsars.org.au/api/"), help="GraphQL URL")
parser.add_argument("-p", "--pulsar", help="Pulsar Jname")
parser.add_argument("-d", "--date", help="UTC date of the observation")
args = parser.parse_args()

client = GraphQLClient(args.url, args.token, logger)

# Query observations to get obs ID
obs_client = Observation(client)
obs_client.get_dicts  = True
obs_data = obs_client.list(
    pulsar_name=[args.pulsar],
    project_short="MONSPSR_TIMING",
    main_project="MONSPSR",
    utcs=args.date,
    utce=args.date,
    obs_type="fold",
)
assert len(obs_data) == 1, "Expected 1 observation, got %d" % len(obs_data)
obs_id = decode_id(obs_data[0]["id"])

# Upload ephem
ephemeris_client = Ephemeris(client)
ephemeris_response = ephemeris_client.create(
    args.pulsar,
    f"/fred/oz002/ldunn/meertime_dataportal/data/pars/{args.pulsar}.par",
    project_short="MONSPSR_TIMING",
    comment="",
)
ephemeris_id = get_graphql_id(ephemeris_response, "ephemeris", logging.getLogger(__name__))

# Upload template
template_client  = Template(client)
template_response = template_client.create(
    args.pulsar,
    "OTHER",
    f"/fred/oz002/ldunn/meertime_dataportal/data/profiles/{args.pulsar}.std",
    project_short="MONSPSR_TIMING",
)
template_id = get_rest_api_id(template_response, logging.getLogger(__name__))


# Create pipeline run
# Read in results JSON
with open(f"/fred/oz002/ldunn/meertime_dataportal/data/post/{args.pulsar}/{args.date}/results.json", "r") as f:
    results_dict = json.load(f)
pipeline_run_client   = PipelineRun(client)
pipeline_run_client.get_dicts = True
pipe_run_data = pipeline_run_client.create(
    obs_id,
    ephemeris_id,
    template_id,
    "monspipe",
    "This pipeline processed the observations from the UTMOST-NS pulsar timing programme",
    "1.0.0",
    "Completed",
    "/fred/oz002/ldunn/meertime_dataportal/data/post/",
    {},
    results_dict=results_dict
)
pipe_id = get_graphql_id(pipe_run_data, "pipeline_run", logging.getLogger(__name__))

image_data = []
# file_loc, file_type, file_res, cleaned
image_data.append( (f"{args.pulsar}_{args.date}_DFTp_raw.png",   'profile',    'high', False) )
image_data.append( (f"{args.pulsar}_{args.date}_FY_raw.png",     'phase-time', 'high', False) )
image_data.append( (f"{args.pulsar}_{args.date}_GT_raw.png",     'phase-freq', 'high', False) )
image_data.append( (f"{args.pulsar}_{args.date}_DFTp_clean.png", 'profile',    'high', True ) )
image_data.append( (f"{args.pulsar}_{args.date}_FY_clean.png",   'phase-time', 'high', True ) )
image_data.append( (f"{args.pulsar}_{args.date}_GT_clean.png",   'phase-freq', 'high', True ) )
image_data.append( (f"{args.pulsar}_{args.date}_pat.png",        'toa-single', 'high', True ) )

# Upload images
pipeline_image_client = PipelineImage(client)
for image_path, image_type, resolution, cleaned in image_data:
    image_response = pipeline_image_client.create(
        pipe_id,
        f"/fred/oz002/ldunn/meertime_dataportal/data/images/{args.pulsar}/{args.date}/{image_path}",
        image_type,
        resolution,
        cleaned,
    )
    content = json.loads(image_response.content)
    if image_response.status_code not in (200, 201):
        logger.error("Failed to upload image")
        exit(1)

# Upload TOAs
toa_client = Toa(client)
with open(f"/fred/oz002/ldunn/meertime_dataportal/data/post/{args.pulsar}/{args.date}/toa.tim", "r") as f:
    toa_lines = f.readlines()
toa_client.create(
    pipe_id,
    "MONSPSR_TIMING",
    f"/fred/oz002/ldunn/meertime_dataportal/data/pars/{args.pulsar}.par",
    template_id,
    toa_lines,
    False,
    True,
    True,
    1,
    1,
)