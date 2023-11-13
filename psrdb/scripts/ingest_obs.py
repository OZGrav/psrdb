#!/usr/bin/env python

import logging
import time
import os
import json
from datetime import datetime
from decouple import config
from psrdb.graphql_client import GraphQLClient
from psrdb.utils.other import setup_logging, get_graphql_id

from psrdb.tables.pulsar import Pulsar
from psrdb.tables.calibration import Calibration
from psrdb.tables.observation import Observation

LOG_DIRECTORY = config("LOG_DIRECTORY", default="logs/")
LOG_FILE = f"{time.strftime('%Y-%m-%d')}{config('LOG_FILENAME', default='ingest_obs.log')}"


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Ingest PTUSE fold mode observation")
    parser.add_argument("json", type=str, nargs="*", help="meertime.json file location")
    parser.add_argument(
        "-t",
        "--token",
        action="store",
        help="JWT token. Best configured via env variable INGEST_TOKEN.",
        default=os.environ.get("PSRDB_TOKEN"),
    )
    parser.add_argument(
        "-u",
        "--url",
        action="store",
        default=os.environ.get("PSRDB_URL"),
        help="GraphQL URL. Can be configured via INGEST_URL env variable",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Increase verbosity",
    )
    parser.add_argument(
        "-vc",
        "--verbose_client",
        action="store_true",
        default=False,
        help="Increase graphql client verbosity",
    )
    args = parser.parse_args()

    # Set up logger
    if args.verbose:
        level=logging.DEBUG
    else:
        level=logging.INFO
    logger = setup_logging(
        logfile=True,
        filedir=LOG_DIRECTORY,
        filename=LOG_FILE,
        level=level,
    )

    # Load client
    client = GraphQLClient(args.url, args.token, verbose=args.verbose_client)

    for json_path in args.json:

        logger.info(f"Loading data from: {json_path}")

        # Load data from json
        with open(json_path, 'r') as json_file:
            meertime_data = json.load(json_file)
        utc_start_dt = datetime.strptime(f"{meertime_data['utcStart']} +0000", "%Y-%m-%d-%H:%M:%S %z")
        utc_start_dt = utc_start_dt.strftime("%Y-%m-%dT%H:%M:%S+0000")

        # Create pulsar if it doesn't already exist
        pulsar = Pulsar(client)
        pulsar.create(
            name=meertime_data["pulsarName"],
        )

        # Get or upload calibration
        calibration = Calibration(client)
        cal_response = calibration.create(
            schedule_block_id=meertime_data["schedule_block_id"],
            type=meertime_data["cal_type"],
            location=meertime_data["cal_location"],
        )
        cal_id = get_graphql_id(cal_response, "calibration", logger)
        logger.debug(f"Completed ingesting cal_id: {cal_id}")

        # Upload observation
        observation = Observation(client)
        response = observation.create(
            pulsarName=meertime_data["pulsarName"],
            telescopeName=meertime_data["telescopeName"],
            projectCode=meertime_data["projectCode"],
            calibrationId=cal_id,
            ephemerisText=meertime_data["ephemerisText"],
            utcStart=utc_start_dt,
            frequency=meertime_data["frequency"],
            bandwidth=meertime_data["bandwidth"],
            nchan=meertime_data["nchan"],
            beam=meertime_data["beam"],
            nant=meertime_data["nant"],
            nantEff=meertime_data["nantEff"],
            npol=meertime_data["npol"],
            obsType=meertime_data["obsType"],
            raj=meertime_data["raj"],
            decj=meertime_data["decj"],
            duration=meertime_data["duration"],
            nbit=meertime_data["nbit"],
            tsamp=meertime_data["tsamp"],
            foldNbin=meertime_data["foldNbin"],
            foldNchan=meertime_data["foldNchan"],
            foldTsubint=meertime_data["foldTsubint"],
            filterbankNbit=meertime_data["filterbankNbit"],
            filterbankNpol=meertime_data["filterbankNpol"],
            filterbankNchan=meertime_data["filterbankNchan"],
            filterbankTsamp=meertime_data["filterbankTsamp"],
            filterbankDm=meertime_data["filterbankDm"],
        )
        observation_id = get_graphql_id(response, "observation", logger)
        logger.info(f"Completed ingesting observation_id: {observation_id}")


if __name__ == "__main__":
    main()
