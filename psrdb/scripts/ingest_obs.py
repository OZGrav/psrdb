#!/usr/bin/env python

import glob
import logging
import time
import os
import json
import getpass
from datetime import datetime, timedelta
from decouple import config, Csv
from psrdb.graphql_client import GraphQLClient
from psrdb.util import header, ephemeris
from psrdb.util import time as util_time

from psrdb.tables.pulsar import Pulsar
from psrdb.tables.ephemeris import Ephemeris
from psrdb.tables.calibration import Calibration
from psrdb.tables.observation import Observation

CALIBRATIONS_DIR = config("CALIBRATIONS_DIR")
RESULTS_DIR = config("RESULTS_DIR")
FOLDING_DIRS = config("FOLDING_DIRS", cast=Csv())
PTUSE_FOLDING_DIR = config("PTUSE_FOLDING_DIR")
LOG_DIRECTORY = config("LOG_DIRECTORY")
LOG_FILE = f"{LOG_DIRECTORY}{time.strftime('%Y-%m-%d')}{config('LOG_FILENAME')}"


def get_id(response, table):
    content = json.loads(response.content)
    print(content.keys())

    if "errors" in content.keys():
        print(f"Error in GraphQL response: {content['errors']}")
        return None
    else:
        data = content["data"]
        mutation = "create%s" % (table.capitalize())
        try:
            return int(data[mutation][table]["id"])
        except KeyError:
            return None


def get_id_from_listing(response, table, listing=None):
    content = json.loads(response.content)
    if "errors" not in content.keys():
        data = content["data"]

        if listing is None:
            listing = f"all{table.capitalize()}s"

        try:
            return data[listing]["edges"][0]["node"]["id"]
        except (KeyError, IndexError):
            return None


def get_calibration(utc_start):
    print(config("FOLDING_DIRS", cast=Csv()))
    utc_start_dt = datetime.strptime(utc_start, "%Y-%m-%d-%H:%M:%S")
    auto_cal_epoch = "2020-04-04-00:00:00"
    auto_cal_epoch_dt = datetime.strptime(auto_cal_epoch, "%Y-%m-%d-%H:%M:%S")

    if utc_start_dt > auto_cal_epoch_dt:
        return ("pre", "None")

    cals = sorted(glob.glob(f"{CALIBRATIONS_DIR}/*.jones"), reverse=True)
    for cal in cals:
        cal_file = os.path.basename(cal)
        cal_epoch = cal_file.rstrip(".jones")
        cal_epoch_dt = datetime.strptime(cal_epoch, "%Y-%m-%d-%H:%M:%S")
        if cal_epoch_dt < utc_start_dt:
            return ("post", cal)

    raise RuntimeError(f"Could not find calibration file for utc_start={utc_start}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Ingest PTUSE fold mode observation")
    parser.add_argument("json", type=str, help="meertime.json file location")
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
    format = "%(asctime)s : %(levelname)s : %(msg)s"
    if args.verbose:
        logging.basicConfig(format=format, level=logging.DEBUG)
    else:
        logging.basicConfig(format=format, filename=LOG_FILE, level=logging.INFO)

    # Load client
    url = args.url
    token = args.token
    client = GraphQLClient(args.url, args.verbose_client)
    # Return human readable ID
    literal = False
    quiet = True

    # Load data from json
    with open(args.json, 'r') as json_file:
        meertime_data = json.load(json_file)
    utc_start_dt = datetime.strptime(f"{meertime_data['utcStart']} +0000", "%Y-%m-%d-%H:%M:%S %z").strftime("%Y-%m-%dT%H:%M:%S")

    # Get or upload calibration
    calibration = Calibration(client, url, token)
    cal_response = calibration.create(
        delay_cald_id=meertime_data["delaycal_id"],
        type=meertime_data["cal_type"],
        location=meertime_data["cal_location"],
    )
    cal_id = get_id(cal_response, "calibration")

    # Upload observation
    observation = Observation(client, url, token)
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
    observation_id = get_id(response, "observation")
    print(observation_id)
    logging.info("observation_id=%d" % (observation_id))
    exit()

    # update sessions
    sessions = Sessions(client, url, token)
    sessions.get_dicts = True

    # find any session in which the utc_start of this observation is within 2hrs of the end of the sesion
    end_lte = utc_start_dt
    end_gte = end_lte - timedelta(0, 7200)
    matching_sessions = sessions.list(
        None,
        telescope_id,
        None,
        None,
        None,
        end_lte.strftime("%Y-%m-%dT%H:%M:%S%z"),
        end_gte.strftime("%Y-%m-%dT%H:%M:%S%z"),
    )
    latest_session = None
    for s in matching_sessions:
        if latest_session is None:
            latest_session = s["node"]
        if s["node"]["end"] > latest_session["end"]:
            latest_session = s["node"]

    utc_end = utc_start_dt + timedelta(0, duration)
    if latest_session is None:
        session_id = sessions.create(
            telescope_id, utc_start_dt.strftime("%Y-%m-%dT%H:%M:%S%z"), utc_end.strftime("%Y-%m-%dT%H:%M:%S%z")
        )
    else:
        session_id = sessions.decode_id(latest_session["id"])
        sessions.update(session_id, telescope_id, latest_session["start"], utc_end.strftime("%Y-%m-%dT%H:%M:%S%z"))


if __name__ == "__main__":
    main()
