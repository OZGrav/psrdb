#!/usr/bin/env python

import glob
import logging
import time
import os
import json
import getpass
from datetime import datetime, timedelta

from psrdb.tables import *
from psrdb.graphql_client import GraphQLClient
from psrdb.util import header, ephemeris
from psrdb.util import time as util_time

CALIBRATIONS_DIR = "/fred/oz005/users/aparthas/reprocessing_MK/poln_calibration"
RESULTS_DIR = "/fred/oz005/kronos"
SEARCH_DIRS = ["/fred/oz005/search"]
LOG_DIRECTORY = "logs"
LOG_FILE = "%s/%s" % (LOG_DIRECTORY, time.strftime("%Y-%m-%d_c2g_receiver.log"))


def get_id(response, table):
    content = json.loads(response.content)
    if not "errors" in content.keys():
        data = content["data"]
        mutation = "create%s" % (table.capitalize())
        if mutation in data.keys():
            if table in data[mutation].keys():
                if "id" in data[mutation][table]:
                    return int(data[mutation][table]["id"])
    return None


def get_id_from_listing(response, table, listing=None):
    content = json.loads(response.content)
    if not "errors" in content.keys():
        data = content["data"]
        if listing is None:
            listing = "all%ss" % (table.capitalize())
        if listing in data.keys():
            if "edges" in data[listing]:
                edge_list = data[listing]["edges"]
                if len(edge_list) >= 1:
                    if "node" in edge_list[0].keys():
                        if "id" in edge_list[0]["node"]:
                            return edge_list[0]["node"]["id"]
    return None


def get_calibration(utc_start):
    utc_start_dt = datetime.strptime(utc_start, "%Y-%m-%d-%H:%M:%S")
    auto_cal_epoch = "2020-04-04-00:00:00"
    auto_cal_epoch_dt = datetime.strptime(auto_cal_epoch, "%Y-%m-%d-%H:%M:%S")
    if utc_start_dt < auto_cal_epoch_dt:
        cals = sorted(glob.glob(CALIBRATIONS_DIR + "/*.jones"), reverse=True)
        for cal in cals:
            cal_file = os.path.basename(cal)
            cal_epoch = cal_file.rstrip(".jones")
            cal_epoch_dt = datetime.strptime(cal_epoch, "%Y-%m-%d-%H:%M:%S")
            if cal_epoch_dt < utc_start_dt:
                return ("post", cal)
        raise RuntimeError("Could not find calibration file for utc_start=" + utc_start)
    else:
        return ("pre", "None")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Ingest PTUSE fold mode observation")
    parser.add_argument(
        "-t",
        "--token",
        action="store",
        help="JWT token. Best configured via env variable INGEST_TOKEN.",
        default=os.environ.get("INGEST_TOKEN"),
    )
    parser.add_argument(
        "-u",
        "--url",
        action="store",
        default=os.environ.get("INGEST_URL"),
        help="GraphQL URL. Can be configured via INGEST_URL env variable",
    )
    parser.add_argument("beam", type=str, help="beam number")
    parser.add_argument("utc_start", type=str, help="utc_start of the obs")
    parser.add_argument("source", type=str, help="source of the obs")
    parser.add_argument("freq", type=str, help="coarse centre frequency of the obs in MHz")
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help="Increase verbosity")
    parser.add_argument(
        "-vc", "--verbose_client", action="store_true", default=False, help="Increase graphql client verbosity"
    )
    args = parser.parse_args()

    beam = args.beam
    utc_start = args.utc_start
    source = args.source
    freq = args.freq
    url = args.url
    token = args.token

    format = "%(asctime)s : %(levelname)s : " + "%s/%s/%s/%s" % (beam, utc_start, source, freq) + " : %(msg)s"
    if args.verbose:
        logging.basicConfig(format=format, level=logging.INFO)
    else:
        logging.basicConfig(format=format, filename=LOG_FILE, level=logging.INFO)

    if args.verbose_client:
        logging.basicConfig(format=format, level=logging.DEBUG)

    client = GraphQLClient(args.url, args.verbose_client)

    results_dir = "%s/%s/%s/%s" % (RESULTS_DIR, beam, utc_start, source)

    # results directory
    obs_header = "%s/obs.header" % (results_dir)
    if not os.path.isfile(obs_header):
        error = Exception("%s not found." % obs_header)
        logging.error(str(error))
        raise error

    location = None
    for dir in SEARCH_DIRS:
        trial_dir = "%s/%s/%s/%s/%s" % (dir, source, utc_start, beam, freq)
        if os.path.exists(trial_dir):
            location = trial_dir
    if location is None:
        raise RuntimeError("Could not find observation in " + str(SEARCH_DIRS))

    # Get the parent pipeline ID
    pipelines = Pipelines(client, url, token)
    parent_id = pipelines.decode_id(
        json.loads(pipelines.list(None, "None").content)["data"]["allPipelines"]["edges"][0]["node"]["id"]
    )

    try:
        hdr = header.PTUSEHeader(obs_header)
    except Exception as error:
        logging.error(str(error))
        raise error
    hdr.set("BEAM", beam)
    hdr.parse()

    literal = False
    quiet = True

    targets = Targets(client, url, token)
    targets.set_field_names(literal, quiet)
    response = targets.list(None, hdr.source)
    encoded_target_id = get_id_from_listing(response, "target")
    if encoded_target_id:
        target_id = int(targets.decode_id(encoded_target_id))
        logging.info("target_id=%d (pre-existing)" % (target_id))
    else:
        response = targets.create(hdr.source, hdr.tied_beam_ra, hdr.tied_beam_dec)
        target_id = get_id(response, "target")
        logging.info("target_id=%d" % (target_id))

    telescopes = Telescopes(client, url, token)
    # mutation uses get_or_create so no need to verify if exists
    response = telescopes.create(hdr.telescope)
    telescope_id = get_id(response, "telescope")
    logging.info("telescope_id=%d" % (telescope_id))

    instrument_configs = Instrumentconfigs(client, url, token)
    # mutation uses get_or_create so no need to verify if exists
    response = instrument_configs.create(hdr.machine, hdr.bandwidth, hdr.frequency, hdr.nchan, hdr.npol, hdr.beam)
    instrument_config_id = get_id(response, "instrumentconfig")
    logging.info("instrument_config_id=%d" % (instrument_config_id))

    calibrations = Calibrations(client, url, token)
    (calibration_type, calibration_location) = get_calibration(utc_start)
    calibration_id = get_id
    response = calibrations.create(calibration_type, calibration_location)
    calibration_id = get_id(response, "calibration")
    logging.info("calibration_id=%d" % (calibration_id))

    if (
        hdr.proposal_id.startswith("SCI-20180516-MB")
        or hdr.proposal_id.startswith("SCI-20200222-MB")
        or hdr.proposal_id.startswith("SCI-2018-0516-MB")
    ):
        program = "MeerTIME"
    elif hdr.proposal_id.startswith("SCI-20180923-MK") or hdr.proposal_id.startswith("DDT-20200506-BS"):
        program = "Trapum"
    elif hdr.proposal_id.startswith("COM-"):
        program = "Commissioning"
    else:
        program = "Unknown"

    programs = Programs(client, url, token)
    response = programs.create(telescope_id, program)
    program_id = get_id(response, "program")

    projects = Projects(client, url, token)
    projects.set_field_names(literal, quiet)
    # 18 months (548 days) embargo in us:
    embargo_days = 548
    response = projects.list(None, program_id, hdr.proposal_id)
    encoded_project_id = get_id_from_listing(response, "project")
    if encoded_project_id:
        project_id = int(projects.decode_id(encoded_project_id))
        logging.info("project_id=%d (pre-existing)" % project_id)
    else:
        response = projects.create(program_id, hdr.proposal_id, "unknown", embargo_days, "")
        project_id = get_id(response, "project")
        logging.info("project_id=%d" % project_id)

    # estimate the duration of the observation
    nfiles = len(glob.glob("%s/*.sf" % (location)))
    duration = nfiles * hdr.search_tsubint
    suspect = False
    comment = ""
    observations = Observations(client, url, token)
    utc_start_dt = datetime.strptime(utc_start, "%Y-%m-%d-%H:%M:%S")
    response = observations.create(
        target_id,
        calibration_id,
        telescope_id,
        instrument_config_id,
        project_id,
        hdr.configuration,
        utc_start_dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
        duration,
        hdr.nant,
        hdr.nant_eff,
        suspect,
        comment,
    )
    observation_id = get_id(response, "observation")
    logging.info("observation_id=%d" % (observation_id))

    created_at = util_time.get_current_time()
    created_by = getpass.getuser()

    response = pipelines.create(hdr.machine, "None", hdr.machine_version, created_at, created_by, hdr.machine_config)
    pipeline_id = get_id(response, "pipeline")
    logging.info("pipeline_id=%d" % (pipeline_id))

    embargo_us = 1e6 * 60 * 60 * 24 * 548
    results = "{}"

    processings = Processings(client, url, token)
    parent_processing_id = None
    utc_dt = datetime.strptime(f"{utc_start} +0000", "%Y-%m-%d-%H:%M:%S %z")
    embargo_end_dt = utc_dt + timedelta(microseconds=embargo_us)
    embargo_end = embargo_end_dt.strftime("%Y-%m-%dT%H:%M:%S")

    response = processings.create(observation_id, pipeline_id, parent_id, embargo_end, location, "{}", "{}", results)
    processing_id = get_id(response, "processing")
    logging.info("processing_id=%d" % (processing_id))

    filterbankings = Filterbankings(client, url, token)
    response = filterbankings.create(
        processing_id, hdr.search_nbit, hdr.search_npol, hdr.search_nchan, hdr.search_dm, hdr.search_tsamp
    )
    filterbanking_id = get_id(response, "filterbanking")
    logging.info("filterbanking_id=%d" % (filterbanking_id))

    # process image results for this observations
    pipelineimages = Pipelineimages(client, url, token)

    # get the list of images in the results dir
    pngs = glob.glob("%s/*.*.png" % (results_dir))
    if len(pngs) == 0:
        # generate pngs from the first search mode file
        os.chdir(results_dir)
        search_files = sorted(glob.glob("%s/*.sf" % (location)), reverse=True)
        if len(search_files) > 0 and os.path.exists(search_files[0]):
            cmd = "pfits_plotsearch -f " + search_files[0] + " -t1 0 -t2 1"
            logging.info(cmd)
            os.system(cmd)

    rank = 0
    for png in glob.glob("%s/*.*.png" % (results_dir)):
        # check image size
        if os.stat(png).st_size > 0:
            image_type = os.path.basename(png)[6:-4]  # clunky! strip the pfits_ prefix and .png suffix
            response = pipelineimages.create(png, image_type, rank, processing_id)
            pipeline_image_id = get_id(response, "pipelineimage")
            rank += 1
            logging.info("pipeline_image_id=%d" % (pipeline_image_id))
        else:
            logging.info("Ignoring empty pipeline image %s" % (png))

    # trigger the web-cache to update with the newly uploaded images
    filterbankings.update(
        filterbanking_id,
        processing_id,
        hdr.search_nbit,
        hdr.search_npol,
        hdr.search_nchan,
        hdr.search_dm,
        hdr.search_tsamp,
    )

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
