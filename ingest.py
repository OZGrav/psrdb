#!/usr/bin/env python

import glob
import logging
import time
import os
import json
import getpass
from datetime import datetime, timedelta

from tables import *
from graphql_client import GraphQLClient
from util import header, ephemeris
from util import time as util_time

CALIBRATIONS_DIR = "../ingest-ajameson/test-data/calibrations"
RESULTS_DIR = "../ingest-ajameson/test-data/kronos"
FOLDING_DIR = "../ingest-ajameson/test-data/timing"
PTUSE_FOLDING_DIR = "../ingest-ajameson/test-data/ptuse-folding"
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


def main(beam, utc_start, source, freq, client, url, token):

    obs_type = "fold"
    results_dir = "%s/%s/%s/%s" % (RESULTS_DIR, beam, utc_start, source)

    # results directory
    obs_header = "%s/obs.header" % (results_dir)
    if not os.path.isfile(obs_header):
        error = Exception("%s not found." % obs_header)
        logging.error(str(error))
        raise error

    try:
        hdr = header.PTUSEHeader(obs_header)
    except Exception as error:
        logging.error(str(error))
        raise error
    hdr.set("BEAM", beam)
    hdr.parse()

    try:
        obs_results = header.KeyValueStore("%s/obs.results" % (results_dir))
    except Exception as error:
        logging.error(str(error))
        raise error

    literal = False
    quiet = True

    targets = Targets(client, url, token)
    targets.set_field_names(literal, quiet)
    response = targets.list_graphql(None, hdr.source)
    encoded_target_id = get_id_from_listing(response, "target")
    if encoded_target_id:
        target_id = int(targets.decode_id(encoded_target_id))
        logging.info("target_id=%d (pre-existing)" % (target_id))
    else:
        response = targets.create(hdr.source, hdr.tied_beam_ra, hdr.tied_beam_dec)
        target_id = get_id(response, "target")
        logging.info("target_id=%d" % (target_id))

    pulsars = Pulsars(client, url, token)
    pulsars.set_field_names(literal, quiet)
    response = pulsars.list_graphql(None, hdr.source)
    encoded_pulsar_id = get_id_from_listing(response, "pulsar")
    if encoded_target_id:
        pulsar_id = int(targets.decode_id(encoded_pulsar_id))
        logging.info("pulsar_id=%d (pre-existing)" % (pulsar_id))
    else:
        response = pulsars.create(hdr.source, "", "")
        pulsar_id = get_id(response, "pulsar")
        logging.info("pulsar_id=%d" % (pulsar_id))

    pulsartargets = Pulsartargets(client, url, token)
    # mutation uses get_or_create so no need to verify if exists
    response = pulsartargets.create(pulsar_id, target_id)
    pulsartargets_id = get_id(response, "pulsartarget")

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

    projects = Projects(client, url, token)
    projects.set_field_names(literal, quiet)
    # 18 months (548 days) embargo in us:
    embargo_days = 548
    response = projects.list_graphql(None, hdr.proposal_id)
    encoded_project_id = get_id_from_listing(response, "project")
    if encoded_project_id:
        project_id = int(projects.decode_id(encoded_project_id))
        logging.info("project_id=%d (pre-existing)" % project_id)
    else:
        response = projects.create(hdr.proposal_id, "unknown", embargo_days, "")
        project_id = get_id(response, "project")
        logging.info("project_id=%d" % project_id)

    # estimate the duration of the observation
    duration = float(obs_results.get("length"))
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

    eph = ephemeris.Ephemeris()
    freq_sum_fn = "%s/%s/%s/%s/freq.sum" % (RESULTS_DIR, beam, utc_start, source)
    time_sum_fn = "%s/%s/%s/%s/time.sum" % (RESULTS_DIR, beam, utc_start, source)
    eph_fname = "%s/%s/%s/%s/pulsar.eph" % (RESULTS_DIR, beam, utc_start, source)
    par_fname = "%s/%s.par" % (PTUSE_FOLDING_DIR, source)
    if os.path.exists(freq_sum_fn):
        eph.load_from_archive_as_str(freq_sum_fn)
    elif os.path.exists(time_sum_fn):
        eph.load_from_archive_as_str(time_sum_fn)
    elif os.path.exists(eph_fname):
        eph.load_from_file(eph_fname)
    elif os.path.exists(par_fname):
        eph.load_from_file(par_fname)
    else:
        eph.load_from_string("")

    created_at = util_time.get_current_time()
    created_by = getpass.getuser()
    valid_from = util_time.get_time(0)
    valid_to = util_time.get_time(4294967295)
    comment = "Created by tables.ephemeris.new"

    # get_or_create in the mutation always creates, since the created_at, valid from times are always different
    # TODO filter on the actual ephemeris, but JSONfield filtering doesn't seem to work
    ephemerides = Ephemerides(client, url, token)
    ephemerides.set_field_names(literal, quiet)
    response = ephemerides.list_graphql(None, pulsar_id, eph.p0, eph.dm, eph.rm)
    encoded_ephemeris_id = get_id_from_listing(response, "ephemeris", listing="allEphemerides")
    if encoded_ephemeris_id:
        ephemeris_id = int(ephemerides.decode_id(encoded_ephemeris_id))
        logging.info("ephemeris_id=%d (pre-existing)" % ephemeris_id)
    else:
        response = ephemerides.create(
            pulsar_id,
            created_at,
            created_by,
            json.dumps(eph.ephem),
            eph.p0,
            eph.dm,
            eph.rm,
            comment,
            valid_from,
            valid_to,
        )
        ephemeris_id = get_id(response, "ephemeris")
        logging.info("ephemeris_id=%d" % (ephemeris_id))

    pipelines = Pipelines(client, url, token)
    # TODO check existing / understand why gein the mutation always creates
    response = pipelines.create(hdr.machine, "None", hdr.machine_version, created_at, created_by, hdr.machine_config)
    pipeline_id = get_id(response, "pipeline")
    logging.info("pipeline_id=%d" % (pipeline_id))

    embargo_us = 1e6 * 60 * 60 * 24 * 548
    location = "%s/%s/%s/%s/%s/" % (FOLDING_DIR, beam, source, utc_start, freq)
    results = json.dumps({"snr": float(obs_results.get("snr"))})

    processings = Processings(client, url, token)
    # TODO check existing / understand why the mutation always creates
    parent_processing_id = None
    utc_dt = datetime.strptime(f"{utc_start} +0000", "%Y-%m-%d-%H:%M:%S %z")
    embargo_end_dt = utc_dt + timedelta(microseconds=embargo_us)
    embargo_end = embargo_end_dt.strftime("%Y-%m-%dT%H:%M:%S")
    parent_id = -1  # this will get converted to null by the mutation

    response = processings.create(observation_id, pipeline_id, parent_id, embargo_end, location, "{}", "{}", results)
    processing_id = get_id(response, "processing")
    logging.info("processing_id=%d" % (processing_id))

    foldings = Foldings(client, url, token)
    # TODO check existing / understand why gein the mutation always creates
    response = foldings.create(
        processing_id, ephemeris_id, hdr.fold_nbin, hdr.fold_npol, hdr.fold_nchan, hdr.fold_dm, hdr.fold_tsubint
    )
    folding_id = get_id(response, "folding")
    logging.info("folding_id=%d" % (folding_id))

    # process image results for this observations
    pipelineimages = Pipelineimages(client, url, token)
    # TODO check existing / understand why gein the mutation always creates
    rank = 0
    for png in glob.glob("%s/*.*.png" % (results_dir)):
        # check image size
        if os.stat(png).st_size > 0:
            image_type = png[-11:-4]  # clunky!
            response = pipelineimages.create(png, image_type, rank, processing_id)
            pipeline_image_id = get_id(response, "pipelineimage")
            rank += 1
            logging.info("pipeline_image_id=%d" % (pipeline_image_id))
        else:
            logging.info("Ignoring empty pipeline image %s" % (png))


if __name__ == "__main__":
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

    format = "%(asctime)s : %(levelname)s : " + "%s/%s/%s/%s" % (beam, utc_start, source, freq) + " : %(msg)s"
    # logging.basicConfig(format=format,filename=LOG_FILE,level=logging.INFO)
    # logging.basicConfig(format=format, level=logging.DEBUG)

    client = GraphQLClient(args.url, args.verbose_client)

    main(beam, utc_start, source, freq, client, args.url, args.token)
