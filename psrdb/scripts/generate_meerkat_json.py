#!/usr/bin/env python

import glob
import logging
import time
import os
import json
import getpass
from datetime import datetime, timedelta
from decouple import config, Csv
import shlex
import subprocess

import psrchive as psr

from psrdb.util import header


CALIBRATIONS_DIR = config("CALIBRATIONS_DIR")
RESULTS_DIR = config("RESULTS_DIR")
FOLDING_DIRS = config("FOLDING_DIRS", cast=Csv())
PTUSE_FOLDING_DIR = config("PTUSE_FOLDING_DIR")
LOG_DIRECTORY = config("LOG_DIRECTORY")
LOG_FILE = f"{LOG_DIRECTORY}{time.strftime('%Y-%m-%d')}{config('LOG_FILENAME')}"


def generate_obs_length(freq_summed_archive):
    """
    Determine the length of the observation from the input archive file
    """

    ar = psr.Archive_load(freq_summed_archive)
    ar = ar.total()
    length = ar.get_first_Integration().get_duration()

    return length


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


def get_archive_ephemeris(freq_summed_archive):
    """
    Get the ephemeris from the archive file using the vap command.
    """
    comm = "vap -E {0}".format(freq_summed_archive)
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()
    ephemeris_text = proc.stdout.read().decode("utf-8")
    return ephemeris_text

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Ingest PTUSE fold mode observation")
    parser.add_argument("obs_header", type=str, help="obs.header file location")
    parser.add_argument("beam", type=str, help="beam number of the observation")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Increase verbosity",
    )
    args = parser.parse_args()

    # Set up logger
    format = "%(asctime)s : %(levelname)s : %(msg)s"
    if args.verbose:
        logging.basicConfig(format=format, level=logging.DEBUG)
    else:
        logging.basicConfig(format=format, filename=LOG_FILE, level=logging.INFO)

    # Load data from header
    obs_data = header.PTUSEHeader(args.obs_header)
    obs_data.parse()
    obs_data.set("BEAM", args.beam)
    # Grab observation length from the frequency summed archive
    freq_summed_archive = f"{RESULTS_DIR}/{args.beam}/{obs_data.utc_start}/{obs_data.source}/freq.sum"
    obs_length = generate_obs_length(freq_summed_archive)

    cal_type, cal_location = get_calibration(obs_data.utc_start)

    ephemeris_text = get_archive_ephemeris(freq_summed_archive)

    meertime_dict = {
        "pulsarName": obs_data.source,
        "telescopeName": obs_data.telescope,
        "projectCode": obs_data.proposal_id,
        "delaycal_id": obs_data.delaycal_id,
        "cal_type": cal_type,
        "cal_location": cal_location,
        "frequency": obs_data.frequency,
        "bandwidth": obs_data.bandwidth,
        "nchan": obs_data.nchan,
        "beam": args.beam,
        "nant": obs_data.nant,
        "nantEff": obs_data.nant_eff,
        "npol": obs_data.npol,
        "obsType": obs_data.obs_type,
        "utcStart": obs_data.utc_start,
        "raj": obs_data.ra,
        "decj": obs_data.dec,
        "duration": obs_length,
        "nbit": obs_data.nbit,
        "tsamp": obs_data.tsamp,
        "foldNbin": obs_data.fold_nbin,
        "foldNchan": obs_data.fold_nchan,
        "foldTsubint": obs_data.fold_tsubint,
        "filterbankNbit": obs_data.search_nbit,
        "filterbankNpol": obs_data.search_npol,
        "filterbankNchan": obs_data.search_nchan,
        "filterbankTsamp": obs_data.search_tsamp,
        "filterbankDm": obs_data.search_dm,
        "ephemerisText": ephemeris_text,
    }

    with open("meertime.json" 'w') as json_file:
        json.dump(meertime_dict, json_file)


if __name__ == "__main__":
    main()
