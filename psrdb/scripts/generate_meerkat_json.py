#!/usr/bin/env python

import os
import sys
import glob
import json
import shlex
import logging
import subprocess
from decouple import config
from datetime import datetime

import psrchive as psr

from psrdb.utils import header


CALIBRATIONS_DIR = config("CALIBRATIONS_DIR", "/fred/oz005/users/aparthas/reprocessing_MK/poln_calibration")
RESULTS_DIR = config("RESULTS_DIR", "/fred/oz005/kronos")
FOLDING_DIR = config("FOLDING_DIR", "/fred/oz005/timing")
SEARCH_DIR  = config("SEARCH_DIR",  "/fred/oz005/search")


def generate_obs_length(archive):
    """
    Determine the length of the observation from the input archive file
    """

    ar = psr.Archive_load(archive)
    ar = ar.total()
    return ar.get_first_Integration().get_duration()

def get_sf_length(sf_files):
    """
    Determine the length of input sf files with the vap command
    """
    comm = f"vap -c length {' '.join(sf_files)}"
    args = shlex.split(comm)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, text=True, bufsize=1)
    vap_lines = []
    try:
        # Read and process the output line by line in real-time
        for line in iter(proc.stdout.readline, ''):
            print(line, end='', flush=True)
            vap_lines.append(line)

    # Handle Ctrl+C to gracefully terminate the subprocess
    except KeyboardInterrupt:
        logging.error("Process interrupted. Terminating...")
        sys.exit(1)

    finally:
        # Wait for the subprocess to complete
        proc.wait()

    lengths = []
    for line in vap_lines[1:]:
        if line == '':
            continue
        lengths.append(float(line.split()[1].strip()))
    return sum(lengths)


def get_calibration(utc_start):
    utc_start_dt = datetime.strptime(utc_start, "%Y-%m-%d-%H:%M:%S")
    auto_cal_epoch = "2020-04-04-00:00:00"
    auto_cal_epoch_dt = datetime.strptime(auto_cal_epoch, "%Y-%m-%d-%H:%M:%S")

    if utc_start_dt > auto_cal_epoch_dt:
        return ("pre", None)

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

    if ephemeris_text.startswith('\n'):
        # Remove newline character at start of output
        ephemeris_text = ephemeris_text.lstrip('\n')
    return ephemeris_text

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Ingest PTUSE fold mode observation")
    parser.add_argument("obs_header", type=str, help="obs.header file location")
    parser.add_argument("beam", type=int, help="beam number of the observation")
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default="./",
        help="Output directory of the meertime.json file",
    )
    parser.add_argument(
        "-n",
        "--output_name",
        type=str,
        default="meertime.json",
        help="Output name of the json file. Default is meertime.json",
    )
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
        logging.basicConfig(format=format, level=logging.INFO)

    # Load data from header
    obs_data = header.PTUSEHeader(args.obs_header)
    obs_data.parse()
    obs_data.set("BEAM", args.beam)

    if obs_data.schedule_block_id is None or obs_data.schedule_block_id == "None":
        logging.error(f"No schedule block ID for {obs_data.source} {obs_data.utc_start} {args.beam}")
        sys.exit(42)

    # Find raw archive and frequency summed files
    freq_summed_archive = f"{RESULTS_DIR}/{args.beam}/{obs_data.utc_start}/{obs_data.source}/freq.sum"
    if obs_data.obs_type == "fold":
        archive_files = glob.glob(f"{FOLDING_DIR}/{obs_data.source}/{obs_data.utc_start}/{args.beam}/*/*.ar")
    elif obs_data.obs_type == "search":
        archive_files = glob.glob(f"{SEARCH_DIR}/{obs_data.source}/{obs_data.utc_start}/{args.beam}/*/*.sf")
    if obs_data.obs_type != "cal":
        if not os.path.exists(freq_summed_archive) and not archive_files:
            logging.error(f"Could not find freq.sum and archive files for {obs_data.source} {obs_data.utc_start} {args.beam}")
            sys.exit(42)


    # Check if ther are freq.sum and archive files
    if obs_data.obs_type == "cal":
        obs_length = -1
    elif not os.path.exists(freq_summed_archive):
        logging.warning(f"Could not find freq.sum file for {obs_data.source} {obs_data.utc_start} {args.beam}")
        logging.warning("Finding observation length from archive files (This may take a while)")
        obs_length = get_sf_length(archive_files)
    else:
        # Grab observation length from the frequency summed archive
        obs_length = generate_obs_length(freq_summed_archive)

    cal_type, cal_location = get_calibration(obs_data.utc_start)

    ephemeris_text = get_archive_ephemeris(freq_summed_archive)

    meertime_dict = {
        "pulsarName": obs_data.source,
        "telescopeName": obs_data.telescope,
        "projectCode": obs_data.proposal_id,
        "schedule_block_id": obs_data.schedule_block_id,
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

    with open(os.path.join(args.output_dir, args.output_name), 'w') as json_file:
        json.dump(meertime_dict, json_file, indent=1)


if __name__ == "__main__":
    main()
