#!/usr/bin/env python

import os
import json
import logging
from decouple import config
from datetime import datetime, timezone

from psrdb.utils import header
from psrdb.utils.upload import generate_obs_length, get_archive_ephemeris
from psrdb.load_data import MOLONGLO_CALIBRATIONS


MOLONGLO_RESULTS_DIR = config("MOLONGLO_RESULTS_DIR", "/fred/oz002/ldunn/meertime_dataportal/data/post")


def get_calibration(utc_start):
    utc_start_dt = datetime.strptime(utc_start, "%Y-%m-%d-%H:%M:%S").replace(tzinfo=timezone.utc)
    with open(MOLONGLO_CALIBRATIONS) as f:
        for line in f.readlines():
            if line[0] == '#':
                continue

            date = utc_start_dt.strptime(line.split(' ')[0], '%Y-%m-%d').replace(tzinfo=timezone.utc)
            delta = date - utc_start_dt
            if delta.total_seconds() > 0:
                break

            calibration = line.split(' ')[1].strip()

    return calibration


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Ingest Molonglo fold mode observation")
    parser.add_argument("obs_header", type=str, help="obs.header file location")
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

    # Find raw archive and frequency summed files
    freq_summed_archive = f"{MOLONGLO_RESULTS_DIR}/{obs_data.source}/{obs_data.utc_start}/{obs_data.source}_{obs_data.utc_start}.FT"
    # Check if there are freq.sum and archive files
    obs_length = generate_obs_length(freq_summed_archive)

    cal_location = get_calibration(obs_data.utc_start)

    ephemeris_text = get_archive_ephemeris(freq_summed_archive)

    meertime_dict = {
        "pulsarName": obs_data.source,
        "telescopeName": "MONS",
        "projectCode": "MONSPSR_TIMING",
        "schedule_block_id": obs_data.schedule_block_id,
        "cal_type": "pre",
        "cal_location": cal_location,
        "frequency": obs_data.frequency,
        "bandwidth": obs_data.bandwidth,
        "nchan": obs_data.nchan,
        "beam": obs_data.beam,
        "nant": obs_data.nant,
        "nantEff": obs_data.nant,
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
