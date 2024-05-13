import os
import shutil
import logging
import argparse
from glob import glob
from datetime import datetime

from psrdb.graphql_client import GraphQLClient
from psrdb.utils.other import setup_logging, get_rest_api_id, get_graphql_id, decode_id
from psrdb.tables.pulsar import Pulsar
from psrdb.tables.pulsar_fold_summary import PulsarFoldSummary
from psrdb.tables.toa import Toa
from psrdb.tables.observation import Observation


PROJECT_TIMING_DATA = {
    "PTA": {
        "npol": [
            1,
        ],
        "nchan": [
            1,
            16,
            32,
        ],
        "nsub": [
            "1",
            "mode",
        ],
    },
    "TPA": {
        "npol": [
            1,
        ],
        "nchan": [
            1,
            16,
        ],
        "nsub": [
            "1",
        ],
    },
    "RelBin": {
        "npol": [
            1,
        ],
        "nchan": [
            1,
            16,
        ],
        "nsub": [
            "1",
            "max",
        ],
    },
}

TEST_PULSARS = [
    "J1744-1134",
    "J1909-3744",
    "J1112-6613",
    "J1302-6350",
    "J1157-5112",
    "J1933-6211",
]


def main():
    parser = argparse.ArgumentParser(description="Create a data release of MeerTIME timing data.")
    parser.add_argument("-d", "--date", type=str, help="Date of the finial date to use for the release in the format 'YYYY-MM-DD-HH:MM:SS'")
    parser.add_argument("-n", "--name", type=str, help="Name of the release")
    parser.add_argument("--dir", type=str, help="Output directory", default="/fred/oz005/users/nswainst/test_data_release/")
    parser.add_argument("--test", action='store_true', help="Only run for test pulsars")
    args = parser.parse_args()

    base_dir = os.path.abspath(args.dir)
    print(f"Creating data release {args.name} with final date {args.date} in the directory {base_dir}")

    # PSRDB setup
    logger = setup_logging(level=logging.INFO)
    client = GraphQLClient(os.environ.get("PSRDB_URL"),  os.environ.get("PSRDB_TOKEN"), logger=logger)
    pulsar_client = Pulsar(client)
    pulsar_client.get_dicts = True
    pfs_client = PulsarFoldSummary(client)
    pfs_client.get_dicts = True
    pfs_client.set_use_pagination(True)
    obs_client = Observation(client)
    obs_client.get_dicts = True
    obs_client.set_use_pagination(True)
    toa_client = Toa(client)

    # Get all pulsars
    pulsar_data = pulsar_client.list()
    pulsars = []
    for pulsar in pulsar_data:
        pulsars.append(pulsar['name'])

    # Get all PulsarFoldSummary and sort pulsars into their projects
    project_pulsars = {}
    pfs_data = pfs_client.list(
        main_project="MeerTIME",
    )
    for pfs in pfs_data:
        projects = pfs['allProjects']
        pulsar = pfs['pulsar']['name']
        for project in projects.split(", "):
            if project not in project_pulsars.keys():
                project_pulsars[project] = []
            if pulsar not in project_pulsars[project]:
                project_pulsars[project].append(pulsar)


    os.chdir(base_dir)
    os.makedirs("decimated", exist_ok=True)

    # Get the decimated files
    for pulsar in pulsars:
        if args.test and pulsar not in TEST_PULSARS:
            continue
        os.chdir(f"{base_dir}/decimated")
        os.makedirs(pulsar, exist_ok=True)
        os.chdir(f"{base_dir}/decimated/{pulsar}")
        print(f"Getting decimated files for {pulsar}")
        obs_data = obs_client.list(
            pulsar_name=[pulsar],
            main_project="MeerTIME",
            utce=args.date,
            obs_type='fold',
        )
        for obs in obs_data:
            utc = datetime.strptime(
                obs['utcStart'],
                '%Y-%m-%dT%H:%M:%S+00:00',
            ).strftime('%Y-%m-%d-%H:%M:%S')
            beam = obs['beam']
            decimated_16ch = glob(f"/fred/oz005/timing_processed/{pulsar}/{utc}/{beam}/decimated/{pulsar}_{utc}_*_chopped.16ch_1p_1t.ar")[0]
            print(f"cp {decimated_16ch} {base_dir}/decimated/{pulsar}")
            shutil.copyfile(decimated_16ch, f"{base_dir}/decimated/{pulsar}/{os.path.basename(decimated_16ch)}")
            decimated_1ch = glob(f"/fred/oz005/timing_processed/{pulsar}/{utc}/{beam}/decimated/{pulsar}_{utc}_*_chopped.1ch_1p_1t.ar")[0]
            print(f"cp {decimated_1ch} {base_dir}/decimated/{pulsar}")
            shutil.copyfile(decimated_1ch, f"{base_dir}/decimated/{pulsar}/{os.path.basename(decimated_1ch)}")


    # Get the ToAs
    for project in project_pulsars.keys():
        if project not in PROJECT_TIMING_DATA.keys():
            print(f"Skipping project {project}")
            continue
        print(f"\nProject: {project}, running for {len(project_pulsars[project])} pulsars: {project_pulsars[project]}")
        # Make directory and move into it
        os.chdir(base_dir)
        os.makedirs(project, exist_ok=True)
        for pulsar in project_pulsars[project]:
            if args.test and pulsar not in TEST_PULSARS:
                continue
            os.chdir(f"{base_dir}/{project}")
            os.makedirs(pulsar, exist_ok=True)
            os.chdir(f"{base_dir}/{project}/{pulsar}")

            for npol in PROJECT_TIMING_DATA[project]["npol"]:
                for nchan in PROJECT_TIMING_DATA[project]["nchan"]:
                    for nsub in PROJECT_TIMING_DATA[project]["nsub"]:
                        print(f"  Pulsar: {pulsar}, npol: {npol}, nchan: {nchan}, nsub: {nsub}")

                        # Filtered file
                        toa_client.download(
                            pulsar=pulsar,
                            project_short=project,
                            npol=npol,
                            obs_nchan=nchan,
                            nsub_type=nsub,
                            utce=args.date,
                            exclude_badges=[
                                "Session Timing Jump",
                                "Session Sensitivity Reduction",
                                "Session RFI",
                                "DM Drift",
                            ]
                        )
                        os.rename(
                            f"toa_{pulsar}_{project}_{nsub}_nsub_nchan{nchan}_npol{npol}.tim",
                            f"toa_{pulsar}_{project}_flagged_{nsub}_nsub_nchan{nchan}_npol{npol}.tim",
                        )

                        # raw file
                        toa_client.download(
                            pulsar=pulsar,
                            project_short=project,
                            npol=npol,
                            obs_nchan=nchan,
                            nsub_type=nsub,
                            utce=args.date,
                        )
                        os.rename(
                            f"toa_{pulsar}_{project}_{nsub}_nsub_nchan{nchan}_npol{npol}.tim",
                            f"toa_{pulsar}_{project}_raw_{nsub}_nsub_nchan{nchan}_npol{npol}.tim",
                        )
            print(f"  Pulsar: {pulsar}")


if __name__ == "__main__":
    main()