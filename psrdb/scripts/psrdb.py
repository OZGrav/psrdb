#!/usr/bin/env python
import sys

from psrdb.graphql_table import GraphQLTable
from psrdb.graphql_client import GraphQLClient

from psrdb.tables.pulsar import Pulsar
from psrdb.tables.telescope import Telescope
from psrdb.tables.main_project import MainProject
from psrdb.tables.project import Project
from psrdb.tables.ephemeris import Ephemeris
from psrdb.tables.template import Template
from psrdb.tables.calibration import Calibration
from psrdb.tables.observation import Observation
from psrdb.tables.pipeline_run import PipelineRun
from psrdb.tables.pulsar_fold_result import PulsarFoldResult
from psrdb.tables.pipeline_image import PipelineImage
from psrdb.tables.toa import Toa
from psrdb.tables.residual import Residual


def main():
    parser = GraphQLTable.get_default_parser()
    subparsers = parser.add_subparsers(
        dest="command", help="Database models which can be interrogated"
    )
    subparsers.required = True

    tables = [
        Pulsar,
        Telescope,
        MainProject,
        Project,
        Ephemeris,
        Template,
        Calibration,
        Observation,
        PipelineRun,
        PulsarFoldResult,
        PipelineImage,
        Toa,
        Residual,
    ]

    configured = []
    for t in tables:
        n = t.get_name()
        p = subparsers.add_parser(n, help=t.get_description())
        t.configure_parsers(p)
        configured.append({"name": n, "parser": p, "table": t})

    args = parser.parse_args()
    if args.url is None:
        raise RuntimeError("GraphQL URL must be provided in $PSRDB_URL or via -u option")
    if args.token is None:
        raise RuntimeError("GraphQL Token must be provided in $PSRDB_TOKEN or via -t option")

    for c in configured:
        if args.command == c["name"]:
            client = GraphQLClient(args.url, args.token, verbose=args.verbose)
            table = c["table"](client)
            table.set_quiet(args.quiet)
            table.set_use_pagination(True)
            response = table.process(args)
            if 'status_code' in dir(response):
                if response.status_code not in (200, 201):
                    logger.error(f"Query failed with the error code {response.status_code}, error:")
                    print(json.loads(response.content))
                    sys.exit(response.status_code)
                if args.verbose:
                    import json
                    print(response.status_code)
                    print(json.loads(response.content))


if __name__ == "__main__":
    main()
