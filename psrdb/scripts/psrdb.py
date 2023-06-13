#!/usr/bin/env python

from psrdb.graphql_table import GraphQLTable
from psrdb.graphql_client import GraphQLClient

from psrdb.tables.main_project import MainProject
from psrdb.tables.project import Project
from psrdb.tables.telescope import Telescope
from psrdb.tables.pulsar import Pulsar
from psrdb.tables.ephemeris import Ephemeris
from psrdb.tables.calibration import Calibration

#     Basebandings,
#     Calibrations,
#     Collections,
#     Filterbankings,
#     Foldings,
#     Instrumentconfigs,
#     Launches,
#     Observations,
#     Processingcollections,
#     Processings,
#     Programs,
#     MainProject,
#     Project,
#     Pulsar,
#     Pulsartargets,
#     Pipelineimages,
#     Pipelinefiles,
#     Pipelines,
#     Sessions,
#     Targets,
#     Telescopes,
#     Templates,
#     Toas,
# )


def main():
    parser = GraphQLTable.get_default_parser()
    subparsers = parser.add_subparsers(
        dest="command", help="Database models which can be interrogated"
    )
    subparsers.required = True

    tables = [
        MainProject,
        Project,
        Telescope,
        Pulsar,
        Ephemeris,
        Calibration,
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
            client = GraphQLClient(args.url, args.very_verbose)
            table = c["table"](client, args.url, args.token)
            table.set_field_names(args.literal, args.quiet)
            table.set_use_pagination(True)
            response = table.process(args)
            if args.verbose or args.very_verbose:
                import json

                print(response.status_code)
                print(json.loads(response.content))


if __name__ == "__main__":
    main()
