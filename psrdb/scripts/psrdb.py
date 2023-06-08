#!/usr/bin/env python

from psrdb.tables.graphql_table import GraphQLTable
from psrdb.graphql_client import GraphQLClient
from psrdb.tables.main_project import MainProject
from psrdb.tables.project import Project
from psrdb.tables.telescope import Telescope
from psrdb.tables.pulsar import Pulsar

#     Basebandings,
#     Calibrations,
#     Collections,
#     Ephemerides,
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
from psrdb.joins import FoldedObservations, ProcessedObservations, ToaedObservations


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
    ]

    joins = [
        FoldedObservations,
        ProcessedObservations,
        ToaedObservations,
    ]

    configured = []
    for t in tables:
        n = t.get_name()
        p = subparsers.add_parser(n, help=t.get_description())
        t.configure_parsers(p)
        configured.append({"name": n, "parser": p, "table": t})

    for j in joins:
        n = j.get_name()
        p = subparsers.add_parser(n, help=j.get_description())
        j.configure_parsers(p)
        configured.append({"name": n, "parser": p, "table": j})

    args = parser.parse_args()
    GraphQLTable.configure_logging(args)

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
