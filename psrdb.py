#!/usr/bin/env python

import logging
from os import environ

import tables
from tables.graphql_table import GraphQLTable
import joins
from joins.graphql_join import GraphQLJoin
from graphql_client import GraphQLClient

if __name__ == "__main__":

    parser = GraphQLTable.get_default_parser()
    subparsers = parser.add_subparsers(dest="command", help="Database models which can be interrogated")
    subparsers.required = True

    tables = [
        tables.Basebandings,
        tables.Calibrations,
        tables.Collections,
        tables.Ephemerides,
        tables.Filterbankings,
        tables.Foldings,
        tables.Instrumentconfigs,
        tables.Launches,
        tables.Observations,
        tables.Processingcollections,
        tables.Processings,
        tables.Programs,
        tables.Projects,
        tables.Pulsars,
        tables.Pulsartargets,
        tables.Pipelineimages,
        tables.Pipelinefiles,
        tables.Pipelines,
        tables.Sessions,
        tables.Targets,
        tables.Telescopes,
        tables.Templates,
        tables.Toas,
    ]

    joins = [
        joins.FoldedObservations,
        joins.ProcessedObservations,
        joins.ToaedObservations,
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
