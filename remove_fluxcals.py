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

# LOG_FILE = "%s/%s" % (LOG_DIRECTORY, time.strftime("%Y-%m-%d_c2g_receiver.log"))


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


def main(source, url, verbose, token):

    client = GraphQLClient(url, verbose)

    sources = ["J1939-6342_S", "J1939-6342_N", "J1939-6342_O", "J0408-6545_N", "J0408-6545_O", "J0408-6545_S"]

    pulsars = Pulsars(client, url, token)
    pulsars.set_field_names(True, True)
    pulsar_ids = []
    for source in sources:
        pulsar_json = json.loads(pulsars.list_graphql(None, source).content)["data"]["allPulsars"]["edges"]
        print(pulsar_json)
        if len(pulsar_json) == 0:
            print("Warning: no pulsar with jname " + source + " existed in Pulsars")
            return
        if len(pulsar_json) > 1:
            print("Error: found " + str(len(pulsar_json)) + " pulsars with jname " + source + " existed in Pulsars")
            return

        pulsar_id_encoded = pulsar_json[0]["node"]["id"]
        pulsar_id = pulsars.decode_id(pulsar_id_encoded)
        pulsar_ids.append(pulsar_id)

    pulsartargets = Pulsartargets(client, url, token)
    pulsartargets.set_field_names(True, True)
    pulsar_target_ids = []

    ephemerides = Ephemerides(client, url, token)
    ephemerides.set_field_names(True, True)
    ephemeris_ids = []

    foldings = Foldings(client, url, token)
    foldings.set_field_names(True, True)
    folding_ids = []

    for pulsar_id in pulsar_ids:
        pulsar_target_json = json.loads(pulsartargets.list_graphql(None, None, None, pulsar_id, None).content)["data"][
            "allPulsartargets"
        ]["edges"]
        for elem in pulsar_target_json:
            pulsar_target_id = pulsartargets.decode_id(elem["node"]["id"])
            pulsar_target_ids.append(pulsar_target_id)
    print("pulsar_target_ids=" + str(pulsar_target_ids))

    for pulsar_id in pulsar_ids:
        ephemeris_json = json.loads(ephemerides.list_graphql(None, pulsar_id, None, None, None).content)["data"][
            "allEphemerides"
        ]["edges"]
        for elem in ephemeris_json:
            ephemeris_id_encoded = elem["node"]["id"]
            ephemeris_id = ephemerides.decode_id(ephemeris_id_encoded)
            ephemeris_ids.append(ephemeris_id)

    print("ephemeris_ids=" + str(ephemeris_ids))

    for ephemeris_id in ephemeris_ids:
        folding_ids_json = json.loads(foldings.list_graphql(None, None, ephemeris_id).content)["data"]["allFoldings"][
            "edges"
        ]
        for elem in folding_ids_json:
            folding_ids.append(foldings.decode_id(elem["node"]["id"]))

    print(
        "pulsar_ids={}, pulsar_target_ids={} ephemeris_id={} folding_ids={}".format(
            pulsar_ids, pulsar_target_ids, ephemeris_ids, folding_ids
        )
    )

    del foldings
    del ephemerides
    del pulsartargets
    del pulsars
    del client

    time.sleep(1)
    client = GraphQLClient(url, verbose)
    pulsars = Pulsars(client, url, token)
    pulsartargets = Pulsartargets(client, url, token)
    ephemerides = Ephemerides(client, url, token)
    foldings = Foldings(client, url, token)

    # Now have all required information to delete, dont delete processing_ids, they should be safe to keep

    for folding_id in folding_ids:
        print("deleting folding_id=" + str(folding_id))
        foldings.delete(folding_id)
    del foldings

    time.sleep(1)

    for ephemeris_id in ephemeris_ids:
        print("deleting ephemeris_id=" + str(ephemeris_id))
        ephemerides.delete(ephemeris_id)
    del ephemerides

    time.sleep(1)

    for pulsar_target_id in pulsar_target_ids:
        print("deleting pulsar_target_id=" + str(pulsar_target_id))
        pulsartargets.delete(pulsar_target_id)
    del pulsartargets

    time.sleep(1)

    for pulsar_id in pulsar_ids:
        print("deleting pulsar_id=" + str(pulsar_id))
        pulsars.delete(pulsar_id)
    del pulsars
    del client


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Delete PTUSE fluxcal pulsar")
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
    parser.add_argument("source", type=str, help="source of the obs")
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help="Increase verbosity")
    parser.add_argument(
        "-vc", "--verbose_client", action="store_true", default=False, help="Increase graphql client verbosity"
    )
    args = parser.parse_args()

    source = args.source

    format = "%(asctime)s : %(levelname)s : " + "%s" % (source) + " : %(msg)s"
    # logging.basicConfig(format=format,filename=LOG_FILE,level=logging.INFO)
    # logging.basicConfig(format=format, level=logging.DEBUG)

    main(source, args.url, args.verbose_client, args.token)
