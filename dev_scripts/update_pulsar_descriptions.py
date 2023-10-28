
import os

import psrqpy
from pulsar_paragraph.pulsar_classes import PulsarParagraph
from pulsar_paragraph.pulsar_paragraph import create_pulsar_paragraph

from psrdb.tables.pulsar import Pulsar
from psrdb.graphql_client import GraphQLClient
from psrdb.utils.other import setup_logging, decode_id

# PSRDB setup
logger = setup_logging()
client = GraphQLClient(os.environ.get("PSRDB_URL"), False, logger=logger)
puslar_client = Pulsar(client, os.environ.get("PSRDB_TOKEN"))
puslar_client.get_dicts = True
puslar_client.set_use_pagination(True)

query = psrqpy.QueryATNF().pandas
pulsar_paragraph = PulsarParagraph()

# Query based on provided parameters
pulsar_data = puslar_client.list()
for pulsar in pulsar_data:
    if "_" in pulsar['name']:
        continue
    # Extract data from obs_data
    pulsar_id = decode_id(pulsar['id'])
    pulsar_name = pulsar['name']
    if pulsar_name == "J1402-5124":
        pulsar_name = "J1402-5021"
    elif pulsar_name == "J1803-3002":
        pulsar_name = "J1803-3002A"
    elif pulsar_name == "J1826-2413":
        pulsar_name = "J1826-2415"
    elif pulsar_name == "J1147-6608":
        pulsar_name = "J1146-6610"
    elif pulsar_name == "J0514-4407":
        pulsar_name = "J0514-4408"
    elif pulsar_name == "J1325-6256":
        pulsar_name = "J1325-6253"
    elif pulsar_name == "J0922-5202":
        pulsar_name = "J0921-5202"
    elif pulsar_name == "J1759-2402":
        pulsar_name = "J1759-24"
    elif pulsar_name == "J0024-7204AA":
        pulsar_name = "J0024-7204aa" # I think this one has been given a wrong ephemeris so will neeed to be fixed
    elif pulsar_name == "J1653-4518":
        pulsar_name = "J1653-45"
    elif pulsar_name == "J0837-2454":
        pulsar_name = "J0837-24"

    elif pulsar_name in [
            "J1939-6342", # J0437 mislabeled
            "J1924-2914", # J0437 mislabeled
            "J2052-3640", # J0437 mislabeled
            "J2214-3835", # J0437 mislabeled
            "J0710-1604", # Perhaps a candidate that wasn't detectable
            "J2003-0934", # A craft candidate
            "J1823-3022", # A trapum candidate
        ]:
        continue
    print(pulsar_name)
    comment = create_pulsar_paragraph(
        pulsar_names=[pulsar_name],
        query=query,
        pulsar_paragraph=pulsar_paragraph,
    )[0]
    # print(comment)
    puslar_client.update(pulsar_id, pulsar_name, comment)