
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
    # Extract data from obs_data
    pulsar_id = decode_id(pulsar['id'])
    pulsar = pulsar['name']
    print(pulsar)
    comment = create_pulsar_paragraph(
        pulsar_names=pulsar,
        query=query,
        pulsar_paragraph=pulsar_paragraph,
    )[0]
    puslar_client.update(pulsar_id, pulsar, comment)