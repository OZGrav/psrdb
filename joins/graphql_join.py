import json
import logging
import requests as r
from base64 import b64decode, b64encode
import binascii
from graphql_client import GraphQLClient
from tables.graphql_table import GraphQLTable


class GraphQLJoin(GraphQLTable):
    """Abstract base class to perform create, update and select GraphQL queries"""

    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)
        self.table_name = None

    def build_filter_query(self, fields):
        query_name = f"all{self.table_name.title()}"

        clauses = []
        for f in fields:
            field = f["field"]
            join = f["join"]
            value = f["value"]
            if field.endswith("_Id"):
                id_encoded = b64encode(f"{join}Node:{value}".encode("ascii")).decode("utf-8")
                clauses.append(field + ': "' + id_encoded + '"')
            else:
                clauses.append(field + ': "' + value + '"')

        template = """
        query {
            %s (%s) {
                edges {
                    node {
                        %s
                    }
                }
            }
        }
        """
        delim = ",\n                "
        query = template % (query_name, ", ".join(clauses), delim.join(self.field_names))
        return query
