import json
import logging
import requests as r
from base64 import b64decode, b64encode
import binascii
from copy import copy

from psrdb.graphql_client import GraphQLClient
from psrdb.utils.other import to_camel_case


def generate_graphql_query(table_name, filters, conection_fields, node_fields):
    """Generate a GraphQL query for a table"""

    # From filter create query arguments
    arguments = []
    for f in filters:
        field = f["field"]
        value = f["value"]
        if value is not None:
            # if field.lower().endswith("id") or type(value) == int:
            #     # Convert encoded graphql id to an integer
            #     value = b64encode(f"{join}Node:{value}".encode("ascii")).decode("utf-8")
            if type(value) == str:
                arguments.append(f'{field}: "{value}"')
            elif type(value) == bool:
                arguments.append(f"{field}: {str(value).lower()}")
            else:
                arguments.append(f"{field}: {value}")
    # Prepare the arguments to the template format
    if len(arguments) > 0:
        arguments = ',\n        '.join(arguments)
        query_arguments = f"(\n        {arguments}\n    )"
    else:
        query_arguments = ""

    # Prepare the fields to the template format
    node_fields = "\n                ".join(node_fields)
    conection_fields= "\n        ".join(conection_fields)

    # Convert table name to camel case to match graphql query
    query_name = to_camel_case(table_name)

    # Combine everything into a query
    query = f"""query {{
    {query_name} {query_arguments} {{
        {conection_fields}
        edges {{
            node {{
                {node_fields}
            }}
        }}
    }}
}}
    """
    return query



class GraphQLTable:
    """Abstract base class to perform create, update and select GraphQL queries"""

    def __init__(self, client, token, logger=None):

        # the graphQL client may also be a djangodb mock endpoint
        self.client = client
        self.token = token
        if type(self.client) == GraphQLClient:
            self.header = {"Authorization": f"JWT {token}"}
        else:
            self.header = {"HTTP_AUTHORIZATION": f"JWT {token}"}

        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

        self.get_dicts = False
        self.print_stdout = False
        self.create_mutation = None
        self.create_variables = {}
        self.update_mutation = None
        self.update_variables = {}
        self.list_query = None
        self.list_variables = None
        self.delete_mutation = None
        self.delete_variables = {}
        self.paginate = False

        self.cli_name = None
        self.cli_description = None
        self.quiet = False
        self.table_name = self.__class__.__name__[0].lower() + self.__class__.__name__[1:]

        # record name is the singular form of the record
        self.record_name = self.__class__.__name__.lower()

        # if records.endswith("ides"):
        #     self.record_name = records.rstrip("ides") + "is"
        # elif records.endswith("es"):
        #     self.record_name = records.rstrip("es")
        # elif records.endswith("s"):
        #     self.record_name = records.rstrip("s")
        # else:
        #     raise RuntimeError("Could not determine singular form of record from " + records)

        self.human_readable = True
        self.literal_field_names = []
        self.field_names = []

    def set_use_pagination(self, paginate):
        self.paginate = paginate

    def set_field_names(self, literal, id_only):
        if literal:
            if len(self.literal_field_names) > 0:
                self.field_names = self.literal_field_names
        if id_only:
            self.field_names = ["id"]
            self.quiet = True
        self.human_readable = not literal

    def encode_table_id(self, table, id):
        unencoded = f"{table}Node:{id}"
        return b64encode(unencoded.encode("ascii")).decode("utf-8")

    def encode_id(self, id):
        return self.encode_table_id(self.__class__.__name__, id)

    def decode_id(self, encoded):
        decoded = b64decode(encoded).decode("ascii")
        return decoded.split(":")[1]

    def delete(self, id):
        """Delete a record in the table matching the unique id."""
        self.delete_variables = {"id": id}
        return self.delete_graphql()

    def parse_mutation_response(self, response, record_name):
        """Parse the response from a create or update mutation and return the id of the record"""
        if response.status_code == 200:
            content = json.loads(response.content)
            print(content)
            if not "errors" in content.keys():
                for key in content["data"].keys():
                    record_set = content["data"][key]
                    if record_set.get(record_name):
                        nodes = record_set[record_name]
                        for node in nodes:
                            if self.print_stdout:
                                print(node["id"])
                    else:
                        self.logger.warning(f"Record {record_name} did not exist in returned json")
            else:
                self.logger.warning(f"Errors returned in content {content['errors']}")
        else:
            self.logger.warning(f"Bad response status_code={response.status_code}")
        return None

    def create_graphql(self):

        self.logger.debug(f"Using mutation {self.create_mutation}")
        self.logger.debug(f"Using mutation vars in a dict {self.create_variables}")

        payload = {"query": self.create_mutation, "variables": json.dumps(self.create_variables)}
        response = self.client.post(payload, **self.header)
        self.parse_mutation_response(response, self.record_name)
        return response

    def update_graphql(self, delim="\t"):

        self.logger.debug(f"Using mutation {self.update_mutation}")
        self.logger.debug(f"Using mutation vars dict {self.update_variables}")

        payload = {"query": self.update_mutation, "variables": json.dumps(self.update_variables)}
        response = self.client.post(payload, **self.header)
        self.parse_mutation_response(response, self.record_name)
        return response

    def delete_graphql(self):
        self.logger.debug(f"Using mutation {self.delete_mutation}")
        self.logger.debug(f"Using mutation vars dict {self.delete_variables}")

        payload = {"query": self.delete_mutation, "variables": json.dumps(self.delete_variables)}
        response = self.client.post(payload, **self.header)
        self.parse_mutation_response(response, self.record_name)
        return response

    def list_graphql(self, table_name, input_filters, input_connection_fields, input_node_fields):
        print_headers = True
        cursor = None
        has_next_page = True
        result = []
        while has_next_page:
            # Append page information to input filters and fields
            filters = copy(input_filters)
            filters.append({"field": "first", "value": 100})
            if cursor is not None:
                filters.append({"field": "after", "value": cursor})
            connection_fields = input_connection_fields
            connection_fields.append("pageInfo { hasNextPage endCursor }")

            # Generate the query
            query = generate_graphql_query(table_name, filters, connection_fields, input_node_fields)
            self.logger.debug(f"Using query: {query}")

            # Send the query
            payload = {"query": query}
            response = self.client.post(payload, **self.header)
            has_next_page = False
            if response.status_code == 200:
                content = json.loads(response.content)
                self.logger.debug(f"Response content: {content}")
                if "errors" not in content.keys():
                    data = content["data"][to_camel_case(table_name)]
                    # Get next page info (if available)
                    if data["pageInfo"]["hasNextPage"]:
                        cursor = data["pageInfo"]["endCursor"]
                        has_next_page = True

                    for node in data["edges"]:
                        if self.get_dicts:
                            result.append(node["node"])
                        self.print_record_set(node["node"], "\t", print_headers=print_headers)
                        print_headers = cursor is None

        if self.get_dicts:
            return result
        else:
            return response

    def print_record_set_fields(self, prefix, record_set, delim):
        fields = []
        if "node" in record_set.keys():
            record_set = record_set["node"]
        for k in record_set.keys():
            if type(record_set[k]) is dict:
                if prefix is None:
                    fields.extend(self.print_record_set_fields(k, record_set[k], delim))
                else:
                    fields.extend(self.print_record_set_fields(prefix + "_" + k, record_set[k], delim))
            else:
                if prefix is None:
                    fields.append(str(k))
                else:
                    fields.append(prefix + "_" + str(k))
        if prefix is None and self.print_stdout:
            print(delim.join(fields))
        return fields

    def get_record_set_value(self, key, value):
        if key == "id":
            try:
                result = self.decode_id(value)
            except (binascii.Error, UnicodeDecodeError):
                result = value
        elif type(value) is dict:
            k = list(value.keys())[0]
            v = list(value.values())[0]
            result = self.get_record_set_value(k, v)
        else:
            result = value
        return str(result)

    def print_record_set_values(self, prefix, record_set, delim):
        values = []
        if "node" in record_set.keys():
            record_set = record_set["node"]
        for k in record_set.keys():
            if type(record_set[k]) is dict:
                if prefix is None:
                    values.extend(self.print_record_set_values(k, record_set[k], delim))
                else:
                    values.extend(self.print_record_set_values(k + "_" + prefix, record_set[k], delim))
            else:
                values.append(self.get_record_set_value(k, record_set[k]))
        if prefix is None and self.print_stdout:
            print(delim.join(values))
        return values

    def print_record_set(self, record_set, delim, print_headers=True):
        num_records = len(record_set)
        if num_records == 0:
            return
        if type(record_set) == list:
            if not self.quiet and print_headers:
                self.print_record_set_fields(None, record_set[0], delim)
            for record in record_set:
                self.print_record_set_values(None, record, delim)
        elif type(record_set) == dict:
            if not self.quiet and print_headers:
                self.print_record_set_fields(None, record_set, delim)
            self.print_record_set_values(None, record_set, delim)
        else:
            raise RuntimeError("did not understand type of recordset")

    @classmethod
    def get_default_parser(cls, desc=""):
        from argparse import ArgumentParser
        from os import environ

        parser = ArgumentParser(description=desc)
        parser.add_argument("-t", "--token", nargs=1, default=environ.get("PSRDB_TOKEN"), help="JWT token")
        parser.add_argument("-u", "--url", nargs=1, default=environ.get("PSRDB_URL"), help="GraphQL URL")
        parser.add_argument(
            "-l",
            "--literal",
            action="store_true",
            default=False,
            help="Return literal IDs in tables instead of more human readable text",
        )
        parser.add_argument("-q", "--quiet", action="store_true", default=False, help="Return ID only")
        parser.add_argument("-v", "--verbose", action="store_true", default=False, help="Increase verbosity")
        parser.add_argument("-vv", "--very_verbose", action="store_true", default=False, help="Increase verbosity")
        return parser
