import json
import logging
import requests as r
from base64 import b64decode, b64encode
import binascii
from graphql_client import GraphQLClient


class GraphQLTable:
    """Abstract base class to perform create, update and select GraphQL queries"""

    def __init__(self, client, url, token):

        # the graphQL client may also be a djangodb mock endpoint
        self.client = client
        self.url = url
        self.token = token
        if type(self.client) == GraphQLClient:
            self.header = {"Authorization": f"JWT {token}"}
        else:
            self.header = {"HTTP_AUTHORIZATION": f"JWT {token}"}

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
        self.table_name = self.__class__.__name__

        # record name is the singular form of the record
        records = self.__class__.__name__.lower()

        if records.endswith("ides"):
            self.record_name = records.rstrip("ides") + "is"
        elif records.endswith("es"):
            self.record_name = records.rstrip("es")
        elif records.endswith("s"):
            self.record_name = records.rstrip("s")
        else:
            raise RuntimeError("Could not determine singular form of record from " + records)

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

    def create_graphql(
        self,
    ):

        logging.debug(f"Using mutation {self.create_mutation}")
        logging.debug(f"Using mutation vars in a dict {self.create_variables}")

        payload = {"query": self.create_mutation, "variables": json.dumps(self.create_variables)}
        response = self.client.post(self.url, payload, **self.header)
        if response.status_code == 200:
            content = json.loads(response.content)
            if not "errors" in content.keys():
                for key in content["data"].keys():
                    record_set = content["data"][key]
                    if record_set.get(self.record_name):
                        if self.print_stdout:
                            print(record_set[self.record_name]["id"])
                    else:
                        logging.warning(f"Record {self.record_name} did not exist in returned json")
            else:
                logging.warning(f"Errors returned in content {content['errors']}")
        else:
            logging.warning(f"Bad response status_code={response.status_code}")
        return response

    def update_graphql(self, delim="\t"):

        logging.debug(f"Using mutation {self.update_mutation}")
        logging.debug(f"Using mutation vars dict {self.update_variables}")

        payload = {"query": self.update_mutation, "variables": json.dumps(self.update_variables)}
        response = self.client.post(self.url, payload, **self.header)
        if response.status_code == 200:
            content = json.loads(response.content)
            if not "errors" in content.keys():
                for key in content["data"].keys():
                    record_set = content["data"][key]
                    if "edges" in record_set.keys():
                        record_set = record_set["edges"]
                    if record_set.get(self.record_name):
                        self.print_record_set(record_set[self.record_name], delim)
                    else:
                        logging.warning(f"No record matching the update query was found, check the primary ID")
        return response

    def list_graphql(self, graphql_query, delim="\t"):
        graphql_query.set_field_list(self.field_names)
        graphql_query.set_use_pagination(self.paginate)
        print_headers = True
        cursor = None
        has_next_page = True
        result = []
        while has_next_page:
            query = graphql_query.paginate(cursor)
            logging.debug(f"Using query {query}")
            payload = {"query": query}
            response = self.client.post(self.url, payload, **self.header)
            has_next_page = False
            if response.status_code == 200:
                content = json.loads(response.content)
                if not "errors" in content.keys():
                    for key in content["data"].keys():
                        record_set = content["data"][key]
                        if record_set is None:
                            continue
                        if type(record_set) == dict:
                            if "pageInfo" in record_set.keys():
                                if content["data"][key]["pageInfo"]["hasNextPage"]:
                                    cursor = content["data"][key]["pageInfo"]["endCursor"]
                                    has_next_page = True
                            if "edges" in record_set.keys():
                                record_set = record_set["edges"]
                            if self.get_dicts:
                                result = result + record_set
                            self.print_record_set(record_set, delim, print_headers=print_headers)
                            print_headers = cursor is None

        if self.get_dicts:
            return result
        else:
            return response

    def get_graphql(self, graphql_query):
        graphql_query.set_field_list(self.field_names)
        graphql_query.set_use_pagination(self.paginate)
        print_headers = True
        cursor = None
        has_next_page = True
        result = {}
        while has_next_page:
            query = graphql_query.paginate(cursor)
            logging.debug(f"Using query {query}")
            payload = {"query": query}
            response = self.client.post(self.url, payload, **self.header)
            has_next_page = False
            if response.status_code == 200:
                content = json.loads(response.content)
                result.update(content)
                if not "errors" in content.keys():
                    for key in content["data"].keys():
                        record_set = content["data"][key]
                        if record_set is None:
                            continue
                        if type(record_set) == dict:
                            if "pageInfo" in record_set.keys():
                                if content["data"][key]["pageInfo"]["hasNextPage"]:
                                    cursor = content["data"][key]["pageInfo"]["endCursor"]
                                    has_next_page = True
                            if "edges" in record_set.keys():
                                record_set = record_set["edges"]
        return result

    def delete_graphql(self):
        logging.debug(f"Using mutation {self.delete_mutation}")
        logging.debug(f"Using mutation vars dict {self.delete_variables}")

        payload = {"query": self.delete_mutation, "variables": json.dumps(self.delete_variables)}
        response = self.client.post(self.url, payload, **self.header)
        if response.status_code == 200:
            content = json.loads(response.content)
            if not "errors" in content.keys():
                for key in content["data"].keys():
                    record_set = content["data"][key]
                    if self.record_name in record_set.keys():
                        print(record_set[self.record_name]["id"])
                    else:
                        logging.debug(f"Record {self.record_name} did not exist in returned json")
        else:
            logging.warning(f"Bad response status_code={response.status_code}")
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

    @classmethod
    def configure_logging(cls, args):
        format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        if args.verbose or args.very_verbose:
            logging.basicConfig(format=format, level=logging.DEBUG)
        else:
            logging.basicConfig(format=format, level=logging.INFO)

        if args.url is None:
            raise RuntimeError("GraphQL URL must be provided in $PSRDB_URL or via -u option")
        if args.token is None:
            raise RuntimeError("GraphQL Token must be provided in $PSRDB_TOKEN or via -t option")
