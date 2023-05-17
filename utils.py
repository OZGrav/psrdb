import argparse
import json
from os import environ


jwt_mutation = """
    mutation TokenAuth($username: String!, $password: String!) {
        tokenAuth(input:{username: $username, password: $password}) {
            token
            payload
            refreshExpiresIn
        }
    }
"""


def __create_viewer(client, django_user_model):
    username = "viewer"
    password = "reweiv"
    viewer = django_user_model.objects.create_user(username=username, password=password)
    return viewer


jwt_vars_viewer = """
    {
        "username": "viewer",
        "password":  "reweiv"
    }
"""

jwt_vars_creator = """
    {
        "username": "creator",
        "password":  "rotaerc"
    }
"""


def obtain_jwt_token(client, mutation, vars):
    # obtain the token
    payload = {"query": mutation, "variables": vars}
    response = client.post("/graphql/", payload)
    data = json.loads(response.content)["data"]
    jwt_token = data["tokenAuth"]["token"]
    return jwt_token


def obtain_default_args():
    class Struct(object):
        pass

    args = Struct()
    args.subcommand = None
    args.verbose = False
    args.very_verbose = False
    return args


def obtain_psrdb_parser(desc=""):
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("-t", "--token", nargs=1, default=environ.get("PSRDB_TOKEN"), help="JWT token")
    parser.add_argument("-u", "--url", nargs=1, default=environ.get("PSRDB_URL"), help="GraphQL URL")
    parser.add_argument(
        "-l",
        "--literal",
        action="store_true",
        default=False,
        help="Return literal IDs in tables instead of more human readable text",
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help="Increase verbosity")
    parser.add_argument("-vv", "--very_verbose", action="store_true", default=False, help="Increase verbosity")
    return parser


def obtain_psrdb_subparser():
    parser = obtain_psrdb_parser()
    subparser = parser.add_subparsers(dest="command", required=True, help="Database models which can be interrogated")
    return subparser
