import hashlib
import json

from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Ephemerides(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($pulsar: Int!, $created_at: DateTime!, $created_by: String!, $ephemeris: JSONString!, $p0: Decimal!, $dm: Float!, $rm: Float!, $comment: String!, $valid_from: DateTime!, $valid_to: DateTime!) {
            createEphemeris (input: {
                pulsar_id: $pulsar,
                created_at: $created_at,
                created_by: $created_by,
                ephemeris: $ephemeris,
                p0: $p0,
                dm: $dm,
                rm: $rm,
                comment: $comment,
                valid_from: $valid_from,
                valid_to: $valid_to
                }) {
                ephemeris {
                    id
                }    
            }
        }
        """

        self.update_mutation = """
        mutation ($id: Int!, $pulsar: Int!, $created_at: DateTime!, $created_by: String!, $ephemeris: JSONString!, $p0: Decimal!, $dm: Float!, $rm: Float!, $comment: String!, $valid_from: DateTime!, $valid_to: DateTime!) {
            updateEphemeris (id: $id, input: {
                pulsar_id: $pulsar,
                created_at: $created_at,
                created_by: $created_by,
                ephemeris: $ephemeris,
                p0: $p0,
                dm: $dm,
                rm: $rm,
                comment: $comment,
                valid_from: $valid_from,
                valid_to: $valid_to
                }) {
                ephemeris {
                    id,
                    pulsar {id},
                    createdAt,
                    createdBy,
                    ephemeris,
                    p0,
                    dm,
                    rm,
                    comment,
                    validFrom,
                    validTo
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteEphemeris(id: $id) {
                ok
            }
        }
        """

        self.field_names = [
            "id",
            "pulsar {jname}",
            "createdAt",
            "createdBy",
            "ephemeris",
            "p0",
            "dm",
            "rm",
            "comment",
            "validFrom",
            "validTo",
        ]
        self.literal_field_names = [
            "id",
            "pulsar {id}",
            "createdAt",
            "createdBy",
            "ephemeris",
            "p0",
            "dm",
            "rm",
            "comment",
            "validFrom",
            "validTo",
        ]

    def list(self, id=None, pulsar_id=None, p0=None, dm=None, rm=None, eph=None):
        """Return a list of records matching the id and/or the pulsar id, p0, dm, rm."""
        # P0 is stored with a maximum of 8 decimal places only
        m = 10 ** 8
        if p0 is None:
            p0_filtered = None
        else:
            p0_filtered = round(p0 * m) / m

        eph_hash = None
        if eph:
            # convert string to json dict, to ensure the hash matches
            eph_json = json.loads(eph)
            eph_hash = hashlib.md5(json.dumps(eph_json, sort_keys=True, indent=2).encode("utf-8")).hexdigest()

        filters = [
            {"field": "pulsar_Id", "value": pulsar_id, "join": "Pulsars"},
            {"field": "p0", "value": p0_filtered, "join": None},
            {"field": "dm", "value": dm, "join": None},
            {"field": "rm", "value": rm, "join": None},
            {"field": "ephemerisHash", "value": eph_hash, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def update(self, id, pulsar, created_at, created_by, ephemeris, p0, dm, rm, comment, valid_from, valid_to):
        self.update_variables = {
            "id": id,
            "pulsar": pulsar,
            "created_at": created_at,
            "created_by": created_by,
            "ephemeris": ephemeris,
            "p0": p0,
            "dm": dm,
            "rm": rm,
            "comment": comment,
            "valid_from": valid_from,
            "valid_to": valid_to,
        }
        return self.update_graphql()

    def create(self, pulsar, created_at, created_by, ephemeris, p0, dm, rm, comment, valid_from, valid_to):
        self.create_variables = {
            "pulsar": pulsar,
            "created_at": created_at,
            "created_by": created_by,
            "ephemeris": ephemeris,
            "p0": p0,
            "dm": dm,
            "rm": rm,
            "comment": comment,
            "valid_from": valid_from,
            "valid_to": valid_to,
        }
        return self.create_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(
                args.pulsar,
                args.created_at,
                args.created_by,
                args.ephemeris,
                args.p0,
                args.dm,
                args.rm,
                args.comment,
                args.valid_from,
                args.valid_to,
            )
        elif args.subcommand == "update":
            return self.update(
                args.id,
                args.pulsar,
                args.created_at,
                args.created_by,
                args.ephemeris,
                args.p0,
                args.dm,
                args.rm,
                args.comment,
                args.valid_from,
                args.valid_to,
            )
        elif args.subcommand == "list":
            return self.list(args.id, args.pulsar, args.p0, args.dm, args.rm, args.eph)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "ephemerides"

    @classmethod
    def get_description(cls):
        return "A pulsar ephemeris"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Ephemerides model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing ephemerides")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list ephemeris matching the id [int]")
        parser_list.add_argument(
            "--pulsar", metavar="PSR", type=int, help="list ephemeris matching the pulsar id [int]"
        )
        parser_list.add_argument(
            "--p0", metavar="P0", type=float, help="list ephemeris matching the pulsar P0 [float]"
        )
        parser_list.add_argument(
            "--dm", metavar="DM", type=float, help="list ephemeris matching the pulsar DM [float]"
        )
        parser_list.add_argument(
            "--rm", metavar="RM", type=float, help="list ephemeris matching the pulsar RM [float]"
        )
        parser_list.add_argument("--eph", metavar="EPH", type=str, help="list ephemeris matching the ephemeris [JSON]")

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new ephemeris")
        parser_create.add_argument(
            "pulsar", metavar="PSR", type=int, help="id of the pulsar to which this ephemeris relates [int]"
        )
        parser_create.add_argument(
            "created_at", metavar="DATE", type=str, help="creation date of the ephemeris [YYYY-MM-DDTHH:MM:SS+HH:MM]"
        )
        parser_create.add_argument("created_by", metavar="AUTHOR", type=str, help="creator of the ephemeris [str]")
        parser_create.add_argument("ephemeris", metavar="EPHEM", type=str, help="JSON containing the ephemeris [str]")
        parser_create.add_argument("p0", metavar="P0", type=float, help="period in the ephemeris [float]")
        parser_create.add_argument("dm", metavar="DM", type=float, help="DM in the ephemeris [float]")
        parser_create.add_argument("rm", metavar="RM", type=float, help="RM in the ephemeris [float]")
        parser_create.add_argument("comment", metavar="COMMENT", type=str, help="comment about the ephemeris [str]")
        parser_create.add_argument(
            "valid_from",
            metavar="FROM",
            type=str,
            help="start of the validity of the ephemeris [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )
        parser_create.add_argument(
            "valid_to", metavar="TO", type=str, help="end of the validity of the ephemeris [YYYY-MM-DDTHH:MM:SS+HH:MM]"
        )

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update an existing ephemeris")
        parser_update.add_argument("id", metavar="ID", type=int, help="database id of the ephemeris to update [int]")

        parser_update.add_argument(
            "pulsar", metavar="PSR", type=int, help="id of the pulsar to which this ephemeris relates [int]"
        )
        parser_update.add_argument(
            "created_at", metavar="DATE", type=str, help="creation date of the ephemeris [YYYY-MM-DDTHH:MM:SS+HH:MM]"
        )
        parser_update.add_argument("created_by", metavar="AUTHOR", type=str, help="creator of the ephemeris [str]")
        parser_update.add_argument("ephemeris", metavar="EPHEM", type=str, help="JSON containing the ephemeris [str]")
        parser_update.add_argument("p0", metavar="P0", type=float, help="period in the ephemeris [float]")
        parser_update.add_argument("dm", metavar="DM", type=float, help="DM in the ephemeris [float]")
        parser_update.add_argument("rm", metavar="RM", type=float, help="RM in the ephemeris [float]")
        parser_update.add_argument("comment", metavar="COMMENT", type=str, help="comment about the ephemeris [str]")
        parser_update.add_argument(
            "valid_from",
            metavar="FROM",
            type=str,
            help="start of the validity of the ephemeris [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )
        parser_update.add_argument(
            "valid_to", metavar="TO", type=str, help="end of the validity of the ephemeris [YYYY-MM-DDTHH:MM:SS+HH:MM]"
        )

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing ephemeris")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the ephemeris [int]")


if __name__ == "__main__":
    parser = Ephemerides.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    e = Ephemerides(client, args.url, args.token)
    e.process(args)
