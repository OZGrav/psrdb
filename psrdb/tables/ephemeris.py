import json
import hashlib

from psrdb.graphql_table import GraphQLTable


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the Ephemeris database object on the command line in different ways based on the sub-commands.")
    Ephemeris.configure_parsers(parser)
    return parser


class Ephemeris(GraphQLTable):
    """Class for interacting with the Ephemeris database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "ephemeris"

        self.field_names = [
            "id",
            "pulsar {name}",
            "createdAt",
            "createdBy {email}",
            "ephemerisHash",
            "p0",
            "dm",
            "comment",
            "validFrom",
            "validTo",
        ]
        self.literal_field_names = [
            "id",
            "pulsar {id}",
            "createdAt",
            "createdBy {email}",
            "ephemerisHash",
            "p0",
            "dm",
            "comment",
            "validFrom",
            "validTo",
        ]

    def list(self, id=None, pulsar_id=None, p0=None, dm=None, eph=None):
        """Return a list of Ephemeris information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        id : int, optional
            Filter by the database ID, by default None.
        pulsar_id : int, optional
            Filter by the pulsar ID, by default None.
        p0 : float, optional
            Filter by the pulsar period, by default None.
        dm : float, optional
            Filter by the pulsar DM, by default None.
        eph : str, optional
            Filter by the ephemeris hash, by default None.

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
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
            {"field": "id", "value": id},
            {"field": "pulsar_Id", "value": pulsar_id},
            {"field": "p0", "value": p0_filtered},
            {"field": "dm", "value": dm},
            {"field": "ephemerisHash", "value": eph_hash},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def create(
            self,
            pulsar,
            ephemeris,
            project_code=None,
            project_short=None,
            comment=None,
        ):
        """Create a new Ephemeris database object.

        Parameters
        ----------
        pulsar : str
            The pulsar name.
        ephemeris : str
            The ephemeris text as a single string (includes new line characters).
        project_code : str, optional
            The project code, by default None
        project_short : str, optional
            The project short name (e.g PTA), by default None
        comment : str, optional
            A comment about the ephemeris, by default None

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "createEphemeris"
        self.mutation = """
        mutation (
            $pulsar: String!,
            $ephemerisText: String!,
            $projectCode: String,
            $projectShort: String,
            $comment: String,
        ) {
            createEphemeris (input: {
                pulsarName: $pulsar,
                ephemerisText: $ephemerisText,
                projectCode: $projectCode,
                projectShort: $projectShort,
                comment: $comment,
                }) {
                ephemeris {
                    id
                }
            }
        }
        """
        # Read ephemeris file
        with open(ephemeris, "r") as f:
            ephemeris_str = f.read()
        self.variables = {
            "pulsar": pulsar,
            "ephemerisText": ephemeris_str,
            "projectCode": project_code,
            "projectShort": project_short,
            "comment": comment,
        }
        return self.mutation_graphql()

    def update(self, id, pulsar, created_at, created_by, ephemeris, p0, dm, rm, comment, valid_from, valid_to):
        """Update a Ephemeris database object.

        Parameters
        ----------
        id : int
            The database ID
        pulsar : str
            The pulsar name.
        ephemeris : str
            The ephemeris text as a single string (includes new line characters).
        project_code : str, optional
            The project code, by default None
        project_short : str, optional
            The project short name (e.g PTA), by default None
        comment : str, optional
            A comment about the ephemeris, by default None

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "updateEphemeris"
        self.mutation = """
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
        self.variables = {
            "id": id,
            "pulsar": pulsar,
            "created_at": created_at,
            "created_by": created_by,
            "ephemeris": ephemeris,
            "p0": p0,
            "dm": dm,
            "comment": comment,
            "valid_from": valid_from,
            "valid_to": valid_to,
        }
        return self.mutation_graphql()

    def delete(self, id):
        """Delete a Ephemeris database object.

        Parameters
        ----------
        id : int
            The database ID

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "deleteEphemeris"
        self.mutation = """
        mutation ($id: Int!) {
            deleteEphemeris(id: $id) {
                ok
            }
        }
        """
        self.variables = {
            "id": id,
        }
        return self.mutation_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(
                args.pulsar,
                args.ephemeris_loc,
                args.project_code,
                args.comment,
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
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "ephemeris"

    @classmethod
    def get_description(cls):
        return "A pulsar ephemeris"

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

