from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Instrumentconfigs(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($name: String!, $bandwidth: Decimal!, $frequency: Decimal!, $nchan: Int!, $npol: Int!, $beam: String!) {
            createInstrumentconfig(input: {
                name: $name,
                bandwidth: $bandwidth,
                frequency: $frequency,
                nchan: $nchan,
                npol: $npol,
                beam: $beam
                }) {
                instrumentconfig {
                    id
                }
            }
        }
        """
        # Update an existing record
        self.update_mutation = """
        mutation ($id: Int!, $name: String!, $bandwidth: Decimal!, $frequency: Decimal!, $nchan: Int!, $npol: Int!, $beam: String!) {
            updateInstrumentconfig(id: $id, input: {
                name: $name,
                bandwidth: $bandwidth,
                frequency: $frequency,
                nchan: $nchan,
                npol: $npol,
                beam: $beam
                }) {
                instrumentconfig {
                    id,
                    name,
                    bandwidth,
                    frequency,
                    nchan,
                    npol,
                    beam
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteInstrumentconfig(id: $id) {
                ok
            }
        }
        """

        self.field_names = ["id", "name", "frequency", "bandwidth", "nchan", "npol", "beam"]

    def list(self, id=None, name=None, beam=None):
        """Return a list of records matching the id and/or the name, beam."""
        filters = [
            {"field": "name", "value": name, "join": None},
            {"field": "beam", "value": beam, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, name, bandwidth, frequency, nchan, npol, beam):
        self.create_variables = {
            "name": name,
            "bandwidth": bandwidth,
            "frequency": frequency,
            "nchan": nchan,
            "npol": npol,
            "beam": beam,
        }
        return self.create_graphql()

    def update(self, id, name, bandwidth, frequency, nchan, npol, beam):
        self.update_variables = {
            "id": id,
            "name": name,
            "bandwidth": bandwidth,
            "frequency": frequency,
            "nchan": nchan,
            "npol": npol,
            "beam": beam,
        }
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.name, args.bandwidth, args.frequency, args.nchan, args.npol, args.beam)
        elif args.subcommand == "update":
            return self.update(args.id, args.name, args.bandwidth, args.frequency, args.nchan, args.npol, args.beam)
        elif args.subcommand == "list":
            return self.list(args.id, args.name, args.beam)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "instrumentconfigs"

    @classmethod
    def get_description(cls):
        return "An instrument configuration defined by a name, bandwidth, frequency, nchan, npol, and beam"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("InstrumentConfigs model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing instrument configurations")
        parser_list.add_argument(
            "--id", metavar="ID", type=int, help="list instrument configuration matching the id [int]"
        )
        parser_list.add_argument(
            "--name", metavar="NAME", type=str, help="list instrument configuration matching the name [str]"
        )
        parser_list.add_argument(
            "--beam", metavar="BEAM", type=str, help="list instrument configuration matching the beam [str]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new instrument configuration")
        parser_create.add_argument("name", metavar="NAME", type=str, help="name of the instrument configuration [str]")
        parser_create.add_argument(
            "frequency", metavar="FREQ", type=float, help="frequency of the instrument configuration [float]"
        )
        parser_create.add_argument(
            "bandwidth", metavar="BW", type=float, help="bandwidth of the instrument configuration [float]"
        )
        parser_create.add_argument(
            "nchan", metavar="NCHAN", type=int, help="number of channels of the instrument configuration [int]"
        )
        parser_create.add_argument(
            "npol", metavar="NPOL", type=int, help="number of polarisation of the instrument configuration [int]"
        )
        parser_create.add_argument(
            "beam", metavar="BEAM", type=str, help="beam description of the instrument configuration [str]"
        )

        # create the parser for the "create" command
        parser_update = subs.add_parser("update", help="update an existing instrument configuration")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the instrument configuration [int]")
        parser_update.add_argument("name", metavar="NAME", type=str, help="name of the instrument configuration [str]")
        parser_update.add_argument(
            "frequency", metavar="FREQ", type=float, help="frequency of the instrument configuration [float]"
        )
        parser_update.add_argument(
            "bandwidth", metavar="BW", type=float, help="bandwidth of the instrument configuration [float]"
        )
        parser_update.add_argument(
            "nchan", metavar="NCHAN", type=int, help="number of channels of the instrument configuration [int]"
        )
        parser_update.add_argument(
            "npol", metavar="NPOL", type=int, help="number of polarisation of the instrument configuration [int]"
        )
        parser_update.add_argument(
            "beam", metavar="BEAM", type=str, help="beam description of the instrument configuration [str]"
        )

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing instrument configuration")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the instrument configuration [id]")


if __name__ == "__main__":

    parser = Instrumentconfigs.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    ic = Instrumentconfigs(client, args.url, args.token)
    ic.process(args)
