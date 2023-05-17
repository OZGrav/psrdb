from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Filterbankings(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($processing_id: Int!, $nbit: Int!, $npol: Int!, $nchan: Int!, $dm: Float!, $tsamp: Float!) {
            createFilterbanking(input: { 
                processing_id: $processing_id,
                nbit: $nbit,
                npol: $npol,
                nchan: $nchan,
                dm: $dm,
                tsamp: $tsamp
            }) {
                filterbanking {
                    id
                }
            }
        }
        """
        # Update an existing record
        self.update_mutation = """
        mutation ($id: Int!, $processing_id: Int!, $nbit: Int!, $npol: Int!, $nchan: Int!, $dm: Float!, $tsamp: Float!) {
            updateFilterbanking(id: $id, input: { 
                processing_id: $processing_id,
                nbit: $nbit,
                npol: $npol,
                nchan: $nchan,
                dm: $dm,
                tsamp: $tsamp
            }) {
                filterbanking {
                    id,
                    processing { id },
                    nbit,
                    npol,
                    nchan,
                    dm,
                    tsamp
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteFilterbanking(id: $id) {
                ok
            }
        }
        """

        self.field_names = ["id", "processing { id }", "nbit", "npol", "nchan", "dm", "tsamp"]

    def list(self, id=None, processing=None):
        """Return a list of records matching the id and/or the processing id."""
        filters = [
            {"field": "processing", "value": processing, "join": "Processings"},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, processing, nbit, npol, nchan, dm, tsamp):
        self.create_variables = {
            "processing_id": processing,
            "nbit": nbit,
            "npol": npol,
            "nchan": nchan,
            "dm": dm,
            "tsamp": tsamp,
        }
        return self.create_graphql()

    def update(self, id, processing, nbit, npol, nchan, dm, tsamp):
        self.update_variables = {
            "id": id,
            "processing_id": processing,
            "nbit": nbit,
            "npol": npol,
            "nchan": nchan,
            "dm": dm,
            "tsamp": tsamp,
        }
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.processing, args.nbit, args.npol, args.nchan, args.dm, args.tsamp)
        elif args.subcommand == "update":
            return self.update(args.id, args.processing, args.nbit, args.npol, args.nchan, args.dm, args.tsamp)
        elif args.subcommand == "list":
            return self.list(args.id, args.processing)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "filterbankings"

    @classmethod
    def get_description(cls):
        return "Filterbanking of data to produce an archive."

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Filterbankings model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing pipelines")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list pipelines matching the id [int]")
        parser_list.add_argument(
            "--processing", metavar="PROCID", type=int, help="list pipelines matching the processing id [int]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new filterbanking")
        parser_create.add_argument(
            "processing", metavar="PROC", type=int, help="processing id of the filterbanking [int]"
        )
        parser_create.add_argument("nbit", metavar="NBIT", type=int, help="Number of bits in the filterbanking [int]")
        parser_create.add_argument(
            "npol", metavar="NPOL", type=int, help="Number of polarisations in the filterbanking [int]"
        )
        parser_create.add_argument(
            "nchan", metavar="NCHAN", type=int, help="Number of channels in the filterbanking [int]"
        )
        parser_create.add_argument("dm", metavar="DM", type=float, help="DM of the filterbanking [float]")
        parser_create.add_argument(
            "tsamp", metavar="TSAMP", type=float, help="sampling interval of the filterbanking [float]"
        )

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update the values of an existing filterbanking")
        parser_update.add_argument("id", metavar="ID", type=int, help="database id of the filterbanking [int]")
        parser_update.add_argument(
            "processing", metavar="PROC", type=int, help="processing id of the filterbanking [int]"
        )
        parser_update.add_argument("nbit", metavar="NBIT", type=int, help="Number of bits in the filterbanking [int]")
        parser_update.add_argument(
            "npol", metavar="NPOL", type=int, help="Number of polarisations in the filterbanking [int]"
        )
        parser_update.add_argument(
            "nchan", metavar="NCHAN", type=int, help="Number of channels in the filterbanking [int]"
        )
        parser_update.add_argument("dm", metavar="DM", type=float, help="DM of the filterbanking [float]")
        parser_update.add_argument(
            "tsamp", metavar="TSAMP", type=float, help="sampling interval of the filterbanking [float]"
        )

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing filterbanking")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the filterbanking [int]")


if __name__ == "__main__":
    parser = Filterbankings.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    f = Filterbankings(client, args.url, args.token)
    f.process(args)
