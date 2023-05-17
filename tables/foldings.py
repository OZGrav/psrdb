from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Foldings(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($processing_id: Int!, $folding_ephemeris_id: Int!, $nbin: Int!, $npol: Int!, $nchan: Int!, $dm: Float!, $tsubint: Float!) {
            createFolding(input: { 
                processing_id: $processing_id,
                folding_ephemeris_id: $folding_ephemeris_id,
                nbin: $nbin,
                npol: $npol,
                nchan: $nchan,
                dm: $dm,
                tsubint: $tsubint
            }) {
                folding {
                    id
                }
            }
        }
        """
        # Update an existing record
        self.update_mutation = """
        mutation ($id: Int!, $processing_id: Int!, $folding_ephemeris_id: Int!, $nbin: Int!, $npol: Int!, $nchan: Int!, $dm: Float!, $tsubint: Float!) {
            updateFolding(id: $id, input: { 
                processing_id: $processing_id,
                folding_ephemeris_id: $folding_ephemeris_id,
                nbin: $nbin,
                npol: $npol,
                nchan: $nchan,
                dm: $dm,
                tsubint: $tsubint
            }) {
                folding {
                    id,
                    processing { id },
                    foldingEphemeris { id },
                    nbin,
                    npol,
                    nchan,
                    dm,
                    tsubint
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteFolding(id: $id) {
                ok
            }
        }
        """

        self.field_names = [
            "id",
            "processing { id }",
            "foldingEphemeris { id }",
            "nbin",
            "npol",
            "nchan",
            "dm",
            "tsubint",
        ]

    def list(self, id=None, processing_id=None, folding_ephemeris_id=None):
        """Return a list of records matching the id and/or the processing id, folding ephemeris id"""
        filters = [
            {"field": "processingId", "value": processing_id, "join": "Processings"},
            {"field": "foldingEphemerisId", "value": folding_ephemeris_id, "join": "Ephemerides"},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, processing, eph, nbin, npol, nchan, dm, tsubint):
        self.create_variables = {
            "processing_id": processing,
            "folding_ephemeris_id": eph,
            "nbin": nbin,
            "npol": npol,
            "nchan": nchan,
            "dm": dm,
            "tsubint": tsubint,
        }
        return self.create_graphql()

    def update(self, id, processing, eph, nbin, npol, nchan, dm, tsubint):
        self.update_variables = {
            "id": id,
            "processing_id": processing,
            "folding_ephemeris_id": eph,
            "nbin": nbin,
            "npol": npol,
            "nchan": nchan,
            "dm": dm,
            "tsubint": tsubint,
        }
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.processing, args.eph, args.nbin, args.npol, args.nchan, args.dm, args.tsubint)
        elif args.subcommand == "update":
            return self.update(
                args.id, args.processing, args.eph, args.nbin, args.npol, args.nchan, args.dm, args.tsubint
            )
        elif args.subcommand == "list":
            return self.list(args.id, args.processing, args.eph)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "foldings"

    @classmethod
    def get_description(cls):
        return "Folding of data to produce an archive."

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Foldings model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing foldings")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list foldings matching the id [int]")
        parser_list.add_argument(
            "--processing", metavar="PROC", type=int, help="list foldings matching the processing id [int]"
        )
        parser_list.add_argument(
            "--eph", type=int, metavar="EPH", help="list foldings matching the ephemeris id [int]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new folding")
        parser_create.add_argument("processing", metavar="PROC", type=int, help="processing id of the folding [int]")
        parser_create.add_argument("eph", metavar="EPH", type=int, help="ephemeris id of the folding [int]")
        parser_create.add_argument("nbin", metavar="NBIN", type=int, help="Number of bins in the folding [int]")
        parser_create.add_argument(
            "npol", metavar="NPOL", type=int, help="Number of polarisations in the folding [int]"
        )
        parser_create.add_argument("nchan", metavar="NCHAN", type=int, help="Number of channels in the folding [int]")
        parser_create.add_argument("dm", metavar="DM", type=float, help="DM of the folding [float]")
        parser_create.add_argument(
            "tsubint", metavar="TSUBINT", type=float, help="subintegration time of the folding [float]"
        )

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update the values of an existing folding")
        parser_update.add_argument("id", metavar="ID", type=int, help="database id of the folding [int]")
        parser_update.add_argument("processing", metavar="PROC", type=int, help="processing id of the folding [int]")
        parser_update.add_argument("eph", metavar="EPH", type=int, help="ephemeris id of the folding [int]")
        parser_update.add_argument("nbin", metavar="NBIN", type=int, help="Number of bins in the folding [int]")
        parser_update.add_argument(
            "npol", metavar="NPOL", type=int, help="Number of polarisations in the folding [int]"
        )
        parser_update.add_argument("nchan", metavar="NCHAN", type=int, help="Number of channels in the folding [int]")
        parser_update.add_argument("dm", metavar="DM", type=float, help="DM of the folding [float]")
        parser_update.add_argument(
            "tsubint", metavar="TSUBINT", type=float, help="subintegration time of the folding [float]"
        )

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing folding")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the folding [int]")


if __name__ == "__main__":
    parser = Foldings.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    f = Foldings(client, args.url, args.token)
    f.process(args)
