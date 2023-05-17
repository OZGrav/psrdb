"""
    CLI interface for the Basebandings model
"""

from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Basebandings(GraphQLTable):
    """
    Extends the GraphQLTable to provide the create, update and list command line interfaces
    the Baseband model.
    """

    def __init__(self, graphql_client, url, token):

        self.create_variables = None
        self.update_variables = None
        self.list_variables = None
        self.list_query = None

        GraphQLTable.__init__(self, graphql_client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($processing_id: Int!) {
            createBasebanding(input: { 
                processing_id: $processing_id,
            }) {
                basebanding {
                    id
                }
            }
        }
        """
        # Update an existing record
        self.update_mutation = """
        mutation ($id: Int!, $processing_id: Int!) {
            updateBasebanding(id: $id, input: { 
                processing_id: $processing_id,
            }) {
                basebanding {
                    id
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteBasebanding(id: $id) {
                ok
            }
        }
        """

        self.field_names = ["id", "processing { id }"]

    def list(self, id=None, processing_id=None):
        """Return a list of records matching the id and/or the processing id."""
        filters = [
            {"field": "processing", "value": processing_id, "join": "Processings"},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, processing):
        """Create a record using the processing id."""
        self.create_variables = {"processing_id": processing}
        return self.create_graphql()

    def update(self, id, processing):
        """Update a record matching the id with the processing id."""
        self.update_variables = {"id": id, "processing_id": processing}
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.processing)
        elif args.subcommand == "update":
            return self.update(args.id, args.processing)
        elif args.subcommand == "list":
            return self.list(args.id, args.processing)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "basebandings"

    @classmethod
    def get_description(cls):
        return "Basebanding of data to produce an archive."

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Basebandings model parser")
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
        parser_list.add_argument("--id", type=int, help="list pipelines matching the id [int]")
        parser_list.add_argument(
            "--processing", type=int, metavar="PROCID", help="list pipelines matching the processing id [int]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new basebanding")
        parser_create.add_argument(
            "processing", type=int, metavar="PROCID", help="processing id of the basebanding [int]"
        )

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update the values of an existing basebanding")
        parser_update.add_argument("id", type=int, metavar="ID", help="id of the basebanding [int]")
        parser_update.add_argument(
            "processing", type=int, metavar="PROCID", help="processing id of the basebanding [int]"
        )

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing basebanding")
        parser_delete.add_argument("id", type=int, metavar="ID", help="id of the basebanding [int]")


if __name__ == "__main__":
    parser = Basebandings.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    b = Basebandings(client, args.url, args.token)
    b.process(args)
