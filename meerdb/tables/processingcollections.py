from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Processingcollections(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($processing: Int!, $collection: Int!) {
            createProcessingcollection(input: {
                processing_id: $processing,
                collection_id: $collection
                }) {
                processingcollection {
                    id
                }
            }
        }
        """
        # update an existing record
        self.update_mutation = """
        mutation ($id: Int!, $processing: Int!, $collection: Int!) {
            updateProcessingcollection(id: $id, input: {
                processing_id: $processing,
                collection_id: $collection
                }) {
                processingcollection {
                    id,
                    processing {id},
                    collection {id}
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteProcessingcollection(id: $id) {
                ok
            }
        }
        """

        self.literal_field_names = ["id", "processing {id}", "collection {id}"]
        self.field_names = ["id", "processing {id}", "collection {name}"]

    def list(self, id=None, processing=None):
        """Return a list of records matching the id and/or the processing id."""
        filters = [
            {"field": "processing", "value": processing, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, processing, collection):
        self.create_variables = {"processing": processing, "collection": collection}
        return self.create_graphql()

    def update(self, id, processing, collection):
        self.update_variables = {"id": id, "processing": processing, "collection": collection}
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.processing, args.collection)
        elif args.subcommand == "update":
            return self.update(args.id, args.processing, args.collection)
        elif args.subcommand == "list":
            return self.list(args.id, args.processing)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "processingcollections"

    @classmethod
    def get_description(cls):
        return "A relation between a processing and a collection"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("ProcessingCollections model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing processing collections")
        parser_list.add_argument(
            "--id", metavar="ID", type=int, help="list processing collections matching the id [int]"
        )
        parser_list.add_argument(
            "--processing",
            metavar="PROC",
            type=int,
            help="list processing collections matching the processing id [int]",
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new processing collection")
        parser_create.add_argument("processing", metavar="PROC", type=int, help="id of the processing [int]")
        parser_create.add_argument("collection", metavar="COLL", type=int, help="id of the collection [int]")

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update an existing processing collection")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the processingcollection [int]")
        parser_update.add_argument("processing", metavar="PROC", type=int, help="id of the processing [int]")
        parser_update.add_argument("collection", metavar="COLL", type=int, help="id of the collection [int]")

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing processing collection")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the processingcollection [int]")


if __name__ == "__main__":
    parser = Processingcollections.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    pc = Processingcollections(client, args.url, args.token)
    pc.process(args)
