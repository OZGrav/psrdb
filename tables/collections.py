from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Collections(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($name: String!, $description: String!) {
            createCollection(input: {
                name: $name,
                description: $description
                }) {
                collection {
                    id
                }
            }
        }
        """
        # update an existing record
        self.update_mutation = """
        mutation ($id: Int!, $name: String!, $description: String!) {
            updateCollection(id: $id, input: {
                name: $name,
                description: $description
                }) {
                collection {
                    id,
                    name,
                    description
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteCollection(id: $id) {
                ok
            }
        }
        """

        self.field_names = ["id", "name", "description"]

    def list(self, id=None, name=None):
        """Return a list of records matching the id and/or the name."""
        filters = [
            {"field": "name", "value": name, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, name, description):
        self.create_variables = {
            "name": name,
            "description": description,
        }
        return self.create_graphql()

    def update(self, id, name, description):
        """Update the name and description for the specified id"""
        self.update_variables = {"id": id, "name": name, "description": description}
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.name, args.description)
        elif args.subcommand == "update":
            return self.update(args.id, args.name, args.description)
        elif args.subcommand == "list":
            return self.list(args.id, args.name)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "collections"

    @classmethod
    def get_description(cls):
        return "A collection of observation processings, defined by a name and a description"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Collections model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing collections")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list collections matching the id [int]")
        parser_list.add_argument("--name", metavar="NAME", type=str, help="list collections matching the name [str]")

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new collection")
        parser_create.add_argument("name", metavar="NAME", type=str, help="name of the collection [str]")
        parser_create.add_argument("description", metavar="DESC", type=str, help="description of the collection [str]")

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update an existing collection")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the collection [int]")
        parser_update.add_argument("name", metavar="NAME", type=str, help="name of the collection [str]")
        parser_update.add_argument("description", metavar="DESC", type=str, help="description of the collection [str]")

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing collection")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the collection [int]")


if __name__ == "__main__":
    parser = Collections.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    c = Collections(client, args.url, args.token)
    c.process(args)
