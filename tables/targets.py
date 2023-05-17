from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Targets(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($name: String!, $raj: String!, $decj: String!) {
            createTarget(input: {
                name: $name,
                raj: $raj,
                decj: $decj
                }) {
                target {
                    id
                }
            }
        }
        """
        # Update an existing record
        self.update_mutation = """
        mutation ($id: Int!, $name: String!, $raj: String!, $decj: String!) {
            updateTarget(id: $id, input: {
                name: $name,
                raj: $raj,
                decj: $decj
                }) {
                target {
                    id,
                    name,
                    raj,
                    decj
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteTarget(id: $id) {
                ok
            }
        }
        """

        self.field_names = ["id", "name", "raj", "decj"]

    def list(self, id=None, name=None):
        """Return a list of records matching the id and/or the name."""
        filters = [
            {"field": "name", "value": name, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, name, raj, decj):
        self.create_variables = {"name": name, "raj": raj, "decj": decj}
        return self.create_graphql()

    def update(self, id, name, raj, decj):
        self.update_variables = {"id": id, "name": name, "raj": raj, "decj": decj}
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.name, args.raj, args.decj)
        elif args.subcommand == "update":
            return self.update(args.id, args.name, args.raj, args.decj)
        elif args.subcommand == "list":
            return self.list(args.id, args.name)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "targets"

    @classmethod
    def get_description(cls):
        return "J2000 position on the sky in RA and DEC with a target name"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Targets model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing targets")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list targets matching the id [int]")
        parser_list.add_argument("--name", metavar="NAME", type=str, help="list targets matching the name [str]")

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new target")
        parser_create.add_argument("name", metavar="NAME", type=str, help="name of the target [str]")
        parser_create.add_argument(
            "raj", metavar="RAJ", type=str, help="right ascension string in J2000 coordinates [str]"
        )
        parser_create.add_argument(
            "decj", metavar="DECJ", type=str, help="declincation string in J2000 coordnates [str]"
        )

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update the values of an existing target")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the target [int]")
        parser_update.add_argument("name", metavar="NAME", type=str, help="name of the target [str]")
        parser_update.add_argument(
            "raj", metavar="RAJ", type=str, help="right ascension string in J2000 coordinates [str]"
        )
        parser_update.add_argument(
            "decj", metavar="DECJ", type=str, help="declincation string in J2000 coordnates [str]"
        )

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing target")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the target [int]")


if __name__ == "__main__":

    parser = Targets.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    t = Targets(client, args.url, args.token)
    t.process(args)
