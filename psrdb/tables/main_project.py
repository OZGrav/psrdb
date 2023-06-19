from psrdb.graphql_table import GraphQLTable
from psrdb.graphql_query import graphql_query_factory


class MainProject(GraphQLTable):
    def __init__(self, client, token):
        GraphQLTable.__init__(self, client, token)

        # create a new record
        self.create_mutation = """
        mutation ($telescope: String!, $name: String!) {
            createMainProject(input: {
                telescopeName: $telescope,
                name: $name,
                }) {
                mainproject {
                    id
                }
            }
        }
        """
        self.update_mutation = """
        mutation ($id: Int!, $telescope: String!, $name: String!) {
            updateMainProject(id: $id, input: {
                telescopeName: $telescope,
                name: $name,
                }) {
                mainproject {
                    id,
                    name,
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteMainProject(id: $id) {
                ok
            }
        }
        """

        self.literal_field_names = ["id", "telescope {id} ", "name"]
        self.field_names = ["id", "telescope {name} ", "name"]

    def list(self, id=None, telescope=None, name=None):
        """Return a list of records matching the id and/or the telescope id, name."""
        filters = [
            {"field": "telescope", "value": telescope, "join": "Telescopes"},
            {"field": "name", "value": name, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, telescope, name):
        self.create_variables = {
            "telescope": telescope,
            "name": name,
        }
        return self.create_graphql()

    def update(self, id, telescope, name):
        self.update_variables = {
            "id": id,
            "telescope": telescope,
            "name": name,
        }
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.telescope, args.name)
        elif args.subcommand == "update":
            return self.update(args.id, args.telescope, args.name)
        elif args.subcommand == "list":
            return self.list(args.id, args.telescope, args.name)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "mainproject"

    @classmethod
    def get_description(cls):
        return "A MainProject defined by a code, short name, embargo period and a description"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("MainProject model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing MainProject")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list MainProject matching the id [int]")
        parser_list.add_argument(
            "--telescope", metavar="TEL", type=int, help="list MainProject matching the telescope id [int]"
        )
        parser_list.add_argument("--name", metavar="NAME", type=str, help="list MainProject matching the name [str]")

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new MainProject")
        parser_create.add_argument("telescope", metavar="TEL", type=str, help="name of the telescope [str]")
        parser_create.add_argument("name", metavar="NAME", type=str, help="of the MainProject [str]")

        parser_update = subs.add_parser("update", help="update an existing MainProject")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of existing MainProject [int]")
        parser_update.add_argument("telescope", metavar="TEL", type=int, help="name of the telescope [str]")
        parser_update.add_argument("name", metavar="NAME", type=str, help="of the MainProject [str]")

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing MainProject")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of existing MainProject [int]")


if __name__ == "__main__":
    parser = MainProject.get_parsers()
    args = parser.parse_args()

    from psrdb.graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    p = MainProject(client, args.url, args.token)
    p.process(args)
