from psrdb.graphql_table import GraphQLTable
from psrdb.graphql_query import graphql_query_factory


class Pulsar(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($name: String!, $comment: String) {
            createPulsar(input: {
                name: $name, comment: $comment
            }) {
                pulsar {
                    id
                }
            }
        }
        """
        # Update an existing record
        self.update_mutation = """
        mutation ($id: Int!, $name: String!, $comment: String) {
            updatePulsar(id: $id, input: {
                name: $name,
                comment: $comment
            }) {
                pulsar {
                    id,
                    name,
                    comment
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deletePulsar(id: $id) {
                ok
            }
        }
        """

        self.field_names = ["id", "name", "comment"]

    def list(self, id=None, name=None):
        """Return a list of records matching the id and/or the pulsar name."""
        filters = [
            {"field": "name", "value": name, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        print(graphql_query)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, name, comment=None):
        self.create_variables = {"name": name, "comment": comment}
        return self.create_graphql()

    def update(self, id, name, comment):
        self.update_variables = {"id": id, "name": name, "comment": comment}
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.name, args.comment)
        elif args.subcommand == "update":
            return self.update(args.id, args.name, args.comment)
        elif args.subcommand == "list":
            return self.list(args.id, args.name)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "pulsar"

    @classmethod
    def get_description(cls):
        return "A pulsar source defined by a J2000 name"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Pulsar model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        # create the parser for the "list" command
        parser_list = subs.add_parser("list", help="list existing Pulsars")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list Pulsars matching the id [int]")
        parser_list.add_argument("--name", metavar="name", type=str, help="list Pulsars matching the name [str]")

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new pulsar")
        parser_create.add_argument("name", metavar="name", type=str, help="name of the pulsar [str]")
        parser_create.add_argument("--comment", metavar="COMMENT", type=str, help="description of the pulsar [str]")

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update the values of an existing pulsar")
        parser_update.add_argument("id", metavar="ID", type=int, help="database id of the pulsar [int]")
        parser_update.add_argument("name", metavar="name", type=str, help="name of the pulsar [str]")
        parser_update.add_argument("comment", metavar="COMMENT", type=str, help="description of the pulsar [str]")

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing pulsar")
        parser_delete.add_argument("id", metavar="ID", type=int, help="database id of the pulsar [int]")


if __name__ == "__main__":

    parser = Pulsar.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from psrdb.graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    p = Pulsar(client, args.url, args.token)
    p.process(args)
