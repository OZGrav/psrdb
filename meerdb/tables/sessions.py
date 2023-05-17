from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Sessions(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($telescope: Int!, $start: DateTime!, $end: DateTime!) {
            createSession(input: {
                telescope_id: $telescope,
                start: $start,
                end: $end,
                }) {
                session {
                    id
                }
            }
        }
        """
        self.update_mutation = """
        mutation ($id: Int!, $telescope: Int!, $start: DateTime!, $end: DateTime!) {
            updateSession(id: $id, input: {
                telescope_id: $telescope,
                start: $start,
                end: $end,
                }) {
                session {
                    id,
                    telescope {id},
                    start,
                    end,
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteSession(id: $id) {
                ok
            }
        }
        """

        self.literal_field_names = ["id", "telescope {id} ", "start", "end"]
        self.field_names = ["id", "telescope {name} ", "start", "end"]

    def list(
        self,
        id=None,
        telescope_id=None,
        telescope_name=None,
        start_lte=None,
        start_gte=None,
        end_lte=None,
        end_gte=None,
    ):
        """Return a list of records matching the id and/or the provided arguments."""
        filters = [
            {"field": "telescope_Id", "value": telescope_id, "join": "Telescopes"},
            {"field": "telescope_Name", "value": telescope_name, "join": "Telescopes"},
            {"field": "start_Lte", "value": start_lte, "join": None},
            {"field": "start_Gte", "value": start_gte, "join": None},
            {"field": "end_Lte", "value": end_lte, "join": None},
            {"field": "end_Gte", "value": end_gte, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, telescope, start, end):
        self.create_variables = {
            "telescope": telescope,
            "start": start,
            "end": end,
        }
        return self.create_graphql()

    def update(self, id, telescope, start, end):
        self.update_variables = {
            "id": id,
            "telescope": telescope,
            "start": start,
            "end": end,
        }
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.telescope, args.start, args.end)
        elif args.subcommand == "update":
            return self.update(args.id, args.telescope, args.start, args.end)
        elif args.subcommand == "list":
            return self.list(
                args.id,
                args.telescope,
                args.telescope_name,
                args.start_lte,
                args.start_gte,
                args.end_lte,
                args.end_gte,
            )
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "sessions"

    @classmethod
    def get_description(cls):
        return "A session defined by a code, short name, embargo period and a description"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Sessions model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing sessions")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list sessions matching the id [int]")
        parser_list.add_argument(
            "--telescope", metavar="TEL", type=int, help="list sessions matching the telescope id [int]"
        )
        parser_list.add_argument(
            "--telescope_name", metavar="TELNAME", type=str, help="list sessions matching the telescope name [str]"
        )
        parser_list.add_argument(
            "--start_lte",
            metavar="SLTE",
            type=str,
            help="list sessions with start <= date [YYYY-MM-DDTHH:MM:SS+00:00]",
        )
        parser_list.add_argument(
            "--start_gte",
            metavar="SGTE",
            type=str,
            help="list sessions with start >= date [YYYY-MM-DDTHH:MM:SS+00:00]",
        )
        parser_list.add_argument(
            "--end_lte", metavar="ELTE", type=str, help="list sessions with end <= date [YYYY-MM-DDTHH:MM:SS+00:00]"
        )
        parser_list.add_argument(
            "--end_gte", metavar="EGTE", type=str, help="list sessions with end >= date [YYYY-MM-DDTHH:MM:SS+00:00]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new session")
        parser_create.add_argument("telescope", metavar="TEL", type=int, help="id of the telescope [int]")
        parser_create.add_argument(
            "start", metavar="START", type=str, help="start datetime of the session [YYYY-MM-DDTHH:MM:SS+00:00]"
        )
        parser_create.add_argument(
            "end", metavar="END", type=str, help="end datetime of the session [YYYY-MM-DDTHH:MM:SS+00:00]"
        )

        parser_update = subs.add_parser("update", help="update an existing session")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of existing session [int]")
        parser_update.add_argument("telescope", metavar="TEL", type=int, help="id of the telescope [int]")
        parser_update.add_argument(
            "start", metavar="START", type=str, help="start datetime of the session [YYYY-MM-DDTHH:MM:SS+00:00]"
        )
        parser_update.add_argument(
            "end", metavar="END", type=str, help="end datetime of the session [YYYY-MM-DDTHH:MM:SS+00:00]"
        )

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing session")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of existing session [int]")


if __name__ == "__main__":
    parser = Sessions.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    s = Sessions(client, args.url, args.token)
    s.process(args)
