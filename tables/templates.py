from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Templates(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)
        self.record_name = "template"

        # create a new record
        self.create_mutation = """
        mutation ($pulsar: Int!,  $frequency: Float!, $bandwidth: Float!, $created_at: DateTime!, $created_by: String!, $location: String!, $method: String!, $type: String!, $comment: String!) {
            createTemplate (input: {
                pulsar_id: $pulsar,
                frequency: $frequency,
                bandwidth: $bandwidth,
                created_at: $created_at,
                created_by: $created_by,
                location: $location,
                method: $method,
                type: $type,
                comment: $comment
            }) {
               template {
                   id,
               }
            }
        }
        """

        self.update_mutation = """
        mutation ($id: Int!, $pulsar: Int!,  $frequency: Float!, $bandwidth: Float!, $created_at: DateTime!, $created_by: String!, $location: String!, $method: String!, $type: String!, $comment: String!) {
            updateTemplate (id: $id, input: {
                pulsar_id: $pulsar,
                frequency: $frequency,
                bandwidth: $bandwidth,
                created_at: $created_at,
                created_by: $created_by,
                location: $location,
                method: $method,
                type: $type,
                comment: $comment
            }) {
                template {
                   id,
                   pulsar {id},
                   frequency,
                   bandwidth,
                   createdAt,
                   createdBy,
                   location,
                   method,
                   type,
                   comment
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteTemplate(id: $id) {
                ok
            }
        }
        """

        self.field_names = [
            "id",
            "pulsar {jname}",
            "frequency",
            "bandwidth",
            "createdAt",
            "createdBy",
            "location",
            "method",
            "type",
            "comment",
        ]
        self.literal_field_names = [
            "id",
            "pulsar {id}",
            "frequency",
            "bandwidth",
            "createdAt",
            "createdBy",
            "location",
            "method",
            "type",
            "comment",
        ]

    def list(self, id=None, pulsar_id=None, frequency=None, bandwidth=None):
        """Return a list of records matching the id and/or the pulsar id, frequency, bandwidth."""
        filters = [
            {"field": "pulsar_Id", "value": pulsar_id, "join": "Pulsars"},
            {"field": "frequency", "value": frequency, "join": None},
            {"field": "bandwidth", "value": bandwidth, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def update(self, id, pulsar, frequency, bandwidth, created_at, created_by, location, method, type, comment):
        self.update_variables = {
            "id": id,
            "pulsar": pulsar,
            "frequency": frequency,
            "bandwidth": bandwidth,
            "created_at": created_at,
            "created_by": created_by,
            "location": location,
            "method": method,
            "type": type,
            "comment": comment,
        }
        return self.update_graphql()

    def create(self, pulsar, frequency, bandwidth, created_at, created_by, location, method, type, comment):
        self.create_variables = {
            "pulsar": pulsar,
            "frequency": frequency,
            "bandwidth": bandwidth,
            "created_at": created_at,
            "created_by": created_by,
            "location": location,
            "method": method,
            "type": type,
            "comment": comment,
        }
        return self.create_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(
                args.pulsar,
                args.frequency,
                args.bandwidth,
                args.created_at,
                args.created_by,
                args.location,
                args.method,
                args.type,
                args.comment,
            )
        elif args.subcommand == "update":
            return self.update(
                args.id,
                args.pulsar,
                args.frequency,
                args.bandwidth,
                args.created_at,
                args.created_by,
                args.location,
                args.method,
                args.type,
                args.comment,
            )
        elif args.subcommand == "list":
            return self.list(args.id, args.pulsar, args.frequency, args.bandwidth)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "templates"

    @classmethod
    def get_description(cls):
        return "A pulsar template/standard"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Templates model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing ephemerides")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list template matching the id [int]")
        parser_list.add_argument(
            "--pulsar", metavar="PSR", type=int, help="list template matching the pulsar id [int]"
        )
        parser_list.add_argument(
            "--frequency",
            metavar="FREQ",
            type=float,
            help="list template matching the pulsar frequency in MHz [float]]",
        )
        parser_list.add_argument(
            "--bandwidth", metavar="BW", type=float, help="list template matching the pulsar bandwidth in MHz [float]]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new template")
        parser_create.add_argument(
            "pulsar", metavar="ID", type=int, help="id of the pulsar for which this template applies [int]"
        )
        parser_create.add_argument(
            "frequency", metavar="FREQ", type=float, help="frequency of this template in MHz [float]"
        )
        parser_create.add_argument(
            "bandwidth", metavar="BW", type=float, help="bandwidth of this template in MHz [float]"
        )
        parser_create.add_argument(
            "created_at", metavar="DATE", type=str, help="template creation date [YYYY-MM-DDTHH:MM:SS+000:00]"
        )
        parser_create.add_argument("created_by", metavar="AUTHOR", type=str, help="creator of the template [str]")
        parser_create.add_argument(
            "location", metavar="LOC", type=str, help="filesystem location of the template [str]"
        )
        parser_create.add_argument("method", metavar="METHOD", type=str, help="method (TBC) of the template [str]")
        parser_create.add_argument("type", metavar="TYPE", type=str, help="type (TBC) of the template [str]")
        parser_create.add_argument("comment", metavar="COMMENT", type=str, help="comment about the template [str]")

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update an existing template")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the template to update [int]")
        parser_update.add_argument(
            "pulsar", metavar="ID", type=int, help="id of the pulsar for which this template applies [int]"
        )
        parser_update.add_argument(
            "frequency", metavar="FREQ", type=float, help="frequency of this template in MHz [float]"
        )
        parser_update.add_argument(
            "bandwidth", metavar="BW", type=float, help="bandwidth of this template in MHz [float]"
        )
        parser_update.add_argument(
            "created_at", metavar="DATE", type=str, help="template creation date [YYYY-MM-DDTHH:MM:SS+000:00]"
        )
        parser_update.add_argument("created_by", metavar="AUTHOR", type=str, help="creator of the template [str]")
        parser_update.add_argument(
            "location", metavar="LOC", type=str, help="filesystem location of the template [str]"
        )
        parser_update.add_argument("method", metavar="METHOD", type=str, help="method (TBC) of the template [str]")
        parser_update.add_argument("type", metavar="TYPE", type=str, help="type (TBC) of the template [str]")
        parser_update.add_argument("comment", metavar="COMMENT", type=str, help="comment about the template [str]")

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing template")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the template to update [int]")


if __name__ == "__main__":
    parser = Templates.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    t = Templates(client, args.url, args.token)
    t.process(args)
