import requests

from psrdb.graphql_table import GraphQLTable


class Template(GraphQLTable):
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "template"
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

    def list(self, id=None, pulsar_id=None, frequency=None, bandwidth=None):
        """Return a list of records matching the id and/or the pulsar id, frequency, bandwidth."""
        filters = [
            {"field": "pulsar_Id", "value": pulsar_id},
            {"field": "frequency", "value": frequency},
            {"field": "bandwidth", "value": bandwidth},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def create(
            self,
            pulsar_name,
            band,
            template_path,
            project_code=None,
            project_short=None,
        ):
        # Open the file in binary mode
        with open(template_path, 'rb') as file:
            variables = {
                "pulsar_name": pulsar_name,
                "project_code": project_code,
                "project_short": project_short,
                "band": band,
            }
            files = {
                "template_upload": file,
            }
            # Post to the rest api
            response = requests.post(f'{self.client.rest_api_url}template/', data=variables, files=files)

        return response

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(
                args.pulsar,
                args.project_code,
                args.band,
                args.template_path,
            )
        elif args.subcommand == "list":
            return self.list(args.id, args.pulsar, args.frequency, args.bandwidth)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "template"

    @classmethod
    def get_description(cls):
        return "A pulsar template/standard"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Template model parser")
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
            "pulsar", metavar="PULSAR", type=str, help="Name of the pulsar for which this template applies [int]"
        )
        parser_create.add_argument(
            "band", metavar="BAND", type=str, help="Band of this template (e.g. LBAND) [str]"
        )
        parser_create.add_argument(
            "project_code", metavar="PROJECT", type=str, help="Code of the project [str]"
        )
        parser_create.add_argument(
            "template_path", metavar="PATH", type=str, help="Path to the template file [str]"
        )

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
    parser = Template.get_parsers()
    args = parser.parse_args()

    from psrdb.graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    t = Template(client, args.url, args.token)
    t.process(args)
