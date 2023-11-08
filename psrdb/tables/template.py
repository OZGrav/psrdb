import requests

from psrdb.graphql_table import GraphQLTable


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the Template database object on the command line in different ways based on the sub-commands.")
    Template.configure_parsers(parser)
    return parser


class Template(GraphQLTable):
    """Class for interacting with the Template database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
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

    def list(self, id=None, pulsar_name=None, band=None, project_short=None):
        """Return a list of Template information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        id : int, optional
            Filter by the database ID, by default None
        pulsar_name : str, optional
            Filter by the pulsar name, by default None
        band : str, optional
            Filter by the band, by default None
        project_short : str, optional
            Filter by the project short name, by default None

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        filters = [
            {"field": "id", "value": id},
            {"field": "pulsar_Name", "value": pulsar_name},
            {"field": "band", "value": band},
            {"field": "project_Short", "value": project_short},
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
        """Create a new Template database object.

        Parameters
        ----------
        pulsar_name : str
            The name of the pulsar.
        band : str
            The band of the template (e.g. SBAND).
        template_path : str
            The path to the template file.
        project_code : str, optional
            The code of the project, by default None
        project_short : str, optional
            The short name of the project (e.g. PTA), by default None

        Returns
        -------
        client_response:
            A client response object.
        """
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
            raise RuntimeError(f"{args.subcommand} command is not implemented")

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