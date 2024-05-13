from datetime import datetime

from psrdb.graphql_table import GraphQLTable
from psrdb.load_data import EXCLUDE_BADGES_CHOICES


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the PulsarFoldSummary database object on the command line in different ways based on the sub-commands.")
    PulsarFoldSummary.configure_parsers(parser)
    return parser


class PulsarFoldSummary(GraphQLTable):
    """Class for interacting with the PulsarFoldSummary database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "pulsar_fold_summary"
        self.field_names = [
            "id",
            "pulsar { name }",
            "mainProject { name }",
            "firstObservation",
            "latestObservation",
            "latestObservationBeam",
            "timespan",
            "numberOfObservations",
            "totalIntegrationHours",
            "lastIntegrationMinutes",
            "allBands",
            "lastSn",
            "highestSn",
            "lowestSn",
            "avgSnPipe",
            "maxSnPipe",
            "mostCommonProject",
            "allProjects",
        ]

    def list(
            self,
            band=None,
            most_common_project=None,
            project=None,
            main_project=None,
        ):
        """Return a list of PulsarFoldSummary information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        pulsar : str, optional
            Filter by the pulsar name, by default None
        mainProject : str, optional
            Filter by the main project name, by default None
        utcStart : str, optional
            Filter by the utcStart, by default None
        beam : int, optional
            Filter by the beam number, by default None

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        filters = [
            {"field": "band", "value": band},
            {"field": "mostCommonProject", "value": most_common_project},
            {"field": "project", "value": project},
            {"field": "mainProject", "value": main_project},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)


    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "list":
            return self.list(
                args.band,
                args.most_common_project,
                args.project,
                args.main_project,
            )
        else:
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "pulsar_fold_summary"

    @classmethod
    def get_description(cls):
        return "A pulsar pulsar_fold_summary/standard"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("PulsarFoldSummary model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing PulsarFoldSummarys")
        parser_list.add_argument("--band", type=str, help="Filter by observing band")
        parser_list.add_argument("--most_common_project", type=str, help="Filter by the name of the most common project")
        parser_list.add_argument("--project",  type=str, help="Filter by summaries containing the project")
        parser_list.add_argument("--main_project", type=str, help="Filter by the name of the main project")

