from psrdb.graphql_table import GraphQLTable
from psrdb.utils.residual import residual_line_to_dict
from psrdb.utils.other import decode_id


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the Residual database object on the command line in different ways based on the sub-commands.")
    Residual.configure_parsers(parser)
    return parser


def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


class Residual(GraphQLTable):
    """Class for interacting with the Residual database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "residual"
        self.field_names = [
            "id",
            "pipelineRun{ id }",
            "ephemeris { id }",
            "template { id }",
            "archive",
            "freqMhz",
            "mjd",
            "mjdErr",
            "telescope",
            "fe",
            "be",
            "f",
            "bw",
            "tobs",
            "tmplt",
            "gof",
            "nbin",
            "nch",
            "chan",
            "rcvr",
            "snr",
            "length",
            "subint",
        ]

    def list(
        self,
        pulsar,
        project_short,
    ):
        """Return a list of Residual information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        id : int, optional
            Filter by the database ID, by default None
        pulsar : str, optional
            Filter by the pulsar name, by default None
        project_short : str, optional
            Filter by the project short code, by default None

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        filters = [
            {"field": "id", "value": id},
            {"field": "pulsar_Name", "value": pulsar},
            {"field": "projectShort", "value": project_short},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def create(
        self,
        residual_lines,
    ):
        """Create a new Residual database object.

        Parameters
        ----------
        residual_lines : list of str
            A list of strings containing the residual lines.

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "createResidual"
        self.mutation = """
        mutation (
            $residualLines: [String]!,
        ) {
            createResidual (input: {
                residualLines: $residualLines,
            }) {
                residual {
                    id,
                }
            }
        }
        """
        # Loop over the lines and grab the important info to reduce upload size
        residual_line_info = []
        for residual_line in residual_lines[1:]:
            residual_line = residual_line.rstrip("\n")
            # Loop over residual lines and turn into a dict
            residual_dict = residual_line_to_dict(residual_line)
            # return only important info as a comma sperated string
            residual_line_info.append(f"{decode_id(residual_dict['id'])},{residual_dict['mjd']},{residual_dict['residual']},{residual_dict['residual_error']},{residual_dict['residual_phase']}")
        # Upload the residuals 1000 at a time
        responses = []
        for residual_chunk in chunk_list(residual_line_info, 1000):
            self.variables = {
                'residualLines': residual_chunk,
            }
            responses.append(self.mutation_graphql())
        return responses[-1]

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            with open(args.residual_path, "r") as f:
                residual_lines = f.readlines()
                return self.create(
                    residual_lines,
                )
        elif args.subcommand == "list":
            return self.list(args.id, args.processing, args.folding, args.ephemeris, args.template)
        else:
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "residual"

    @classmethod
    def get_description(cls):
        return "A pulsar residual/standard"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Residual model parser")
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
        parser_list.add_argument("--id", metavar="ID", type=int, help="list residual matching the id [int]")
        parser_list.add_argument(
            "--processing", metavar="PROC", type=int, help="list residual matching the processing id [int]"
        )
        parser_list.add_argument("--folding", metavar="FOLD", type=int, help="list residual that used the folding id [int]")
        parser_list.add_argument(
            "--ephemeris", metavar="EPH", type=int, help="list residual matching the timing ephemeris id [int]"
        )
        parser_list.add_argument(
            "--template", metavar="TEMPL", type=int, help="list residual matching the template id [int]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="Create a new Residual")
        parser_create.add_argument(
            "residual_path", metavar="TOA", type=str, help="Path to the residual file [str]"
        )