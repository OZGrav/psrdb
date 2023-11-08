from psrdb.graphql_table import GraphQLTable


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the PulsarFoldResult database object on the command line in different ways based on the sub-commands.")
    PulsarFoldResult.configure_parsers(parser)
    return parser


class PulsarFoldResult(GraphQLTable):
    """Class for interacting with the PulsarFoldResult database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "pulsar_fold_result"
        self.field_names = [
            "id",
            "observation { utcStart }",
            "observation { band }",
            "observation { duration }",
            "pipelineRun { id }",
            "pipelineRun { dm }",
            "pipelineRun { dmErr }",
            "pipelineRun { dmEpoch }",
            "pipelineRun { dmEpoch }",
            "pipelineRun { dmChi2r }",
            "pipelineRun { dmTres }",
            "pipelineRun { sn }",
            "pipelineRun { flux }",
            "pipelineRun { rm }",
            "pipelineRun { rmErr }",
            "pipelineRun { percentRfiZapped }",
        ]

    def list(
            self,
            pulsar=None,
            mainProject=None,
            utcStart=None,
            beam=None
        ):
        """Return a list of PulsarFoldResult information based on the `self.field_names` and filtered by the parameters.

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
            {"field": "pulsar", "value": pulsar},
            {"field": "mainProject", "value": mainProject},
            {"field": "utcStart", "value": utcStart},
            {"field": "beam", "value": beam},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def download(
            self,
            pulsar,
            mainProject=None,
            utcStart=None,
            beam=None
        ):
        # Grab a dictionary of the pulsar_fold_results
        filters = [
            {"field": "pulsar", "value": pulsar},
            {"field": "mainProject", "value": mainProject},
            {"field": "utcStart", "value": utcStart},
            {"field": "beam", "value": beam},
        ]
        self.get_dicts = True
        pulsar_fold_result_dicts = GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

        # Create the output name
        output_name = f"pulsar_fold_result_{pulsar}"
        if mainProject is not None and mainProject:
            output_name += f"_{mainProject}"
        if utcStart is not None and utcStart:
            output_name += f"_{utcStart}"
        if beam is not None and beam:
            output_name += f"_beam{beam}"
        output_name += ".csv"

        # Loop over the pulsar_fold_results and dump them as a file
        with open(output_name, "w") as f:
            f.write("ID,UTC Start,Observing band,Duration (s),DM (pc cm^-3),DM error (pc cm^-3),DM epoch (MJD),DM chi2r,DM tres,SN,Flux (mJy),RM (rad m^-2),RM error (rad m^-2),RFI zapped (%)\n")
            for pulsar_fold_result_dict in pulsar_fold_result_dicts:
                data_line = [
                    str(pulsar_fold_result_dict["id"]),
                    str(pulsar_fold_result_dict["observation"]["utcStart"]),
                    str(pulsar_fold_result_dict["observation"]["band"]),
                    str(pulsar_fold_result_dict["observation"]["duration"]),
                    str(pulsar_fold_result_dict["pipelineRun"]["dm"]),
                    str(pulsar_fold_result_dict["pipelineRun"]["dmErr"]),
                    str(pulsar_fold_result_dict["pipelineRun"]["dmEpoch"]),
                    str(pulsar_fold_result_dict["pipelineRun"]["dmChi2r"]),
                    str(pulsar_fold_result_dict["pipelineRun"]["dmTres"]),
                    str(pulsar_fold_result_dict["pipelineRun"]["sn"]),
                    str(pulsar_fold_result_dict["pipelineRun"]["flux"]),
                    str(pulsar_fold_result_dict["pipelineRun"]["rm"]),
                    str(pulsar_fold_result_dict["pipelineRun"]["rmErr"]),
                    str(pulsar_fold_result_dict["pipelineRun"]["percentRfiZapped"]),
                ]
                f.write(f"{','.join(data_line)}\n")
        return output_name


    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "list":
            return self.list(
                args.pulsar,
                args.mainProject,
                args.utcStart,
                args.beam,
            )
        elif args.subcommand == "download":
            return self.download(
                args.pulsar,
                args.mainProject,
                args.utcStart,
                args.beam,
            )
        else:
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "pulsar_fold_result"

    @classmethod
    def get_description(cls):
        return "A pulsar pulsar_fold_result/standard"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("PulsarFoldResult model parser")
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
        parser_list.add_argument("--pulsar", type=str, help="Name of the pulsar [str]")
        parser_list.add_argument("--mainProject", type=str, help="Name of the main project you want to filter pulsar_fold_results by [str]")
        parser_list.add_argument("--utcStart",  type=str, help="UTC start time you want the results of [str]")
        parser_list.add_argument("--beam", type=int, help="Beam number you want to filter pulsar_fold_results by [int]")

        # create the parser for the "download" command
        parser_download = subs.add_parser("download", help="Download TOAs for a pulsar to a .tim file")
        parser_download.add_argument("pulsar", type=str, help="Name of the pulsar [str]")
        parser_download.add_argument("--mainProject", type=str, help="Name of the main project you want to filter pulsar_fold_results by [str]")
        parser_download.add_argument("--utcStart",  type=str, help="UTC start time you want the results of [str]")
        parser_download.add_argument("--beam", type=int, help="Beam number you want to filter pulsar_fold_results by [int]")

