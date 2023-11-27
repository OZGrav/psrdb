from psrdb.graphql_table import GraphQLTable
from psrdb.utils.toa import toa_dict_to_line


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the Toa database object on the command line in different ways based on the sub-commands.")
    Toa.configure_parsers(parser)
    return parser


class Toa(GraphQLTable):
    """Class for interacting with the Toa database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "toa"

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
        id=None,
        pulsar=None,
        pipeline_run_id=None,
        project_short=None,
        dm_corrected=None,
        minimum_nsubs=None,
        maximum_nsubs=None,
        obs_nchan=None,
    ):
        """Return a list of Toa information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        id : int, optional
            Filter by the database ID, by default None
        pulsar : str, optional
            Filter by the pulsar name, by default None
        pipeline_run_id : int, optional
            Filter by the pipeline run id, by default None
        project_short : str
            The project short name (e.g PTA).
        dm_corrected : bool, optional
            Filter by if the toa was DM corrected, by default None
        minimum_nsubs : bool, optional
            Filter by if the toa was generated with the minimum number of time subbands, by default None
        maximum_nsubs : bool, optional
            Filter by if the toa was generated with the maximum number of time subbands, by default None
        obs_nchan : int, optional
            Filter by the number of channels, by default None

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        filters = [
            {"field": "id", "value": id},
            {"field": "pulsar", "value": pulsar},
            {"field": "pipelineRunId", "value": pipeline_run_id},
            {"field": "projectShort", "value": project_short},
            {"field": "dmCorrected", "value": dm_corrected},
            {"field": "obsNchan", "value": obs_nchan},
        ]
        if minimum_nsubs:
            filters.append({"field": "minimumNsubs", "value": minimum_nsubs})
        if maximum_nsubs:
            filters.append({"field": "maximumNsubs", "value": maximum_nsubs})
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def create(
        self,
        pipeline_run_id,
        project_short,
        ephemeris,
        template_id,
        toa_lines,
        dmCorrected,
        minimumNsubs,
        maximumNsubs,
    ):
        """Create a new Toa database object.

        Parameters
        ----------
        pipeline_run_id : int
            The ID of the PipelineRun database object for this Toa.
        project_short : str
            The project short name (e.g PTA).
        ephemeris : str
            The path to the ephemeris file used to create the residuals.
        template_id : int
            The ID of the Template database object for this Toa.
        toa_lines : list
            A list containing the toa lines.
        dmCorrected : bool
            If the toa was DM corrected.
        minimumNsubs : bool
            If the toa was generated with the minimum number of time subbands.
        maximumNsubs : bool
            If the toa was generated with the maximum number of time subbands.

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "createToa"
        self.mutation = """
        mutation (
            $pipelineRunId: Int!,
            $projectShort: String!,
            $templateId: Int!,
            $ephemerisText: String!,
            $toaLines: [String]!,
            $dmCorrected: Boolean!,
            $minimumNsubs: Boolean!,
            $maximumNsubs: Boolean!,
        ) {
            createToa (input: {
                pipelineRunId: $pipelineRunId,
                projectShort: $projectShort,
                templateId: $templateId,
                ephemerisText: $ephemerisText,
                toaLines: $toaLines,
                dmCorrected: $dmCorrected,
                minimumNsubs: $minimumNsubs,
                maximumNsubs: $maximumNsubs,
            }) {
                toa {
                    id,
                }
            }
        }
        """
        # Read ephemeris file
        with open(ephemeris, "r") as f:
            ephemeris_str = f.read()
        # Upload the toa
        self.variables = {
            'pipelineRunId': pipeline_run_id,
            'projectShort': project_short,
            'templateId': template_id,
            'ephemerisText': ephemeris_str,
            'toaLines': toa_lines,
            'dmCorrected': dmCorrected,
            'minimumNsubs': minimumNsubs,
            'maximumNsubs': maximumNsubs,
        }
        return self.mutation_graphql()

    def delete(
        self,
        id,
    ):
        """Delete a Toa database object.

        Parameters
        ----------
        id : int
            The database ID

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "deleteToa"
        self.mutation = """
        mutation ($id: Int!) {
            deleteToa(id: $id) {
                ok
            }
        }
        """
        self.variables = {
            "id": id,
        }
        return self.mutation_graphql()

    def download(
        self,
        pulsar,
        id=None,
        pipeline_run_id=None,
        project_short=None,
        dm_corrected=None,
        minimum_nsubs=None,
        maximum_nsubs=None,
        obs_nchan=None,
    ):
        """Download a file containing ToAs based on the filters.

        Parameters
        ----------
        pulsar : str
            Filter by the pulsar name
        id : int, optional
            Filter by the database ID, by default None
        pipeline_run_id : int, optional
            Filter by the pipeline run id, by default None
        project_short : str
            The project short name (e.g PTA).
        dm_corrected : bool, optional
            Filter by if the toa was DM corrected, by default None
        minimum_nsubs : bool, optional
            Filter by if the toa was generated with the minimum number of time subbands, by default None
        maximum_nsubs : bool, optional
            Filter by if the toa was generated with the maximum number of time subbands, by default None
        obs_nchan : int, optional
            Filter by the number of channels, by default None

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        filters = [
            {"field": "id", "value": id},
            {"field": "pulsar", "value": pulsar},
            {"field": "pipelineRunId", "value": pipeline_run_id},
            {"field": "projectShort", "value": project_short},
            {"field": "dmCorrected", "value": dm_corrected},
            {"field": "obsNchan", "value": obs_nchan},
        ]
        if minimum_nsubs:
            filters.append({"field": "minimumNsubs", "value": minimum_nsubs})
        if maximum_nsubs:
            filters.append({"field": "maximumNsubs", "value": maximum_nsubs})

        self.get_dicts = True
        toa_dicts = GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

        # Create the output name
        output_name = f"toa_{pulsar}"
        if id is not None:
            output_name += f"_id{id}"
        if pipeline_run_id is not None:
            output_name += f"_pipeline_run_id{pipeline_run_id}"
        if dm_corrected is not None and dm_corrected:
            output_name += "_dm_corrected"
        if minimum_nsubs is not None and minimum_nsubs:
            output_name += "_minimum_nsubs"
        if maximum_nsubs is not None and maximum_nsubs:
            output_name += "_maximum_nsubs"
        if obs_nchan is not None:
            output_name += f"_nchan{obs_nchan}"
        output_name += ".tim"

        # Loop over the toas and dump them as a file
        with open(output_name, "w") as f:
            f.write("FORMAT 1\n")
            for toa_dict in toa_dicts:
                # Convert to toa format
                # del toa_dict["id"]
                del toa_dict["pipelineRun"]
                del toa_dict["ephemeris"]
                del toa_dict["template"]
                toa_dict["freq_MHz"] = toa_dict["freqMhz"]
                toa_dict["mjd_err"] = toa_dict["mjdErr"]
                del toa_dict["freqMhz"]
                del toa_dict["mjdErr"]
                toa_line = toa_dict_to_line(toa_dict)
                f.write(f"{toa_line}\n")
        return output_name


    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":

            with open(args.toa_path, "r") as f:
                toa_lines = f.readlines()
                for toa_line in toa_lines[1:]:
                    input_toa_line = toa_line.rstrip("\n")
                    self.create(
                        args.pipeline_run_id,
                        args.ephemeris_id,
                        args.template_id,
                        input_toa_line,
                        args.dm_corrected,
                        args.minimumNsubs,
                        args.maximumNsubs,
                    )
        elif args.subcommand == "delete":
            return self.delete(args.id)
        elif args.subcommand == "list":
            return self.list(args.id, args.processing, args.folding, args.ephemeris, args.template)
        elif args.subcommand == "download":
            return self.download(
                args.pulsar,
                args.id,
                args.pipeline_run_id,
                args.project,
                args.dm_corrected,
                args.minimum_nsubs,
                args.maximum_nsubs,
                args.nchan,
            )
        else:
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "toa"

    @classmethod
    def get_description(cls):
        return "A pulsar toa/standard"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Toa model parser")
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
        parser_list.add_argument("--id", metavar="ID", type=int, help="list toa matching the id [int]")
        parser_list.add_argument(
            "--processing", metavar="PROC", type=int, help="list toa matching the processing id [int]"
        )
        parser_list.add_argument("--folding", metavar="FOLD", type=int, help="list toa that used the folding id [int]")
        parser_list.add_argument(
            "--ephemeris", metavar="EPH", type=int, help="list toa matching the timing ephemeris id [int]"
        )
        parser_list.add_argument(
            "--template", metavar="TEMPL", type=int, help="list toa matching the template id [int]"
        )

        # create the parser for the "download" command
        parser_download = subs.add_parser("download", help="Download TOAs for a pulsar to a .tim file")
        parser_download.add_argument("pulsar", type=str, help="Name of the pulsar [str]")
        parser_download.add_argument("--project", type=str, help="The project short (e.g. PTA) [str]")
        parser_download.add_argument("--id", type=int, help="id of the toa [int]")
        parser_download.add_argument("--pipeline_run_id", type=int, help="pipeline_run_id of the toa [int]")
        parser_download.add_argument("--dm_corrected",  action="store_true", help="Return TOAs that have had their DM corrected for each observation [bool]")
        parser_download.add_argument("--minimum_nsubs", action="store_true", help="Only use TOAs with the minimum number of subints per observation (1) [bool]")
        parser_download.add_argument("--maximum_nsubs", action="store_true", help="Only use TOAs with the maximum number of subints per observation (can be 1 but is often more) [bool]")
        parser_download.add_argument("--nchan", type=int, help="Only use TOAs with this many subchans (common values are 1,4 and 16) [int]")

