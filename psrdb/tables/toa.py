from datetime import datetime

from psrdb.graphql_table import GraphQLTable
from psrdb.utils.toa import toa_dict_to_line
from psrdb.utils.other import chunk_list
from psrdb.load_data import EXCLUDE_BADGES_CHOICES


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
            {"field": "id", "value": int(id) if id is not None else None},
            {"field": "pulsar", "value": pulsar},
            {"field": "pipelineRunId", "value": int(pipeline_run_id) if pipeline_run_id is not None else None},
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
        dmCorrected=False,
        nsub_type=None,
        npol=1,
        nchan=1,
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
        npol : int
            The number of Stokes polarisations.
        nchan : int
            The number of frequency channels.

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
            $nsubType: String!,
            $obsNpol: Int!,
            $obsNchan: Int!,
        ) {
            createToa (input: {
                pipelineRunId: $pipelineRunId,
                projectShort: $projectShort,
                templateId: $templateId,
                ephemerisText: $ephemerisText,
                toaLines: $toaLines,
                dmCorrected: $dmCorrected,
                nsubType: $nsubType,
                obsNpol: $obsNpol,
                obsNchan: $obsNchan,
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

        responses = []
        for toa_chunk in chunk_list(toa_lines, 1000):
            # Upload the toa
            self.variables = {
                'pipelineRunId': int(pipeline_run_id),
                'projectShort': project_short,
                'templateId': int(template_id),
                'ephemerisText': ephemeris_str,
                'toaLines': toa_chunk,
                'dmCorrected': dmCorrected,
                'nsubType': nsub_type,
                "obsNpol": npol,
                "obsNchan": nchan,
            }
            responses.append(self.mutation_graphql())
        if len(responses) == 0:
            return None
        else:
            return responses[-1]

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
            "id": int(id),
        }
        return self.mutation_graphql()

    def download(
        self,
        pulsar,
        id=None,
        pipeline_run_id=None,
        project_short=None,
        dm_corrected=None,
        nsub_type=None,
        obs_nchan=None,
        npol=None,
        exclude_badges=None,
        utcs=None,
        utce=None,
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
        nsub_type : str
            The method used to calculate the number of subintegrations. The choices are:
                "1": a single nsub,
                "max" the maximum number of subints possible for the observation based on the S/N ratio,
                "mode" the length of each subintegration is equal to the most common observation duration,
                "all": all available nsubs (no time scrunching).
        obs_nchan : int, optional
            Filter by the number of channels, by default None
        npol : int
            The number of Stokes polarisations.

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        filters = [
            {"field": "id", "value": int(id) if id is not None else None},
            {"field": "pulsar", "value": pulsar},
            {"field": "pipelineRunId", "value": int(pipeline_run_id) if pipeline_run_id is not None else None},
            {"field": "projectShort", "value": project_short},
            {"field": "dmCorrected", "value": dm_corrected},
            {"field": "nsubType", "value": nsub_type},
            {"field": "obsNchan", "value": obs_nchan},
            {"field": "obsNpol", "value": npol},
        ]
        if exclude_badges is not None:
            filters.append({"field": "excludeBadges", "value": exclude_badges})
        if utcs is not None:
            d = datetime.strptime(utcs, '%Y-%m-%d-%H:%M:%S')
            filters.append({"field": "utcStartGte", "value": f"{d.date()}T{d.time()}+00:00"})
        if utce is not None:
            d = datetime.strptime(utce, '%Y-%m-%d-%H:%M:%S')
            filters.append({"field": "utcStartLte", "value": f"{d.date()}T{d.time()}+00:00"})


        self.get_dicts = True
        toa_dicts = GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names, paginate_num=10000)

        # Create the output name
        output_name = f"toa_{pulsar}"
        if id is not None:
            output_name += f"_id{id}"
        if pipeline_run_id is not None:
            output_name += f"_pipeline_run_id{pipeline_run_id}"
        if project_short is not None:
            output_name += f"_{project_short}"
        if dm_corrected is not None and dm_corrected:
            output_name += "_dm_corrected"
        if nsub_type is not None:
            output_name += f"_{nsub_type}_nsub"
        if obs_nchan is not None:
            output_name += f"_nchan{obs_nchan}"
        if npol is not None:
            output_name += f"_npol{npol}"
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
                self.create(
                    args.pipeline_run_id,
                    args.ephemeris_id,
                    args.template_id,
                    toa_lines,
                    args.dm_corrected,
                    args.minimumNsubs,
                    args.maximumNsubs,
                    args.npol,
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
                args.nsub_type,
                args.nchan,
                args.npol,
                args.exclude_badges,
                args.utcs,
                args.utce,
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
        parser_download.add_argument("--project", type=str, help="The project short (e.g. PTA) [str]", required=True)
        parser_download.add_argument("--id", type=int, help="id of the toa [int]")
        parser_download.add_argument("--pipeline_run_id", type=int, help="pipeline_run_id of the toa [int]")
        parser_download.add_argument("--dm_corrected",  action="store_true", help="Return TOAs that have had their DM corrected for each observation [bool]")
        parser_download.add_argument(
            '--nsub_type',
            type=str,
            choices=['1', 'max', 'mode', 'all'],
            required=True,
            help='The method used to calculate the number of subintegrations. The choices are: '
                '"1": a single nsub, '
                '"max" the maximum number of subints possible for the observation based on the S/N ratio, '
                '"mode" the length of each subintegration is equal to the most common observation duration, '
                '"all": all available nsubs (no time scrunching).'
        )
        parser_download.add_argument("--nchan", type=int, help="Only use TOAs with this many subchans (common values are 1,4 and 16) [int]", required=True)
        parser_download.add_argument("--npol", type=int, help="Only use TOAs with this many stokes polarisations (4 for all and 1 for summed) [int]", required=True)
        parser_download.add_argument(
            '--exclude_badges',
            nargs='*',
            choices=EXCLUDE_BADGES_CHOICES,
            help=f'List of observation badges/flags to exclude from download ToAs. The choices are: {EXCLUDE_BADGES_CHOICES}'
        )
        parser_download.add_argument(
            "--utcs",
            type=str,
            help="Only use observations with utc_start greater than or equal to the timestamp [YYYY-MM-DD-HH:MM:SS]",
        )
        parser_download.add_argument(
            "--utce",
            type=str,
            help="Only use observations with utc_start less than or equal to the timestamp [YYYY-MM-DD-HH:MM:SS]",
        )

