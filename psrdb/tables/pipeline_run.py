import json

from psrdb.graphql_table import GraphQLTable


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the PipelineRun database object on the command line in different ways based on the sub-commands.")
    PipelineRun.configure_parsers(parser)
    return parser


class PipelineRun(GraphQLTable):
    """Class for interacting with the PipelineRun database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "pipeline_run"
        self.field_names = [
            "id",
            "observation { id }",
            "template { id }",
            "ephemeris { id }",
            "pipelineName",
            "pipelineDescription",
            "pipelineVersion",
            "jobState",
            "location",
            "dm",
            "dmErr",
            "dmEpoch",
            "dmChi2r",
            "dmTres",
            "sn",
            "flux",
            "rm",
            "percentRfiZapped",
            "configuration",
        ]

    def list(
        self,
        id=None,
        observation_id=None,
        ephemeris_id=None,
        template_id=None,
        pipelineName=None,
        pipelineDescription=None,
        pipelineVersion=None,
        jobState=None,
        location=None,
        dm=None,
        dmErr=None,
        dmEpoch=None,
        dmChi2r=None,
        dmTres=None,
        sn=None,
        flux=None,
        rm=None,
        percentRfiZapped=None,
    ):
        """Return a list of PipelineRun information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        id : int, optional
            Filter by the database ID, by default None
        observation_id : int, optional
            Filter by the Observation database ID, by default None
        ephemeris_id : int, optional
            Filter by the Ephemeris database ID, by default None
        template_id : int, optional
            Filter by the Template database ID, by default None
        pipelineName : str, optional
            Filter by the pipeline name, by default None
        pipelineDescription : str, optional
            Filter by the pipeline description, by default None
        pipelineVersion : str, optional
            Filter by the pipeline version, by default None
        jobState : str, optional
            Filter by the job state, by default None
        location : str, optional
            Filter by the location, by default None
        dm : float, optional
            Filter by the dm, by default None
        dmErr : float, optional
            Filter by the dmErr, by default None
        dmEpoch : float, optional
            Filter by the dmEpoch, by default None
        dmChi2r : float, optional
            Filter by the dmChi2r, by default None
        dmTres : float, optional
            Filter by the dmTres, by default None
        sn : float, optional
            Filter by the sn, by default None
        flux : float, optional
            Filter by the flux, by default None
        rm : float, optional
            Filter by the rm, by default None
        percentRfiZapped : float, optional
            Filter by the percentRfiZapped, by default None

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        filters = [
            {"field": "id", "value": id},
            {"field": "observation_Id", "value": observation_id},
            {"field": "ephemeris_Id", "value": ephemeris_id},
            {"field": "template_Id", "value": template_id,},
            {"field": "pipelineName", "value": pipelineName},
            {"field": "pipelineDescription", "value": pipelineDescription},
            {"field": "pipelineVersion", "value": pipelineVersion},
            {"field": "jobState", "value": jobState},
            {"field": "location", "value": location},
            {"field": "dm", "value": dm},
            {"field": "dmErr", "value": dmErr},
            {"field": "dmEpoch", "value": dmEpoch},
            {"field": "dmChi2r", "value": dmChi2r},
            {"field": "dmTres", "value": dmTres},
            {"field": "sn", "value": sn},
            {"field": "flux", "value": flux},
            {"field": "rm", "value": rm},
            {"field": "percentRfiZapped", "value": percentRfiZapped},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def create(
        self,
        observationId,
        ephemerisId,
        templateId,
        pipelineName,
        pipelineDescription,
        pipelineVersion,
        jobState,
        location,
        configuration,
        results_dict=None,
    ):
        """Create a new PipelineRun database object.

        Parameters
        ----------
        observationId : int
            The ID of the Observation database object of this PipelineRun.
        ephemerisId : int
            The ID of the Ephemeris database object of this PipelineRun.
        templateId : int
            The ID of the Template database object of this PipelineRun.
        pipelineName : str
            The name of the pipeline used for this PipelineRun.
        pipelineDescription : str
            The description of the pipeline used for this PipelineRun.
        pipelineVersion : str
            The version of the pipeline used for this PipelineRun.
        jobState : str
            The state of the job from ("Pending", "Running", "Completed", "Failed", "Cancelled").
        location : str
            The location of the job outputs.
        configuration : dict
            The input parameters of the pipeline used for this PipelineRun.
        results_dict : dict, optional
            The results of the pipeline which is only uploaded when the run is completed, by default None

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "createPipelineRun"
        self.mutation = """
        mutation (
            $observationId: Int!,
            $ephemerisId: Int!,
            $templateId: Int!,
            $pipelineName: String!,
            $pipelineDescription: String!,
            $pipelineVersion: String!,
            $jobState: String!,
            $location: String!,
            $configuration: String!,
            $dm: Float,
            $dm_err: Float,
            $dm_epoch: Float,
            $dm_chi2r: Float,
            $dm_tres: Float,
            $sn: Float,
            $flux: Float,
            $rm: Float,
            $rm_err: Float,
            $percent_rfi_zapped: Float,
        ) {
            createPipelineRun(input: {
                observationId: $observationId,
                ephemerisId: $ephemerisId,
                templateId: $templateId,
                pipelineName: $pipelineName,
                pipelineDescription: $pipelineDescription,
                pipelineVersion: $pipelineVersion,
                jobState: $jobState,
                location: $location,
                configuration: $configuration,
                dm: $dm,
                dmErr: $dm_err,
                dmEpoch: $dm_epoch,
                dmChi2r: $dm_chi2r,
                dmTres: $dm_tres,
                sn: $sn,
                flux: $flux,
                rm: $rm,
                rmErr: $rm_err,
                percentRfiZapped: $percent_rfi_zapped,
            }) {
                pipelineRun {
                    id
                }
            }
        }
        """
        if results_dict is None:
            results_dict = {
                "dm": None,
                "dm_err": None,
                "dm_epoch": None,
                "dm_chi2r": None,
                "dm_tres": None,
                "sn": None,
                "flux": None,
                "rm": None,
                "rm_err": None,
                "percent_rfi_zapped": None,
            }
        if results_dict["rm"] == "None":
            results_dict["rm"] = None
        if results_dict["rm_err"] == "None":
            results_dict["rm_err"] = None
        self.variables = {
            "observationId": observationId,
            "ephemerisId": ephemerisId,
            "templateId": templateId,
            "pipelineName": pipelineName,
            "pipelineDescription": pipelineDescription,
            "pipelineVersion": pipelineVersion,
            "jobState": jobState,
            "location": location,
            "configuration": json.dumps(configuration),
            "dm": results_dict["dm"],
            "dm_err": results_dict["dm_err"],
            "dm_epoch": results_dict["dm_epoch"],
            "dm_chi2r": results_dict["dm_chi2r"],
            "dm_tres": results_dict["dm_tres"],
            "sn": results_dict["sn"],
            "flux": results_dict["flux"],
            "rm": results_dict["rm"],
            "rm_err": results_dict["rm_err"],
            "percent_rfi_zapped": results_dict["percent_rfi_zapped"],
        }
        return self.mutation_graphql()

    def update(
        self,
        id,
        jobState,
        results_dict=None,
    ):
        """Update a PipelineRun database object.

        Parameters
        ----------
        id : int
            The database ID
        jobState : str
            The state of the job from ("Pending", "Running", "Completed", "Failed", "Cancelled").
        results_dict : dict, optional
            The results of the pipeline which is only uploaded when the run is completed, by default None


        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "updatePipelineRun"
        self.mutation = """
        mutation (
            $id: Int!,
            $jobState: String!,
            $dm: Float,
            $dm_err: Float,
            $dm_epoch: Float,
            $dm_chi2r: Float,
            $dm_tres: Float,
            $sn: Float,
            $flux: Float,
            $rm: Float,
            $rm_err: Float,
            $percent_rfi_zapped: Float,

        ) {
            updatePipelineRun(id: $id, input: {
                jobState: $jobState,
                dm: $dm,
                dmErr: $dm_err,
                dmEpoch: $dm_epoch,
                dmChi2r: $dm_chi2r,
                dmTres: $dm_tres,
                sn: $sn,
                flux: $flux,
                rm: $rm,
                rmErr: $rm_err,
                percentRfiZapped: $percent_rfi_zapped,
            }) {
                pipelineRun {
                    id
                }
            }
        }
        """
        if results_dict is None:
            results_dict = {
                "dm": None,
                "dm_err": None,
                "dm_epoch": None,
                "dm_chi2r": None,
                "dm_tres": None,
                "sn": None,
                "flux": None,
                "rm": None,
                "rm_err": None,
                "percent_rfi_zapped": None,
            }
        if results_dict["rm"] == "None":
            results_dict["rm"] = None
        if results_dict["rm_err"] == "None":
            results_dict["rm_err"] = None
        self.variables = {
            "id": id,
            "jobState": jobState,
            "dm": results_dict["dm"],
            "dm_err": results_dict["dm_err"],
            "dm_epoch": results_dict["dm_epoch"],
            "dm_chi2r": results_dict["dm_chi2r"],
            "dm_tres": results_dict["dm_tres"],
            "sn": results_dict["sn"],
            "flux": results_dict["flux"],
            "rm": results_dict["rm"],
            "rm_err": results_dict["rm_err"],
            "percent_rfi_zapped": results_dict["percent_rfi_zapped"],
        }
        return self.mutation_graphql()

    def delete(
        self,
        id,
    ):
        """Delete a PipelineRun database object.

        Parameters
        ----------
        id : int
            The database ID

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "deletePipelineRun"
        self.mutation = """
        mutation ($id: Int!) {
            deletePipelineRun(id: $id) {
                ok
            }
        }
        """
        self.variables = {
            "id": id,
        }
        return self.mutation_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(
                args.target,
                args.calibration,
                args.telescope,
                args.instrument_config,
                args.project,
                args.config,
                args.utc,
                args.duration,
                args.nant,
                args.nanteff,
                args.suspect,
                args.comment,
            )
        elif args.subcommand == "update":
            with open(args.results_json, "r") as json_file:
                results_dict = json.load(json_file)
            return self.update(
                args.id,
                args.job_state,
                results_dict,
            )
        elif args.subcommand == "list":
            return self.list(
                args.id,
                args.observation_id,
                args.ephemeris_id,
                args.template_id,
                args.pipelineName,
                args.pipelineDescription,
                args.pipelineVersion,
                args.jobState,
                args.location,
                args.dm,
                args.dmErr,
                args.dmEpoch,
                args.dmChi2r,
                args.dmTres,
                args.sn,
                args.flux,
                args.rm,
                args.percentRfiZapped,
            )
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "pipeline_run"

    @classmethod
    def get_description(cls):
        return "PipelineRun details."

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("PipelineRun model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing PipelineRun")

        parser_list.add_argument("--id", metavar="ID", type=int, help="List pipeline run with matching pipeline_run_id [int]")
        parser_list.add_argument("--observation_id", metavar="OBS", type=int, help="List pipeline run with matching observation_id [int]")
        parser_list.add_argument("--ephemeris_id", metavar="EPHEM", type=int, help="List pipeline run with matching ephemeris_id [int]")
        parser_list.add_argument("--template_id", metavar="TEMP", type=int, help="List pipeline run with matching template_id [int]")
        parser_list.add_argument("--pipelineName", metavar="NAME", type=str, help="List pipeline run with matching pipelineName [str]")
        parser_list.add_argument("--pipelineDescription", metavar="DESC", type=str, help="List pipeline run with matching pipelineDescription [str]")
        parser_list.add_argument("--pipelineVersion", metavar="VER", type=str, help="List pipeline run with matching pipelineVersion [str]")
        parser_list.add_argument("--jobState", metavar="STATE", type=str, help="List pipeline run with matching jobState [str]")
        parser_list.add_argument("--location", metavar="LOC", type=str, help="List pipeline run with matching location [str]")
        parser_list.add_argument("--dm", metavar="DM", type=float, help="List pipeline run with matching dm [float]")
        parser_list.add_argument("--dmErr", metavar="DMERR", type=float, help="List pipeline run with matching dmErr [float]")
        parser_list.add_argument("--dmEpoch", metavar="DMEPOCH", type=float, help="List pipeline run with matching dmEpoch [float]")
        parser_list.add_argument("--dmChi2r", metavar="DMCHI2R", type=float, help="List pipeline run with matching dmChi2r [float]")
        parser_list.add_argument("--dmTres", metavar="DMTRES", type=float, help="List pipeline run with matching dmTres [float]")
        parser_list.add_argument("--sn", metavar="SN", type=float, help="List pipeline run with matching sn [float]")
        parser_list.add_argument("--flux", metavar="FLUX", type=float, help="List pipeline run with matching flux [float]")
        parser_list.add_argument("--rm", metavar="RM", type=float, help="List pipeline run with matching rm [float]")
        parser_list.add_argument("--percentRfiZapped", metavar="RFI", type=float, help="List pipeline run with matching percentRfiZapped [float]")

