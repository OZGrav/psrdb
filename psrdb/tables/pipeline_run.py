import json
from datetime import datetime

from psrdb.graphql_table import GraphQLTable
from psrdb.graphql_query import graphql_query_factory


class PipelineRun(GraphQLTable):
    def __init__(self, client, token):
        GraphQLTable.__init__(self, client, token)

        # create a new record
        self.create_mutation = """
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
                percentRfiZapped: $percent_rfi_zapped,
            }) {
                pipelineRun {
                    id
                }
            }
        }
        """

        self.update_mutation = """
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
                percentRfiZapped: $percent_rfi_zapped,
            }) {
                pipelineRun {
                    id
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deletePipelineRun(id: $id) {
                ok
            }
        }
        """

        self.field_names = [
            "id",
            "observation { id }",
            "template { id }",
            "pipelineName",
            "pipelineDescription",
            "pipelineVersion",
            "jobState",
            "location",
            "configuration",
            "dm",
            "dmErr",
            "dmEpoch",
            "dmChi2r",
            "dmTres",
            "sn",
            "flux",
            "rm",
            "percentRfiZapped",
        ]
        self.literal_field_names = [
            "id",
            "observation { id }",
            "ephemeris { id }",
            "template { id }",
            "pipelineName",
            "pipelineDescription",
            "pipelineVersion",
            "jobState",
            "location",
            "configuration",
            "dm",
            "dmErr",
            "dmEpoch",
            "dmChi2r",
            "dmTres",
            "sn",
            "flux",
            "rm",
            "percentRfiZapped",
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
        filters = [
            {"field": "observation_Id", "value": observation_id, "join": "Observation"},
            {"field": "ephemeris_Id", "value": ephemeris_id, "join": "Ephemeris"},
            {"field": "template_Id", "value": template_id, "join": "Template"},
            {"field": "pipelineName", "value": pipelineName, "join": None},
            {"field": "pipelineDescription", "value": pipelineDescription, "join": None},
            {"field": "pipelineVersion", "value": pipelineVersion, "join": None},
            {"field": "jobState", "value": jobState, "join": None},
            {"field": "location", "value": location, "join": None},
            {"field": "dm", "value": dm, "join": None},
            {"field": "dmErr", "value": dmErr, "join": None},
            {"field": "dmEpoch", "value": dmEpoch, "join": None},
            {"field": "dmChi2r", "value": dmChi2r, "join": None},
            {"field": "dmTres", "value": dmTres, "join": None},
            {"field": "sn", "value": sn, "join": None},
            {"field": "flux", "value": flux, "join": None},
            {"field": "rm", "value": rm, "join": None},
            {"field": "percentRfiZapped", "value": percentRfiZapped, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

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
                "percent_rfi_zapped": None,
            }
        self.create_variables = {
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
            "percent_rfi_zapped": results_dict["percent_rfi_zapped"],
        }
        return self.create_graphql()

    def update(
        self,
        id,
        jobState,
        results_dict=None,
    ):
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
                "percent_rfi_zapped": None,
            }
        self.update_variables = {
            "id": id,
            "jobState": jobState,
            "dm": results_dict["dm"],
            "dm_err": results_dict["dm_err"],
            "dm_epoch": results_dict["dm_epoch"],
            "dm_chi2r": results_dict["dm_chi2r"],
            "dm_tres": results_dict["dm_tres"],
            "sn": results_dict["sn"],
            "flux": results_dict["flux"],
            "percent_rfi_zapped": results_dict["percent_rfi_zapped"],
        }
        if "rm" in results_dict.keys():
            self.update_variables["rm"] = results_dict["rm"]
        return self.update_graphql()

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
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "pipelinerun"

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

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new PipelineRun")
        parser_create.add_argument("target", metavar="TGT", type=int, help="target id of the PipelineRun [int]")
        parser_create.add_argument(
            "calibration", metavar="CAL", type=int, help="calibration id of the PipelineRun [int]"
        )
        parser_create.add_argument("telescope", metavar="TEL", type=int, help="telescope id of the PipelineRun [int]")
        parser_create.add_argument(
            "instrument_config", metavar="IC", type=int, help="instrument config id of the PipelineRun [int]"
        )
        parser_create.add_argument("project", metavar="PROJ", type=int, help="project id of the PipelineRun [int]")
        parser_create.add_argument("config", metavar="CFG", type=str, help="json config of the PipelineRun [json]")
        parser_create.add_argument(
            "utc", metavar="UTC", type=str, help="start utc of the PipelineRun [YYYY-MM-DDTHH:MM:SS+00:00]"
        )
        parser_create.add_argument(
            "duration", metavar="DUR", type=float, help="duration of the PipelineRun in seconds [float]"
        )
        parser_create.add_argument(
            "nant", metavar="NANT", type=int, help="number of antennas used during the PipelineRun [int]"
        )
        parser_create.add_argument(
            "nanteff",
            metavar="NANTEFF",
            type=int,
            help="effective number of antennas used during the PipelineRun [int]",
        )
        parser_create.add_argument("suspect", metavar="SUS", type=bool, help="status of the PipelineRun [bool]")
        parser_create.add_argument("comment", metavar="COM", type=str, help="any comment on the PipelineRun [str]")

        parser_update = subs.add_parser("update", help="create a new PipelineRun")
        parser_update.add_argument("id", metavar="ID", type=int, help="ID of the existing PipelineRun [int]")
        parser_update.add_argument("job_state", metavar="STATE", type=str, help="State of the job from ('started', 'finished', 'error') [str]")
        parser_update.add_argument("results_json", metavar="JSON", type=str, help="Path to the results.json file [str]", default="results.json")

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing PipelineRun")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the existing PipelineRun [int]")


if __name__ == "__main__":
    parser = PipelineRun.get_parsers()
    args = parser.parse_args()

    from psrdb.graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    o = PipelineRun(client, args.url, args.token)
    o.process(args)
