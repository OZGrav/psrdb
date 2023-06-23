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
            "PipelineRun { id }",
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
        self.literal_field_names = [
            "PipelineRun { id }",
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
        target_id=None,
        pulsar=None,
        telescope_id=None,
        telescope_name=None,
        project_id=None,
        project_code=None,
        instrumentconfig_id=None,
        instrumentconfig_name=None,
        utcs=None,
        utce=None,
    ):
        # Convert dates to correct format
        if utcs == "":
            utcs = None
        elif utcs is not None:
            d = datetime.strptime(utcs, '%Y-%m-%d-%H:%M:%S')
            utcs = f"{d.date()}T{d.time()}+00:00"
        if utce == "":
            utce = None
        elif utce is not None:
            d = datetime.strptime(utce, '%Y-%m-%d-%H:%M:%S')
            utce = f"{d.date()}T{d.time()}+00:00"
        """Return a list of records matching the id and/or any of the arguments."""
        filters = [
            {"field": "target_Id", "value": target_id, "join": "Targets"},
            {"field": "target_Name", "value": pulsar, "join": "Targets"},
            {"field": "telescope_Id", "value": telescope_id, "join": "Telescopes"},
            {"field": "telescope_Name", "value": telescope_name, "join": "Telescopes"},
            {"field": "project_Id", "value": project_id, "join": "Projects"},
            {"field": "project_Code", "value": project_code, "join": "Projects"},
            {"field": "instrumentConfig_Id", "value": instrumentconfig_id, "join": "InstrumentConfigs"},
            {"field": "instrumentConfig_Name", "value": instrumentconfig_name, "join": "InstrumentConfigs"},
            {"field": "utcStart_Gte", "value": utcs, "join": None},
            {"field": "utcStart_Lte", "value": utce, "join": None},
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
                args.target_id,
                args.pulsar,
                args.telescope_id,
                args.telescope_name,
                args.project_id,
                args.project_code,
                args.instrumentconfig_id,
                args.instrumentconfig_name,
                args.utcs,
                args.utce,
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
        parser_list.add_argument("--id", metavar="ID", type=int, help="list PipelineRun matching the id [int]")
        parser_list.add_argument(
            "--target_id", metavar="TGTID", type=int, help="list PipelineRun matching the target (pulsar) id [int]"
        )
        parser_list.add_argument(
            "--pulsar", metavar="TGTNAME", type=str, help="list PipelineRun matching the target (pulsar) name [str]"
        )
        parser_list.add_argument(
            "--telescope_id", metavar="TELID", type=int, help="list PipelineRun matching the telescope id [int]"
        )
        parser_list.add_argument(
            "--telescope_name", metavar="TELNAME", type=str, help="list PipelineRun matching the telescope name [int]"
        )
        parser_list.add_argument(
            "--instrumentconfig_id",
            metavar="ICID",
            type=int,
            help="list PipelineRun matching the instrument_config id [int]",
        )
        parser_list.add_argument(
            "--instrumentconfig_name",
            metavar="ICNAME",
            type=str,
            help="list PipelineRun matching the instrument_config name [str]",
        )
        parser_list.add_argument(
            "--project_id", metavar="PROJID", type=int, help="list PipelineRun matching the project id [id]"
        )
        parser_list.add_argument(
            "--project_code", metavar="PROJCODE", type=str, help="list PipelineRun matching the project code [str]"
        )
        parser_list.add_argument(
            "--utcs",
            metavar="UTCGTE",
            type=str,
            help="list PipelineRun with utc_start greater than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )
        parser_list.add_argument(
            "--utce",
            metavar="UTCLET",
            type=str,
            help="list PipelineRun with utc_start less than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )

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