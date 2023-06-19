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
            $ephemerisLoc: Str!,
            $templateLoc: Str!,
            $pipelineName: Str!,
            $pipelineDescription: Str!,
            $pipelineVersion: Str!,
            $jobState: Str!,
            $location: Str!,
            $configuration: JSONString!,
            $dm: Float,
            $dmErr: Float,
            $dmEpoch: Float,
            $dmChi2r: Float,
            $dmTres: Float,
            $sn: Float,
            $flux: Float,
            $rm: Float,
            $percent_rfi_zapped: Float,
        ) {
            createPipelineRun(input: {
                observationId: $observationId,
                ephemerisLoc: $ephemerisLoc,
                templateLoc: $templateLoc,
                pipelineName: $pipelineName,
                pipelineDescription: $pipelineDescription,
                pipelineVersion: $pipelineVersion,
                jobState: $jobState,
                location: $location,
                configuration: $configuration,
                dm: $dm,
                dmErr: $dmErr,
                dmEpoch: $dmEpoch,
                dmChi2r: $dmChi2r,
                dmTres: $dmTres,
                sn: $sn,
                flux: $flux,
                rm: $rm,
                percent_rfi_zapped: $percent_rfi_zapped,
            }) {
                pipelineRun {
                    id
                }
            }
        }
        """

        self.update_mutation = """
        mutation ($id: Int!, $target: Int!, $calibration: Int!, $telescope: Int!, $instrument_config: Int!, $project: Int!, $config: JSONString!, $duration: Float!, $utc_start: DateTime!, $nant: Int!,   $nant_eff: Int!, $suspect: Boolean!, $comment: String) {
            updateObservation(id: $id, input: {
                target_id: $target,
                calibration_id: $calibration,
                telescope_id: $telescope,
                instrument_config_id: $instrument_config,
                project_id: $project,
                config: $config,
                utcStart: $utc_start,
                duration: $duration,
                nant: $nant,
                nantEff: $nant_eff,
                suspect: $suspect,
                comment: $comment
            }) {
                observation {
                    id,
                    target { id },
                    calibration { id },
                    telescope { id },
                    instrumentConfig { id },
                    project { id },
                    config,
                    utcStart,
                    duration,
                    nant,
                    nantEff,
                    suspect,
                    comment
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteObservation(id: $id) {
                ok
            }
        }
        """

        self.field_names = [
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
            "percent_rfi_zapped",
        ]
        self.literal_field_names = [
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
            "percent_rfi_zapped",
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
        ephemerisLoc,
        templateLoc,
        pipelineName,
        pipelineDescription,
        pipelineVersion,
        jobState,
        location,
        configuration,
        results_dict=None,
    ):
        if results_dict is None:
            dm = None
            dmErr = None
            dmEpoch = None
            dmChi2r = None
            dmTres = None
            sn = None
            flux = None
            rm = None
            percent_rfi_zapped = None
        else:
            # Unpack dictionary
            dm = results_dict["dm"]
            dmErr = results_dict["dmErr"]
            dmEpoch = results_dict["dmEpoch"]
            dmChi2r = results_dict["dmChi2r"]
            dmTres = results_dict["dmTres"]
            sn = results_dict["sn"]
            flux = results_dict["flux"]
            rm = results_dict["rm"]
            percent_rfi_zapped = results_dict["percent_rfi_zapped"]

        self.create_variables = {
            "observationId": observationId,
            "ephemerisLoc": ephemerisLoc,
            "templateLoc": templateLoc,
            "pipelineName": pipelineName,
            "pipelineDescription": pipelineDescription,
            "pipelineVersion": pipelineVersion,
            "jobState": jobState,
            "location": location,
            "configuration": json.dumps(configuration),
            "dm": dm,
            "dmErr": dmErr,
            "dmEpoch": dmEpoch,
            "dmChi2r": dmChi2r,
            "dmTres": dmTres,
            "sn": sn,
            "flux": flux,
            "rm": rm,
            "percent_rfi_zapped": percent_rfi_zapped,
        }
        return self.create_graphql()

    def update(
        self,
        id,
        target,
        calibration,
        telescope,
        instrument_config,
        project,
        config,
        utc,
        duration,
        nant,
        nanteff,
        suspect,
        comment,
    ):
        self.update_variables = {
            "id": id,
            "target": target,
            "calibration": calibration,
            "telescope": telescope,
            "instrument_config": instrument_config,
            "project": project,
            "config": config,
            "utc_start": utc,
            "duration": duration,
            "nant": nant,
            "nant_eff": nanteff,
            "suspect": suspect,
            "comment": comment,
        }
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
            return self.update(
                args.id,
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
        return "observations"

    @classmethod
    def get_description(cls):
        return "Observation details."

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Observations model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing observations")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list observations matching the id [int]")
        parser_list.add_argument(
            "--target_id", metavar="TGTID", type=int, help="list observations matching the target (pulsar) id [int]"
        )
        parser_list.add_argument(
            "--pulsar", metavar="TGTNAME", type=str, help="list observations matching the target (pulsar) name [str]"
        )
        parser_list.add_argument(
            "--telescope_id", metavar="TELID", type=int, help="list observations matching the telescope id [int]"
        )
        parser_list.add_argument(
            "--telescope_name", metavar="TELNAME", type=str, help="list observations matching the telescope name [int]"
        )
        parser_list.add_argument(
            "--instrumentconfig_id",
            metavar="ICID",
            type=int,
            help="list observations matching the instrument_config id [int]",
        )
        parser_list.add_argument(
            "--instrumentconfig_name",
            metavar="ICNAME",
            type=str,
            help="list observations matching the instrument_config name [str]",
        )
        parser_list.add_argument(
            "--project_id", metavar="PROJID", type=int, help="list observations matching the project id [id]"
        )
        parser_list.add_argument(
            "--project_code", metavar="PROJCODE", type=str, help="list observations matching the project code [str]"
        )
        parser_list.add_argument(
            "--utcs",
            metavar="UTCGTE",
            type=str,
            help="list observations with utc_start greater than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )
        parser_list.add_argument(
            "--utce",
            metavar="UTCLET",
            type=str,
            help="list observations with utc_start less than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new observation")
        parser_create.add_argument("target", metavar="TGT", type=int, help="target id of the observation [int]")
        parser_create.add_argument(
            "calibration", metavar="CAL", type=int, help="calibration id of the observation [int]"
        )
        parser_create.add_argument("telescope", metavar="TEL", type=int, help="telescope id of the observation [int]")
        parser_create.add_argument(
            "instrument_config", metavar="IC", type=int, help="instrument config id of the observation [int]"
        )
        parser_create.add_argument("project", metavar="PROJ", type=int, help="project id of the observation [int]")
        parser_create.add_argument("config", metavar="CFG", type=str, help="json config of the observation [json]")
        parser_create.add_argument(
            "utc", metavar="UTC", type=str, help="start utc of the observation [YYYY-MM-DDTHH:MM:SS+00:00]"
        )
        parser_create.add_argument(
            "duration", metavar="DUR", type=float, help="duration of the observation in seconds [float]"
        )
        parser_create.add_argument(
            "nant", metavar="NANT", type=int, help="number of antennas used during the observation [int]"
        )
        parser_create.add_argument(
            "nanteff",
            metavar="NANTEFF",
            type=int,
            help="effective number of antennas used during the observation [int]",
        )
        parser_create.add_argument("suspect", metavar="SUS", type=bool, help="status of the observation [bool]")
        parser_create.add_argument("comment", metavar="COM", type=str, help="any comment on the observation [str]")

        parser_update = subs.add_parser("update", help="create a new observation")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the existing observation [int]")
        parser_update.add_argument("target", metavar="TGT", type=int, help="target id of the observation [int]")
        parser_update.add_argument(
            "calibration", metavar="CAL", type=int, help="calibration id of the observation [int]"
        )
        parser_update.add_argument("telescope", metavar="TEL", type=int, help="telescope id of the observation [int]")
        parser_update.add_argument(
            "instrument_config", metavar="IC", type=int, help="instrument config id of the observation [int]"
        )
        parser_update.add_argument("project", metavar="PROJ", type=int, help="project id of the observation [int]")
        parser_update.add_argument("config", metavar="CFG", type=str, help="json config of the observation [json]")
        parser_update.add_argument(
            "utc", metavar="UTC", type=str, help="start utc of the observation [YYYY-MM-DDTHH:MM:SS+00:00]"
        )
        parser_update.add_argument(
            "duration", metavar="DUR", type=float, help="duration of the observation in seconds [float]"
        )
        parser_update.add_argument(
            "nant", metavar="NANT", type=int, help="number of antennas used during the observation [int]"
        )
        parser_update.add_argument(
            "nanteff",
            metavar="NANTEFF",
            type=int,
            help="effective number of antennas used during the observation [int]",
        )
        parser_update.add_argument("suspect", metavar="SUS", type=bool, help="status of the observation [bool]")
        parser_update.add_argument("comment", metavar="COM", type=str, help="any comment on the observation [str]")

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing observation")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the existing observation [int]")


if __name__ == "__main__":
    parser = Observation.get_parsers()
    args = parser.parse_args()

    from psrdb.graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    o = Observation(client, args.url, args.token)
    o.process(args)
