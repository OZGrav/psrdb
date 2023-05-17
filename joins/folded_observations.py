from joins.graphql_join import GraphQLJoin
from tables.graphql_query import graphql_query_factory


class FoldedObservations(GraphQLJoin):
    def __init__(self, client, url, token):
        GraphQLJoin.__init__(self, client, url, token)

        self.table_name = "foldings"
        self.join_prefix = "processing_Observation_"

        self.literal_field_names = [
            "id",
            "foldingEphemeris { id }",
            "nbin",
            "npol",
            "nchan",
            "dm",
            "tsubint,",
            """
            processing {
                id
                observation {
                    target { id }
                    telescope { id }
                    calibration { id }
                    project { id }
                    instrumentConfig { id }
                    utcStart
                }
                location
            }
            """,
        ]
        self.field_names = [
            "id",
            "foldingEphemeris { id }",
            "nbin",
            "npol",
            "nchan",
            "dm",
            "tsubint",
            """
            foldingEphemeris {
                pulsar { jname }
            }
            """,
            """
            processing {
                id
                observation {
                    target {
                        name
                    },
                    telescope { name }
                    calibration { id }
                    project { code }
                    instrumentConfig { id }
                    utcStart
                }
                location
            }
            """,
        ]

    def list(
        self,
        pulsar_id=None,
        pulsar_jname=None,
        telescope_id=None,
        telescope_name=None,
        project_id=None,
        project_code=None,
        instrument_config_id=None,
        instrument_config_name=None,
        utc_start_gte=None,
        utc_start_lte=None,
    ):
        filters = [
            {"field": "foldingEphemeris_Pulsar_Id", "value": pulsar_id, "join": "Pulsars"},
            {"field": "foldingEphemeris_Pulsar_Jname", "value": pulsar_jname, "join": "Pulsars"},
            {"field": "processing_Observation_Telescope_Id", "value": telescope_id, "join": "Telescopes"},
            {"field": "processing_Observation_Telescope_Name", "value": telescope_name, "join": "Telescopes"},
            {"field": "processing_Observation_Project_Id", "value": project_id, "join": "Projects"},
            {"field": "processing_Observation_Project_Code", "value": project_code, "join": "Projects"},
            {
                "field": "processing_Observation_InstrumentConfig_Id",
                "value": instrument_config_id,
                "join": "InstrumentConfigs",
            },
            {
                "field": "processing_Observation_InstrumentConfig_Name",
                "value": instrument_config_name,
                "join": "InstrumentConfigs",
            },
            {"field": "processing_Observation_UtcStart_Gte", "value": utc_start_gte, "join": None},
            {"field": "processing_Observation_UtcStart_Lte", "value": utc_start_lte, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, None, filters)
        return GraphQLJoin.list_graphql(self, graphql_query)

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "list":
            return self.list(
                args.pulsar_id,
                args.pulsar_jname,
                args.telescope_id,
                args.telescope_name,
                args.project_id,
                args.project_code,
                args.instrument_config_id,
                args.instrument_config_name,
                args.utc_start_gte,
                args.utc_start_lte,
            )

    @classmethod
    def get_name(cls):
        return "foldedobservations"

    @classmethod
    def get_description(cls):
        return "Folded Observations"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLJoin.get_default_parser("Folded observations")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing folded observations")
        parser_list.add_argument("--pulsar_id", type=int, help="list folded observations matching the pulsar id")
        parser_list.add_argument("--pulsar_jname", type=str, help="list folded observations matching the pulsar jname")
        parser_list.add_argument("--telescope_id", type=int, help="list folded observations matching the telescope id")
        parser_list.add_argument(
            "--telescope_name", type=str, help="list folded observations matching the telescope name"
        )
        parser_list.add_argument(
            "--instrument_config_id", type=int, help="list folded observations matching the instrument_config id"
        )
        parser_list.add_argument(
            "--instrument_config_name", type=str, help="list folded observations matching the instrument_config name"
        )
        parser_list.add_argument("--project_id", type=int, help="list folded observations matching the project id")
        parser_list.add_argument("--project_code", type=str, help="list folded observations matching the project code")
        parser_list.add_argument(
            "--utc_start_gte", type=str, help="list folded observations with utc_start greater than the timestamp"
        )
        parser_list.add_argument(
            "--utc_start_lte", type=str, help="list folded observations with utc_start less than the timestamp"
        )


if __name__ == "__main__":
    parser = FoldedObservations.get_parsers()
    args = parser.parse_args()

    GraphQLJoin.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    t = FoldedObservations(client, args.url, args.token)
    t.process(args)
