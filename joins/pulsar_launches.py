from tables.graphql_join import GraphQLJoin


class PulsarsLaunches(GraphQLJoin):
    def __init__(self, client, url, token):
        GraphQLJoin.__init__(self, client, url, token)

        self.table_name = self.__class__.__name__
        self.join_prefix = "launches_"

        self.field_names = ["id", "jname", "state", "comment"]

    def list_graphql(self, args):

        filters = [
            {"arg": "target_id", "field": "observation_Target_Id", "join": "Targets"},
            {"arg": "target_name", "field": "observation_Target_Name", "join": "Targets"},
            {"arg": "telescope_id", "field": "observation_Telescope_Id", "join": "Telescopes"},
            {"arg": "telescope_name", "field": "observation_Telescope_Name", "join": "Telescopes"},
            {"arg": "project_id", "field": "observation_Project_Id", "join": "Projects"},
            {"arg": "project_code", "field": "observation_Project_Code", "join": "Projects"},
            {"arg": "instrument_config_id", "field": "observation_InstrumentConfig_Id", "join": "InstrumentConfigs"},
            {
                "arg": "instrument_config_name",
                "field": "observation_InstrumentConfig_Name",
                "join": "InstrumentConfigs",
            },
            {"arg": "utc_start_gte", "field": "observation_UtcStart_Gte", "join": None},
            {"arg": "utc_start_lte", "field": "observation_UtcStart_Lte", "join": None},
        ]
        fields = []
        for f in filters:
            if hasattr(args, f["arg"]) and not getattr(args, f["arg"]) is None:
                f["value"] = getattr(args, f["arg"])
                fields.append(f)

        if len(fields) > 0:
            self.list_query = self.build_filter_query(fields)
            self.list_variables = "{}"
            return GraphQLJoin.list_graphql(self, ())
        else:
            self.list_query = self.build_list_all_query()
            self.list_variables = "{}"
            return GraphQLJoin.list_graphql(self, ())

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        if args.subcommand == "list":
            return self.list_graphql(args)

    @classmethod
    def get_name(cls):
        return "pulsarLaunches"

    @classmethod
    def get_description(cls):
        return "A pulsar source defined by a J2000 name"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLJoin.get_default_parser("Pulsars model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        # create the parser for the "list" command
        parser_list = subs.add_parser("list", help="list existing Pulsars")
        parser_list.add_argument("--id", type=int, help="list Pulsars matching the id")
        parser_list.add_argument("--jname", type=str, help="list Pulsars matching the jname")


if __name__ == "__main__":

    parser = PulsarLaunches.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    p = PulsarLaunches(client, args.url, args.token)
    response = p.process(args)
