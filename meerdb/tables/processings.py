from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Processings(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($observation_id: Int!, $pipeline_id: Int!, $parent_id: Int!, $embargo_end: DateTime!, $location: String!, $job_state: JSONString!, $job_output: JSONString!, $results: JSONString!) {
            createProcessing(input: {
                observation_id: $observation_id,
                pipeline_id: $pipeline_id,
                parent_id: $parent_id,
                embargo_end: $embargo_end,
                location: $location,
                job_state: $job_state,
                job_output: $job_output,
                results: $results
            }) {
                processing {
                    id
                }
            }
        }
        """
        # Update an existing record
        self.update_mutation = """
        mutation ($id: Int!, $observation_id: Int!, $pipeline_id: Int!, $parent_id: Int!, $embargo_end: DateTime!, $location: String!, $job_state: JSONString!, $job_output: JSONString!, $results: JSONString!) {
            updateProcessing(id: $id, input: {
                observation_id: $observation_id,
                pipeline_id: $pipeline_id,
                parent_id: $parent_id,
                embargo_end: $embargo_end,
                location: $location,
                job_state: $job_state,
                job_output: $job_output,
                results: $results
            }) {
                processing {
                    id,
                    observation { id },
                    pipeline { id },
                    parent { id },
                    location,
                    embargoEnd,
                    jobState,
                    jobOutput,
                    results
                }
            }
        }       """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteProcessing(id: $id) {
                ok
            }
        }
        """

        self.field_names = [
            "id",
            "observation { id }",
            "pipeline { name }",
            "parent { id }",
            "location",
            "embargoEnd",
            "jobState",
            "jobOutput",
            "results",
        ]
        self.literal_field_names = [
            "id",
            "observation { id }",
            "pipeline { id }",
            "parent { id } ",
            "location",
            "embargoEnd",
            "jobState",
            "jobOutput",
            "results",
        ]

    def list(self, id=None, observation_id=None, parent_id=None, location=None, utc_start=None):
        """Return a list of records matching the id and/or the provided arguments."""
        filters = [
            {"field": "observationId", "value": observation_id, "join": "Observations"},
            {"field": "parentId", "value": parent_id, "join": "Processings"},
            {"field": "location", "value": location, "join": None},
            {"field": "observation_UtcStart_Gte", "value": utc_start, "join": "Observations"},
            {"field": "observation_UtcStart_Lte", "value": utc_start, "join": "Observations"},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, observation, pipeline, parent, embargo_end, location, job_state, job_output, results):
        self.create_variables = {
            "observation_id": observation,
            "pipeline_id": pipeline,
            "parent_id": parent,
            "embargo_end": embargo_end,
            "location": location,
            "job_state": job_state,
            "job_output": job_output,
            "results": results,
        }
        return self.create_graphql()

    def update(self, id, observation, pipeline, parent, embargo_end, location, job_state, job_output, results):
        self.update_variables = {
            "id": id,
            "observation_id": observation,
            "pipeline_id": pipeline,
            "parent_id": parent,
            "embargo_end": embargo_end,
            "location": location,
            "job_state": job_state,
            "job_output": job_output,
            "results": results,
        }
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(
                args.observation,
                args.pipeline,
                args.parent,
                args.embargo_end,
                args.location,
                args.job_state,
                args.job_output,
                args.results,
            )
        elif args.subcommand == "update":
            return self.update(
                args.id,
                args.observation,
                args.pipeline,
                args.parent,
                args.embargo_end,
                args.location,
                args.job_state,
                args.job_output,
                args.results,
            )
        elif args.subcommand == "list":
            return self.list(args.id, args.observation, args.parent, args.location, args.utc_start)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "processings"

    @classmethod
    def get_description(cls):
        return "Processing."

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Processings model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing processings")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list processing matching the id [int]")
        parser_list.add_argument(
            "--observation", metavar="OBS", type=int, help="list processing matching the observation id [int]"
        )
        parser_list.add_argument(
            "--parent", metavar="PAR", type=int, help="list processing matching the parent processing id [int]"
        )
        parser_list.add_argument(
            "--utc_start",
            metavar="UTC",
            type=str,
            help="list processing matching the observation utc_start [YYYY-MM-DDTHH:MM:SS+00:00]",
        )
        parser_list.add_argument(
            "--location", metavar="LOC", type=str, help="list processing matching the processing location [str]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new processing")
        parser_create.add_argument(
            "observation", metavar="OBS", type=int, help="observation id for the processing [int]"
        )
        parser_create.add_argument("pipeline", metavar="PL", type=int, help="pipeline id for the processing [int]")
        parser_create.add_argument("parent", metavar="PAR", type=int, help="parent id for the processing int]")
        parser_create.add_argument(
            "location", metavar="LOC", type=str, help="location (on disk) of the processing [str]"
        )
        parser_create.add_argument(
            "embargo_end", metavar="EMB", type=str, help="end of embargo of the processing [YYYY-MM-DDTHH:MM:SS+00:00]"
        )
        parser_create.add_argument(
            "job_state", metavar="JOBS", type=str, help="JSON with the state of the processing [json]"
        )
        parser_create.add_argument(
            "job_output", metavar="JOBO", type=str, help="JSON with output of the processing [json]"
        )
        parser_create.add_argument(
            "results", metavar="RES", type=str, help="JSON with results of the processing [json]"
        )

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update the values of an existing processing")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the processing [int]")
        parser_update.add_argument(
            "observation", metavar="OBS", type=int, help="observation id for the processing [int]"
        )
        parser_update.add_argument("pipeline", metavar="PL", type=int, help="pipeline id for the processing [int]")
        parser_update.add_argument(
            "--parent", metavar="PAR", default=None, type=int, help="parent id for the processing [int]"
        )
        parser_update.add_argument(
            "location", metavar="LOC", type=str, help="location (on disk) of the processing [str]"
        )
        parser_update.add_argument(
            "embargo_end", metavar="EMB", type=str, help="end of embargo of the processing [YYYY-MM-DDTHH:MM:SS+00:00]"
        )
        parser_update.add_argument(
            "job_state", metavar="JOBS", type=str, help="JSON with the state of the processing [json]"
        )
        parser_update.add_argument(
            "job_output", metavar="JOBO", type=str, help="JSON with output of the processing [json]"
        )
        parser_update.add_argument(
            "results", metavar="RES", type=str, help="JSON with results of the processing [json]"
        )

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing processing")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the processing [int]")


if __name__ == "__main__":
    parser = Processings.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)
    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    p = Processings(client, args.url, args.token)
    p.process(args)
