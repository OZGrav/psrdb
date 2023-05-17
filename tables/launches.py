from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Launches(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)

        # create a new record
        self.create_mutation = """
        mutation ($pipeline_id: Int!, $parent_pipeline_id: Int!, $pulsar_id: Int!) {
            createLaunch(input: {
                pipeline_id: $pipeline_id,
                parent_pipeline_id: $parent_pipeline_id,
                pulsar_id: $pulsar_id
                }) {
                launch {
                    id
                }
            }
        }
        """
        # update an existing record
        self.update_mutation = """
        mutation ($id: Int!, $pipeline_id: Int!, $parent_pipeline_id: Int!, $pulsar_id: Int!) {
            updateLaunch(id: $id, input: {
                pipeline_id: $pipeline_id,
                parent_pipeline_id: $parent_pipeline_id,
                pulsar_id: $pulsar_id
                }) {
                launch {
                    id,
                    pipeline {id},
                    parentPipeline {id},
                    pulsar {id}
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteLaunch(id: $id) {
                ok
            }
        }
        """

        self.literal_field_names = ["id", "pipeline {id}", "parentPipeline {id}", "pulsar {id}"]
        self.field_names = ["id", "pipeline {name}", "parentPipeline {name}", "pulsar {jname}"]

    def list(self, id=None, pipeline=None, parentPipeline=None, pulsar=None):
        """Return a list of records matching the id and/or the pipeline id, parent pipeline id, pulsar id."""
        filters = [
            {"field": "pipeline", "value": pipeline, "join": "Pipelines"},
            {"field": "parentPipeline", "value": parentPipeline, "join": "Pipelines"},
            {"field": "pulsar", "value": pulsar, "join": "Pulsars"},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, pipeline_id, parent_pipeline_id, pulsar_id):
        self.create_variables = {
            "pipeline_id": pipeline_id,
            "parent_pipeline_id": parent_pipeline_id,
            "pulsar_id": pulsar_id,
        }
        return self.create_graphql()

    def update(self, id, pipeline_id, parent_pipeline_id, pulsar_id):
        self.update_variables = {
            "id": id,
            "pipeline_id": pipeline_id,
            "parent_pipeline_id": parent_pipeline_id,
            "pulsar_id": pulsar_id,
        }
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.pipeline_id, args.parent_pipeline_id, args.pulsar_id)
        elif args.subcommand == "update":
            return self.update(args.id, args.pipeline_id, args.parent_pipeline_id, args.pulsar_id)
        elif args.subcommand == "list":
            return self.list(args.id, args.pipeline_id, args.parent_pipeline_id, args.pulsar_id)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "launches"

    @classmethod
    def get_description(cls):
        return "A relation between a pulsar and which pipelines are run on those pulsars"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Launches model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing launches")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list launches matching the id [int]")
        parser_list.add_argument(
            "--pipeline_id", metavar="PL", type=int, help="list launches matching the pipeline id [int]"
        )
        parser_list.add_argument(
            "--parent_pipeline_id", metavar="PPL", type=int, help="list launches matching the parent pipeline id [int]"
        )
        parser_list.add_argument(
            "--pulsar_id", metavar="PSR", type=int, help="list launches matching the pulsar id [int]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new launches")
        parser_create.add_argument("pipeline_id", metavar="PL", type=int, help="id of the pipeline [int]")
        parser_create.add_argument(
            "parent_pipeline_id", metavar="PPL", type=int, help="id of the parent pipeline [int]"
        )
        parser_create.add_argument("pulsar_id", metavar="PSR", type=int, help="id of the pulsar [int]")

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update an existing launches")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the launch [int]")
        parser_update.add_argument("pipeline_id", metavar="PL", type=int, help="id of the pipeline [int]")
        parser_update.add_argument(
            "parent_pipeline_id", metavar="PPL", type=int, help="id of the parent pipeline [int]"
        )
        parser_update.add_argument("pulsar_id", metavar="PSR", type=int, help="id of the pulsar [int]")

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing launch")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the launch [int]")


if __name__ == "__main__":
    parser = Launches.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    l = Launches(client, args.url, args.token)
    l.process(args)
