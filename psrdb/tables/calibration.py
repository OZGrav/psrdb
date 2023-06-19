from psrdb.graphql_table import GraphQLTable
from psrdb.graphql_query import graphql_query_factory


class Calibration(GraphQLTable):
    def __init__(self, client, token):
        GraphQLTable.__init__(self, client, token)

        # Create a new record
        self.create_mutation = """
        mutation (
            $delay_cal_id: String,
            $phase_up_id: String,
            $calibration_type: String!,
            $location: String,
        ) {
            createCalibration(input: {
                delayCalId: $delay_cal_id,
                phaseUpId: $phase_up_id,
                calibrationType: $calibration_type,
                location: $location
                }) {
                calibration {
                    id
                }
            }
        }
        """

        # Update an existing record
        self.update_mutation = """
        mutation ($id: Int!, $calibration_type: String!, $location: String!) {
             updateCalibration(id: $id, input: {
                calibrationType: $calibration_type,
                location: $location
            }) {
                calibration {
                    id
                    calibrationType,
                    location
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteCalibration(id: $id) {
                ok
            }
        }
        """

        self.field_names = ["id", "delayCalId", "phaseUpId", "calibrationType", "location"]

    def list(self, id=None, type=None):
        """Return a list of records matching the id and/or the type."""
        filters = [
            {"field": "type", "value": type, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, delay_cal_id, phase_up_id, type, location):
        self.create_variables = {
            "delay_cal_id": delay_cal_id,
            "phase_up_id": phase_up_id,
            "calibration_type": type,
            "location": location,
        }
        return self.create_graphql()

    def update(self, id, delay_cald_id, type, location):
        self.create_variables = {"delay_cald_id": delay_cald_id, "calibration_type": type, "location": location}
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.delay_cal_id, args.type, args.location)
        elif args.subcommand == "list":
            return self.list(args.id, args.type)
        elif args.subcommand == "update":
            return self.update(args.id, args.type, args.location)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "calibration"

    @classmethod
    def get_description(cls):
        return "A defined by its type and location"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Calibration model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing calibrations")
        parser_list.add_argument("--id", type=int, help="list calibrations matching the id [int]")
        parser_list.add_argument("--type", type=str, help="list calibrations matching the type [pre, post or none]")

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new calibration")
        parser_create.add_argument(
            "delay_cal_id", type=str, metavar="CALID", help="ID of the calibration (e.g. 20201022-0018) [str]"
        )
        parser_create.add_argument("type", type=str, metavar="TYPE", help="type of the calibration [pre, post, none]")
        parser_create.add_argument(
            "location", type=str, metavar="LOCATION", help="location of the calibration on the filesystem [str]"
        )

        parser_udpate = subs.add_parser("update", help="update the values of an existing calibration")
        parser_udpate.add_argument("id", type=int, metavar="ID", help="database id of the calibration [int]")
        parser_udpate.add_argument("type", type=str, metavar="TYPE", help="type of the calibration [pre, post, none]")
        parser_udpate.add_argument(
            "location", type=str, metavar="LOCATION", help="location of the calibration on the filesystem [int]"
        )

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing calibration")
        parser_delete.add_argument("id", type=int, help="id of the calibration")


if __name__ == "__main__":

    parser = Calibration.get_parsers()
    args = parser.parse_args()

    from psrdb.graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    c = Calibration(client, args.url, args.token)
    c.process(args)
