from psrdb.graphql_table import GraphQLTable


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the Telescope database object on the command line in different ways based on the sub-commands.")
    Telescope.configure_parsers(parser)
    return parser


class Telescope(GraphQLTable):
    """Class for interacting with the Telescope database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "telescope"
        self.field_names = ["id", "name"]

    def list(self, id=None, name=None):
        """Return a list of Telescope information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        id : int, optional
            Filter by the database ID, by default None
        name : str, optional
            Filter by the name, by default None

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        filters = [
            {"field": "id", "value": id},
            {"field": "name", "value": name},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def create(self, name):
        """Create a new Telescope database object.

        Parameters
        ----------
        name : str
            The name of the Telescope.

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "createTelescope"
        self.mutation = """
        mutation ($name: String!) {
            createTelescope(input: {
                name: $name,
                }) {
                telescope {
                    id
                }
            }
        }
        """
        self.variables = {
            "name": name,
        }
        return self.mutation_graphql()

    def update(self, id, name):
        """Update a Telescope database object.

        Parameters
        ----------
        id : int
            The database ID.
        name : str
            The name of the Telescope.

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "updateTelescope"
        self.mutation = """
        mutation ($id: Int!, $name: String!) {
            updateTelescope(id: $id, input: {
                name: $name,
                }) {
                telescope {
                    id,
                    name
                }
            }
        }
        """
        self.variables = {
            "id": id,
            "name": name,
        }
        return self.mutation_graphql()

    def delete(self, id):
        """Delete a Telescope database object.

        Parameters
        ----------
        id : int
            The database ID

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "deleteTelescope"
        self.mutation = """
        mutation ($id: Int!) {
            deleteTelescope(id: $id) {
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
        if args.subcommand == "list":
            return self.list(args.id, args.name)
        elif args.subcommand == "create":
            return self.create(args.name)
        elif args.subcommand == "update":
            return self.update(args.id, args.name)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "telescope"

    @classmethod
    def get_description(cls):
        return "A telescope defined by a name"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Telescope model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing telescopes")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list telescopes matching the id [int]")
        parser_list.add_argument("--name", metavar="NAME", type=str, help="list telescopes matching the name [str]")


