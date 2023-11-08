from psrdb.graphql_table import GraphQLTable


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the MainProject database object on the command line in different ways based on the sub-commands.")
    MainProject.configure_parsers(parser)
    return parser

class MainProject(GraphQLTable):
    """Class for interacting with the MainProject database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "main_project"
        self.field_names = ["id", "telescope {name} ", "name"]

    def list(self, id=None, telescope=None, name=None):
        """Return a list of MainProject information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        id : int, optional
            Filter by the database ID, by default None
        telescope : str, optional
            Filter by the telescope name, by default None
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
            {"field": "telescope", "value": telescope},
            {"field": "name", "value": name},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def create(self, telescope, name):
        """Create a new MainProject database object.

        Parameters
        ----------
        telescope : str
            The telescope name
        name : str
            The name of the MainProject

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "createMainProject"
        self.mutation = """
        mutation ($telescope: String!, $name: String!) {
            createMainProject(input: {
                telescopeName: $telescope,
                name: $name,
                }) {
                mainproject {
                    id
                }
            }
        }
        """
        self.variables = {
            "telescope": telescope,
            "name": name,
        }
        return self.mutation_graphql()

    def update(self, id, telescope, name):
        """Update a MainProject database object.

        Parameters
        ----------
        id : int
            The database ID
        telescope : str
            The telescope name
        name : str
            The name of the MainProject

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "updateMainProject"
        self.mutation = """
        mutation ($id: Int!, $telescope: String!, $name: String!) {
            updateMainProject(id: $id, input: {
                telescopeName: $telescope,
                name: $name,
                }) {
                mainproject {
                    id,
                    name,
                }
            }
        }
        """
        self.variables = {
            "id": id,
            "telescope": telescope,
            "name": name,
        }
        return self.mutation_graphql()

    def delete(self, id):
        """Delete a MainProject database object.

        Parameters
        ----------
        id : int
            The database ID

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "deleteMainProject"
        self.mutation = """
        mutation ($id: Int!) {
            deleteMainProject(id: $id) {
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
            return self.list(args.id, args.telescope, args.name)
        elif args.subcommand == "create":
            return self.create(args.telescope, args.name)
        elif args.subcommand == "update":
            return self.update(args.id, args.telescope, args.name)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "main_project"

    @classmethod
    def get_description(cls):
        return "A MainProject defined by a code, short name, embargo period and a description"

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing MainProject")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list MainProject matching the id [int]")
        parser_list.add_argument(
            "--telescope", metavar="TEL", type=int, help="list MainProject matching the telescope id [int]"
        )
        parser_list.add_argument("--name", metavar="NAME", type=str, help="list MainProject matching the name [str]")

