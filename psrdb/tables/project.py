from psrdb.graphql_table import GraphQLTable


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the Project database object on the command line in different ways based on the sub-commands.")
    Project.configure_parsers(parser)
    return parser


class Project(GraphQLTable):
    """Class for interacting with the Project database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "project"
        self.field_names = ["id", "mainProject {name}", "code", "short", "embargoPeriod", "description"]

    def list(self, id=None, mainProject=None, code=None):
        """Return a list of Project information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        id : int, optional
            Filter by the database ID, by default None
        mainProject : str, optional
            Filter by the mainProject name, by default None
        code : str, optional
            Filter by the code, by default None

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        filters = [
            {"field": "id", "value": id},
            {"field": "mainProject", "value": mainProject},
            {"field": "code", "value": code},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def create(self, main_project, code, short, embargo_period, description):
        """Create a new Project database object.

        Parameters
        ----------
        main_project : str
            The name of the MainProject this project is under.
        code : str
            The code of the project.
        short : str
            The short name of the project (e.g. PTA).
        embargo_period : int
            The embargo period in days.
        description : str
            A description of the project.

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "createProject"
        self.mutation = """
        mutation ($code: String!, $main_project: String!, $short: String!, $embargoPeriod: Int!, $description: String!) {
            createProject(input: {
                mainProjectName: $main_project,
                code: $code,
                short: $short,
                embargoPeriod: $embargoPeriod,
                description: $description
                }) {
                project {
                    id
                }
            }
        }
        """
        self.variables = {
            "main_project": main_project,
            "code": code,
            "short": short,
            "embargoPeriod": embargo_period,
            "description": description,
        }
        return self.mutation_graphql()

    def update(self, id, main_project, code, short, embargo_period, description):
        """Update a Project database object.

        Parameters
        ----------
        id : int
            The database ID
        main_project : str
            The name of the MainProject this project is under.
        code : str
            The code of the project.
        short : str
            The short name of the project (e.g. PTA).
        embargo_period : int
            The embargo period in days.
        description : str
            A description of the project.

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "updateProject"
        self.mutation = """
        mutation ($id: Int!, $main_project: String!, $code: String!, $short: String!, $embargoPeriod: Int!, $description: String!) {
            updateProject(id: $id, input: {
                mainProjectName: $main_project,
                code: $code,
                short: $short,
                embargoPeriod: $embargoPeriod,
                description: $description
                }) {
                project {
                    id,
                    mainProject {name},
                    code,
                    short,
                    embargoPeriod,
                    description
                }
            }
        }
        """
        self.variables = {
            "id": id,
            "main_project": main_project,
            "code": code,
            "short": short,
            "embargoPeriod": embargo_period,
            "description": description,
        }
        return self.mutation_graphql()

    def delete(self, id):
        """Delete a Project database object.

        Parameters
        ----------
        id : int
            The database ID

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "deleteProject"
        self.mutation = """
        mutation ($id: Int!) {
            deleteProject(id: $id) {
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
        if args.subcommand == "create":
            return self.create(args.mainproject, args.code, args.short, args.embargo_period, args.description)
        elif args.subcommand == "update":
            return self.update(args.id, args.mainproject, args.code, args.short, args.embargo_period, args.description)
        elif args.subcommand == "list":
            return self.list(args.id, args.mainproject, args.code)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "project"

    @classmethod
    def get_description(cls):
        return "A project defined by a code, short name, embargo period and a description"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Project model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing projects")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list projects matching the id [int]")
        parser_list.add_argument(
            "--mainproject", metavar="MAIN", type=str, help="list projects matching the mainproject name [str]"
        )
        parser_list.add_argument("--code", metavar="CODE", type=str, help="list projects matching the code [str]")


