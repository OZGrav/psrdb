from pulsar_paragraph.pulsar_paragraph import create_pulsar_paragraph

from psrdb.graphql_table import GraphQLTable


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the Pulsar database object on the command line in different ways based on the sub-commands.")
    Pulsar.configure_parsers(parser)
    return parser


class Pulsar(GraphQLTable):
    """Class for interacting with the Pulsar database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "pulsar"
        self.field_names = ["id", "name", "comment"]

    def list(self, id=None, name=None):
        """Return a list of Pulsar information based on the `self.field_names` and filtered by the parameters.

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

    def create(self, name, comment=None):
        """Create a new Pulsar database object.

        Parameters
        ----------
        name : str
            The name of the pulsar.
        comment : str, optional
            A description of the pulsar (normally produced by the `pulsar_paragraph` package), by default None

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "createPulsar"
        self.mutation = """
        mutation ($name: String!, $comment: String) {
            createPulsar(input: {
                name: $name, comment: $comment
            }) {
                pulsar {
                    id
                }
            }
        }
        """
        if comment is None:
            # Generate pulsar paragraph
            paragraphs = create_pulsar_paragraph(pulsar_names=[name])
            if len(paragraphs) > 0:
                comment = paragraphs[0]
            else:
                comment = None
        self.variables = {
            "name": name,
            "comment": comment,
        }
        return self.mutation_graphql()

    def update(self, id, name, comment):
        """Update a Pulsar database object.

        Parameters
        ----------
        id : int
            The database ID
        name : str
            The name of the pulsar.
        comment : str, optional
            A description of the pulsar (normally produced by the `pulsar_paragraph` package), by default None

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "updatePulsar"
        self.mutation = """
        mutation ($id: Int!, $name: String!, $comment: String) {
            updatePulsar(id: $id, input: {
                name: $name,
                comment: $comment
            }) {
                pulsar {
                    id,
                    name,
                    comment
                }
            }
        }
        """
        if comment is None:
            # Generate pulsar paragraph
            comment = create_pulsar_paragraph(pulsar_names=[name])[0]
        self.variables = {
            "id": id,
            "name": name,
            "comment": comment,
        }
        return self.mutation_graphql()

    def delete(self, id):
        """Delete a Pulsar database object.

        Parameters
        ----------
        id : int
            The database ID

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "deletePulsar"
        self.mutation = """
        mutation ($id: Int!) {
            deletePulsar(id: $id) {
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
            return self.create(args.name, args.comment)
        elif args.subcommand == "update":
            return self.update(args.id, args.name, args.comment)
        elif args.subcommand == "list":
            return self.list(args.id, args.name)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "pulsar"

    @classmethod
    def get_description(cls):
        return "A pulsar source defined by a J2000 name"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Pulsar model parser")
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
        parser_list.add_argument("--id", metavar="ID", type=int, help="list Pulsars matching the id [int]")
        parser_list.add_argument("--name", metavar="name", type=str, help="list Pulsars matching the name [str]")

