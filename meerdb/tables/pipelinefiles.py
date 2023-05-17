import os

from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory
from base64 import b64encode


def prepare_file(file_fn):
    """
    open a file and encode contents in base64 for submission via graphql
    this ensures that all characters we're sending are acceptable

    input:
    file_fn: string with file name to the file (we do not verify the file is really an file)

    returns: b64-encoded string with contents of the file
    """
    if not os.path.exists(file_fn):
        raise RuntimeError(f"File to upload {file_fn} was not found")

    file_bytes = ""
    if file_fn:
        with open(file_fn, "rb") as fh:
            file_bytes = fh.read()
    # provided file name could point to an empty file
    if file_bytes:
        # encode and strip the bytes marker
        file_bytes = str(b64encode(file_bytes))[2:-1]

    return file_bytes


class Pipelinefiles(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)
        self.record_name = "pipelinefile"

        # create a new record
        self.create_mutation = """
        mutation ($file: String!, $file_name: String!, $file_type: String!, $processing_id: Int!) {
            createPipelinefile (input: {file: $file, file_name: $file_name, file_type: $file_type, processing_id: $processing_id}) {
                pipelinefile {
                    id
                }
            }
        }
        """

        # udpate an existing record
        self.update_mutation = """
        mutation ($id: Int!, $file: String!, $file_name: String!, $file_type: String!, $processing_id: Int!) {
            updatePipelinefile (id: $id, input: {file: $file, file_name: $file_name, file_type: $file_type, processing_id: $processing_id}) {
                pipelinefile {
                    id,
                    file,
                    fileType,
                    processing {id},
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deletePipelinefile(id: $id) {
                ok
            }
        }
        """

        self.field_names = ["id", "file", "fileType", "processing {id}"]

    def list(self, id=None, processing_id=None, file_type=None):
        """Return a list of records matching the id and/or the file type."""
        filters = [
            {"field": "processing", "value": processing_id, "join": "Processings"},
            {"field": "filetype", "value": file_type, "join": None},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, file, file_type, processing_id):
        prepared_file = prepare_file(file)
        self.create_variables = {
            "file": prepared_file,
            "file_name": os.path.basename(file),
            "file_type": file_type,
            "processing_id": processing_id,
        }
        return self.create_graphql()

    def update(self, id, file, file_type, processing_id):
        prepared_file = prepare_file(file)
        self.update_variables = {
            "id": id,
            "file": prepared_file,
            "file_name": os.path.basename(file),
            "file_type": file_type,
            "processing_id": processing_id,
        }
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.file, args.file_type, args.processing_id)
        elif args.subcommand == "update":
            return self.update(args.id, args.file, args.file_type, args.processing_id)
        elif args.subcommand == "list":
            return self.list(args.id, args.file_type, args.processing_id)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "pipelinefiles"

    @classmethod
    def get_description(cls):
        return "A pipelinefile with type that classifies the file"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Pipelinefiles model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        # create the parser for the "list" command
        parser_list = subs.add_parser("list", help="list existing Pipelinefiles")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list Pipelinefiles matching the id [int]")
        parser_list.add_argument(
            "--processing_id", metavar="PROC", type=int, help="list Pipelinefiles matching the processing id [int]"
        )
        parser_list.add_argument(
            "--file_type", metavar="TYPE", type=str, help="list Pipelinefiles matching the file type [str]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new pipelinefile")
        parser_create.add_argument("file", metavar="FILE", type=str, help="path to the file to be uploaded [str]")
        parser_create.add_argument(
            "file_type",
            metavar="TYPE",
            type=str,
            help='class of file type" [str]',
        )
        parser_create.add_argument(
            "processing_id", metavar="PROC", type=int, help="id of the related processing [int]"
        )

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update a new pipelinefile")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the existing pipeline file [int]")
        parser_update.add_argument("file", metavar="FILE", type=str, help="path to the file to be uploaded [str]")
        parser_update.add_argument("file_type", metavar="TYPE", type=str, help="class of file type[str]")
        parser_update.add_argument(
            "processing_id", metavar="PROC", type=int, help="id of the related processing [int]"
        )

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing pipelinefile")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the existing pipeline file [int]")


if __name__ == "__main__":
    parser = Pipelinefiles.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    p = Pipelinefiles(client, args.url, args.token)
    p.process(args)
