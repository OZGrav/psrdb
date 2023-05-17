from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory
from base64 import b64encode


def prepare_image(img_fn):
    """
    open a file and encode contents in base64 for submission via graphql
    this ensures that all characters we're sending are acceptable

    input:
    img_fn: string with file name to the image file (we do not verify the file is really an image)

    returns: b64-encoded string with contents of the file
    """
    img_bytes = ""
    if img_fn and img_fn != '""':
        with open(img_fn, "rb") as fh:
            img_bytes = fh.read()
    # provided file name could point to an empty file
    if img_bytes:
        # encode and strip the bytes marker
        img_bytes = str(b64encode(img_bytes))[2:-1]

    return img_bytes


class Pipelineimages(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)
        self.record_name = "pipelineimage"

        # create a new record
        self.create_mutation = """
        mutation ($image: String!, $image_type: String!, $processing_id: Int!, $rank: Int!) {
            createPipelineimage (input: {image: $image, image_type: $image_type, processing_id: $processing_id, rank: $rank}) {
                pipelineimage {
                    id
                }
            }
        }
        """

        # udpate an existing record
        self.update_mutation = """
        mutation ($id: Int!, $image: String!, $image_type: String!, $processing_id: Int!, $rank: Int!) {
            updatePipelineimage (id: $id, input: {image: $image, image_type: $image_type, processing_id: $processing_id, rank: $rank}) {
                pipelineimage {
                    id,
                    image,
                    imageType,
                    processing {id},
                    rank
                }
            }
        }
        """

        self.delete_mutation = """
        mutation ($id: Int!) {
            deletePipelineimage(id: $id) {
                ok
            }
        }
        """

        self.field_names = ["id", "image", "imageType", "rank", "processing {id}"]

    def list(self, id=None, processing_id=None):
        """Return a list of records matching the id and/or the processing id."""
        filters = [
            {"field": "processing", "value": processing_id, "join": "Processings"},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

    def create(self, image, image_type, rank, processing_id):
        prepared_image = prepare_image(image)
        self.create_variables = {
            "image": prepared_image,
            "image_type": image_type,
            "rank": rank,
            "processing_id": processing_id,
        }
        return self.create_graphql()

    def update(self, id, image, image_type, rank, processing_id):
        prepared_image = prepare_image(image)
        self.update_variables = {
            "id": id,
            "image": prepared_image,
            "image_type": image_type,
            "rank": rank,
            "processing_id": processing_id,
        }
        return self.update_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(args.image, args.image_type, args.rank, args.processing_id)
        elif args.subcommand == "update":
            return self.update(args.id, args.image, args.image_type, args.rank, args.processing_id)
        elif args.subcommand == "list":
            return self.list(args.id, args.processing_id)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "pipelineimages"

    @classmethod
    def get_description(cls):
        return "A pipelineimage with type and rank informing the position of the image"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("PipelineImages model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        # create the parser for the "list" command
        parser_list = subs.add_parser("list", help="list existing Pipelineimages")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list Pipelineimages matching the id [int]")
        parser_list.add_argument(
            "--processing_id", metavar="PROC", type=int, help="list Pipelineimages matching the processing id [int]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="create a new pipelineimage")
        parser_create.add_argument(
            "image_type",
            metavar="TYPE",
            type=str,
            help='description of image type, e.g., "flux" or "snr" or "bandpass" [str]',
        )
        parser_create.add_argument(
            "processing_id", metavar="PROC", type=int, help="id of the related processing [int]"
        )
        parser_create.add_argument(
            "rank",
            metavar="RANK",
            type=int,
            help="rank of the image, used to indicate the order of displaing the image [int]",
        )
        parser_create.add_argument("image", metavar="IMG", type=str, help="path to the image to be uploaded [str]")

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update a new pipelineimage")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the existing pipeline image [int]")
        parser_update.add_argument(
            "image_type",
            metavar="TYPE",
            type=str,
            help='description of image type, e.g., "flux" or "snr" or "bandpass" [str]',
        )
        parser_update.add_argument(
            "processing_id", metavar="PROC", type=int, help="id of the related processing [int]"
        )
        parser_update.add_argument(
            "rank",
            metavar="RANK",
            type=int,
            help="rank of the image, used to indicate the order of displaing the image [int]",
        )
        parser_update.add_argument("image", metavar="IMG", type=str, help="path to the image to be uploaded [str]")

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing pipelineimage")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the existing pipeline image [int]")


if __name__ == "__main__":
    parser = Pipelineimages.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    p = Pipelineimages(client, args.url, args.token)
    p.process(args)
