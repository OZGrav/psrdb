import requests
from base64 import b64encode

from psrdb.graphql_table import GraphQLTable
from psrdb.graphql_query import graphql_query_factory


class PipelineImage(GraphQLTable):
    def __init__(self, client, token):
        GraphQLTable.__init__(self, client, token)
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

    def create(
            self,
            pipeline_run_id,
            image_path,
            image_type,
            resolution,
            cleaned,
        ):
        # Open the file in binary mode
        with open(image_path, 'rb') as file:
            variables = {
                "pipeline_run_id": pipeline_run_id,
                "image_type": image_type,
                "resolution": resolution,
                "cleaned": cleaned,
            }
            files = {
                "image_upload": file,
            }
            # Post to the rest api
            response = requests.post(f'{self.client.rest_api_url}image/', data=variables, files=files)

        return response

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
            return self.create(
                args.pipeline_run_id,
                args.image_path,
                args.image_type,
                args.resolution,
                args.cleaned,
            )
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
        return "pipelineimage"

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
        parser_list = subs.add_parser("list", help="list existing PipelineImage")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list PipelineImage matching the id [int]")
        parser_list.add_argument(
            "--processing_id", metavar="PROC", type=int, help="list PipelineImage matching the processing id [int]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="Create a new PipelineImage")
        parser_create.add_argument(
            "pipeline_run_id", metavar="RUN", type=int, help="ID of the related pipeline run [int]"
        )
        parser_create.add_argument(
            "image_path", metavar="IMG", type=str, help="Path to the image to be uploaded [str]"
        )
        parser_create.add_argument(
            "image_type",
            metavar="TYPE",
            type=str,
            help='Description of image type from ("profile", "profile-pol", "phase-time", "phase-freq", "bandpass", "snr-cumul", "snr-single") [str]',
        )
        parser_create.add_argument(
            "resolution",
            metavar="RES",
            type=str,
            help='Resolution of the image from ("high", "low") [str]',
        )
        parser_create.add_argument(
            "--cleaned",
            action="store_true",
            default=False,
            help='If the image is from cleaned data (RFI removed) [bool] (default: False)',
        )

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
    parser = PipelineImage.get_parsers()
    args = parser.parse_args()

    from psrdb.graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    p = PipelineImage(client, args.url, args.token)
    p.process(args)
