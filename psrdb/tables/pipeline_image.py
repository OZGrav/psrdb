import requests

from psrdb.graphql_table import GraphQLTable


class PipelineImage(GraphQLTable):
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "pipeline_image"
        self.field_names = ["id", "image", "imageType", "rank", "processing {id}"]

    def list(self, id=None, processing_id=None):
        """Return a list of records matching the id and/or the processing id."""
        filters = [
            {"field": "processing", "value": processing_id},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

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
        elif args.subcommand == "list":
            return self.list(args.id, args.processing_id)
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

