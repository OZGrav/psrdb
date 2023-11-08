import requests

from psrdb.graphql_table import GraphQLTable


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the PipelineImage database object on the command line in different ways based on the sub-commands.")
    PipelineImage.configure_parsers(parser)
    return parser


class PipelineImage(GraphQLTable):
    """Class for interacting with the PipelineImage database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.record_name = "pipeline_image"

        self.field_names = ["id", "image", "imageType", "resolution", "cleaned", "pipelineRun {id}"]

    def list(self, id=None, pipeline_run_id=None):
        """Return a list of PipelineImage information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        id : int, optional
            Filter by the database ID, by default None
        pipeline_run_id : int, optional
            Filter by the pipeline run ID, by default None

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        filters = [
            {"field": "id", "value": id},
            {"field": "pipelineRunId", "value": pipeline_run_id},
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
        """Create a new PipelineImage database object.

        Parameters
        ----------
        pipeline_run_id : int
            The ID of the PipelineRun database object this image is associated with.
        image_path : str
            The path to the image file.
        image_type : str
            The type of image (profile, profile-pol, phase-time, phase-freq, bandpass, snr-cumul, snr-single).
        resolution : str
            The resolution of the image (high or low).
        cleaned : bool
            Whether the image is from cleaned data (RFI removed).

        Returns
        -------
        client_response:
            A client response object.
        """
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
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "pipeline_image"

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
