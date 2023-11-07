from psrdb.graphql_table import GraphQLTable
from psrdb.utils.residual import residual_line_to_dict
from psrdb.utils.other import decode_id


def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


class Residual(GraphQLTable):
    def __init__(self, client):
        GraphQLTable.__init__(self, client)
        self.table_name = "residual"
        self.field_names = [
            "id",
            "pipelineRun{ id }",
            "ephemeris { id }",
            "template { id }",
            "archive",
            "freqMhz",
            "mjd",
            "mjdErr",
            "telescope",
            "fe",
            "be",
            "f",
            "bw",
            "tobs",
            "tmplt",
            "gof",
            "nbin",
            "nch",
            "chan",
            "rcvr",
            "snr",
            "length",
            "subint",
        ]

    def list(self, id=None, processing_id=None, input_folding_id=None, timing_ephemeris_id=None, template_id=None):
        """Return a list of records matching the id and/or the provided arguments."""
        filters = [
            {"field": "processingId", "value": processing_id},
            {"field": "inputFoldingId", "value": input_folding_id},
            {"field": "timingEphemerisId", "value": timing_ephemeris_id},
            {"field": "templateId", "value": template_id},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def create(
        self,
        pulsar,
        project_short,
        ephemeris,
        residual_lines,
    ):
        self.mutation_name = "createResidual"
        self.mutation = """
        mutation (
            $pulsar: String!,
            $projectShort: String!,
            $ephemerisText: String!,
            $residualLines: [String]!,
        ) {
            createResidual (input: {
                pulsar: $pulsar,
                projectShort: $projectShort,
                ephemerisText: $ephemerisText,
                residualLines: $residualLines,
            }) {
                residual {
                    id,
                }
            }
        }
        """
        # Read ephemeris file
        with open(ephemeris, "r") as f:
            ephemeris_str = f.read()
        # Loop over the lines and grab the important info to reduce upload size
        residual_line_info = []
        for residual_line in residual_lines[1:]:
            residual_line = residual_line.rstrip("\n")
            # Loop over residual lines and turn into a dict
            residual_dict = residual_line_to_dict(residual_line)
            # return only important info as a comma sperated string
            residual_line_info.append(f"{decode_id(residual_dict['id'])},{residual_dict['mjd']},{residual_dict['residual']},{residual_dict['residual_error']},{residual_dict['residual_phase']}")
        # Upload the residuals 1000 at a time
        for residual_chunk in chunk_list(residual_line_info, 1000):
            self.variables = {
                'pulsar': pulsar,
                'projectShort': project_short,
                'ephemerisText': ephemeris_str,
                'residualLines': residual_chunk,
            }
            self.mutation_graphql()

    def update(
        self,
        id,
        processing,
        input_folding,
        timing_ephemeris,
        template,
        flags,
        frequency,
        mjd,
        site,
        uncertainty,
        quality,
        comment,
    ):
        self.mutation_name = "updateResidual"
        self.mutation = """
        mutation ($id: Int!, $processing: Int!, $inputFolding: Int!, $timingEphemeris: Int!, $template: Int!, $flags: JSONString!, $frequency: Float!, $mjd: String!, $site: String!, $uncertainty: Float!, $quality: String!, $comment: String!) {
            updateResidual (id: $id, input: {
                processing_id: $processing,
                input_folding_id: $inputFolding,
                timing_ephemeris_id: $timingEphemeris,
                template_id: $template,
                flags: $flags,
                frequency: $frequency,
                mjd: $mjd,
                site: $site,
                uncertainty: $uncertainty,
                quality: $quality,
                comment: $comment
            }) {
                residual {
                    id,
                    processing {id},
                    inputFolding {id},
                    timingEphemeris {id},
                    template {id},
                    flags,
                    frequency,
                    mjd,
                    site,
                    uncertainty,
                    quality,
                    comment
                }
            }
        }
        """
        self.variables = {
            "id": id,
            "processing": processing,
            "inputFolding": input_folding,
            "timingEphemeris": timing_ephemeris,
            "template": template,
            "flags": flags,
            "frequency": frequency,
            "mjd": mjd,
            "site": site,
            "uncertainty": uncertainty,
            "quality": quality,
            "comment": comment,
        }
        return self.mutation_graphql()

    def delete(
        self,
        id,
    ):
        self.mutation_name = "deleteResidual"
        self.mutation = """
        mutation ($id: Int!) {
            deleteResidual(id: $id) {
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
            with open(args.residual_path, "r") as f:
                residual_lines = f.readlines()
                return self.create(
                    args.pulsar,
                    args.project_short,
                    args.ephemeris,
                    residual_lines,
                )
        elif args.subcommand == "update":
            return self.update(
                args.id,
                args.processing,
                args.folding,
                args.ephemeris,
                args.template,
                args.flags,
                args.frequency,
                args.mjd,
                args.site,
                args.uncertainty,
                args.quality,
                args.comment,
            )
        elif args.subcommand == "delete":
            return self.delete(args.id)
        elif args.subcommand == "list":
            return self.list(args.id, args.processing, args.folding, args.ephemeris, args.template)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "residual"

    @classmethod
    def get_description(cls):
        return "A pulsar residual/standard"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Residual model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing ephemerides")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list residual matching the id [int]")
        parser_list.add_argument(
            "--processing", metavar="PROC", type=int, help="list residual matching the processing id [int]"
        )
        parser_list.add_argument("--folding", metavar="FOLD", type=int, help="list residual that used the folding id [int]")
        parser_list.add_argument(
            "--ephemeris", metavar="EPH", type=int, help="list residual matching the timing ephemeris id [int]"
        )
        parser_list.add_argument(
            "--template", metavar="TEMPL", type=int, help="list residual matching the template id [int]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="Create a new Residual")
        parser_create.add_argument(
            "pulsar", metavar="PULSAR", type=str, help="Name of the pulsar [str]"
        )
        parser_create.add_argument(
            "ephemeris", metavar="EPH", type=str, help="Path to the timing ephemeris used to create this residual file [str]"
        )
        parser_create.add_argument(
            "project_short", metavar="PROJ", type=str, help="Short code of the project [str]"
        )
        parser_create.add_argument(
            "residual_path", metavar="TOA", type=str, help="Path to the residual file [str]"
        )

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update an existing residual")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the residual to update [int]")
        parser_update.add_argument(
            "processing", metavar="PROC", type=int, help="id of the processing to which this residual applies [int]"
        )
        parser_update.add_argument(
            "folding", metavar="FOLD", type=int, help="id of the folding which is input to this residual [int]"
        )
        parser_update.add_argument(
            "ephemeris", metavar="EPH", type=int, help="id of the timing ephemeris used in this this residual [int]"
        )
        parser_update.add_argument(
            "template", metavar="TEMPL", type=int, help="id of the standard/template used in this this residual [int]"
        )
        parser_update.add_argument("flags", metavar="FLAGS", type=str, help="flags used in this residual [str]")
        parser_update.add_argument(
            "frequency", metavar="FREQ", type=float, help="frequency of this residual in MHz [float]]"
        )
        parser_update.add_argument(
            "mjd", metavar="MJD", type=str, help="modified julian data for this residual in days [str]"
        )
        parser_update.add_argument("site", metavar="SITE", type=str, help="site of code of this residual [str[1]]")
        parser_update.add_argument("uncertainty", metavar="ERR", type=float, help="uncertainty of this residual [float]")
        parser_update.add_argument("quality", metavar="QUAL", type=str, help="quality of this residual [nominal, bad]")
        parser_update.add_argument("comment", metavar="COMMENT", type=str, help="comment about the residual [str]")

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing residual")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the residual to update [int]")

