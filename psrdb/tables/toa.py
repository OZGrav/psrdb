from psrdb.graphql_table import GraphQLTable
from psrdb.graphql_query import graphql_query_factory
from psrdb.utils.toa import toa_line_to_dict, toa_dict_to_line


class Toa(GraphQLTable):
    def __init__(self, client, token):
        GraphQLTable.__init__(self, client, token)
        self.record_name = "toa"

            # Convert string to Decimal
        # create a new record
        self.create_mutation = """
        mutation (
            $pipelineRunId: Int!,
            $ephemerisId: Int!,
            $templateId: Int!,
            $archive: String!,
            $freqMHz: Float!,
            $mjd: Decimal!,
            $mjdErr: Float!,
            $telescope: String!,
            $fe: String,
            $be: String,
            $f: String,
            $bw: Int,
            $tobs: Int,
            $tmplt: String,
            $gof: Float,
            $nbin: Int,
            $nch: Int,
            $chan: Int,
            $rcvr: String,
            $snr: Float,
            $length: Int,
            $subint: Int,
        ) {
            createToa (input: {
                pipelineRunId: $pipelineRunId,
                ephemerisId: $ephemerisId,
                templateId: $templateId,
                archive: $archive,
                freqMHz: $freqMHz,
                mjd: $mjd,
                mjdErr: $mjdErr,
                telescope: $telescope,
                fe: $fe,
                be: $be,
                f: $f,
                bw: $bw,
                tobs: $tobs,
                tmplt: $tmplt,
                gof: $gof,
                nbin: $nbin,
                nch: $nch,
                chan: $chan,
                rcvr: $rcvr,
                snr: $snr,
                length: $length,
                subint: $subint,
            }) {
                toa {
                    id,
                }
            }
        }
        """

        self.update_mutation = """
        mutation ($id: Int!, $processing: Int!, $inputFolding: Int!, $timingEphemeris: Int!, $template: Int!, $flags: JSONString!, $frequency: Float!, $mjd: String!, $site: String!, $uncertainty: Float!, $quality: String!, $comment: String!) {
            updateToa (id: $id, input: {
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
                toa {
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

        self.delete_mutation = """
        mutation ($id: Int!) {
            deleteToa(id: $id) {
                ok
            }
        }
        """

        self.field_names = [
            "id",
            "processing {id}",
            "inputFolding {id}",
            "timingEphemeris {id}",
            "template {id}",
            "flags",
            "frequency",
            "mjd",
            "site",
            "uncertainty",
            "quality",
            "comment",
        ]
        self.literal_field_names = [
            "id",
            "processing {id}",
            "inputFolding {id}",
            "timingEphemeris {id}",
            "template {id}",
            "flags",
            "frequency",
            "mjd",
            "site",
            "uncertainty",
            "quality",
            "comment",
        ]

    def list(self, id=None, processing_id=None, input_folding_id=None, timing_ephemeris_id=None, template_id=None):
        """Return a list of records matching the id and/or the provided arguments."""
        filters = [
            {"field": "processingId", "value": processing_id, "join": "Processings"},
            {"field": "inputFoldingId", "value": input_folding_id, "join": "Foldings"},
            {"field": "timingEphemerisId", "value": timing_ephemeris_id, "join": "Ephemerides"},
            {"field": "templateId", "value": template_id, "join": "Templates"},
        ]
        graphql_query = graphql_query_factory(self.table_name, self.record_name, id, filters)
        return GraphQLTable.list_graphql(self, graphql_query)

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
        self.update_variables = {
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
        return self.update_graphql()

    def create(
        self,
        pipeline_run_id,
        ephemeris_id,
        template_id,
        input_toa_line,
    ):
        # Loop over toa lines and turn into a dict
        toa_dict = toa_line_to_dict(input_toa_line)
        # Revert it back to a line and check it matches before uploading
        output_toa_line = toa_dict_to_line(toa_dict)
        assert input_toa_line == output_toa_line
        # Upload the toa
        self.create_variables = {
            'pipelineRunId': pipeline_run_id,
            'ephemerisId': ephemeris_id,
            'templateId': template_id,
            'archive': toa_dict['archive'],
            'freqMHz': toa_dict['freq_MHz'],
            # Convert decimal type to string so it can be json encoded
            'mjd': str(toa_dict['mjd']),
            'mjdErr': toa_dict['mjd_err'],
            'telescope': toa_dict['telescope'],
            'fe': toa_dict['fe'],
            'be': toa_dict['be'],
            'f': toa_dict['f'],
            'bw': toa_dict['bw'],
            'tobs': toa_dict['tobs'],
            'tmplt': toa_dict['tmplt'],
            'gof': toa_dict['gof'],
            'nbin': toa_dict['nbin'],
            'nch': toa_dict['nch'],
            'chan': toa_dict['chan'],
            'rcvr': toa_dict['rcvr'],
            'snr': toa_dict['snr'],
            'length': toa_dict['length'],
            'subint': toa_dict['subint'],
        }
        return self.create_graphql()


    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":

            with open(args.toa_path, "r") as f:
                toa_lines = f.readlines()
                for toa_line in toa_lines[1:]:
                    input_toa_line = toa_line.rstrip("\n")
                    self.create(
                        args.pipeline_run_id,
                        args.ephemeris_id,
                        args.template_id,
                        input_toa_line,
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
        elif args.subcommand == "list":
            return self.list(args.id, args.processing, args.folding, args.ephemeris, args.template)
        elif args.subcommand == "delete":
            return self.delete(args.id)
        else:
            raise RuntimeError(args.subcommand + " command is not implemented")

    @classmethod
    def get_name(cls):
        return "toa"

    @classmethod
    def get_description(cls):
        return "A pulsar toa/standard"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Toa model parser")
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
        parser_list.add_argument("--id", metavar="ID", type=int, help="list toa matching the id [int]")
        parser_list.add_argument(
            "--processing", metavar="PROC", type=int, help="list toa matching the processing id [int]"
        )
        parser_list.add_argument("--folding", metavar="FOLD", type=int, help="list toa that used the folding id [int]")
        parser_list.add_argument(
            "--ephemeris", metavar="EPH", type=int, help="list toa matching the timing ephemeris id [int]"
        )
        parser_list.add_argument(
            "--template", metavar="TEMPL", type=int, help="list toa matching the template id [int]"
        )

        # create the parser for the "create" command
        parser_create = subs.add_parser("create", help="Create a new TOA")
        parser_create.add_argument(
            "pipeline_run_id", metavar="RUN", type=int, help="ID of the PipelineRun of this TOA [int]"
        )
        parser_create.add_argument(
            "ephemeris_id", metavar="EPH", type=int, help="ID of the timing ephemeris used in this this TOA [int]"
        )
        parser_create.add_argument(
            "template_id", metavar="TEMPL", type=int, help="ID of the standard/template used in this this TOA [int]"
        )
        parser_create.add_argument(
            "toa_path", metavar="TOA", type=str, help="Path to the TOA file [str]"
        )

        # create the parser for the "update" command
        parser_update = subs.add_parser("update", help="update an existing toa")
        parser_update.add_argument("id", metavar="ID", type=int, help="id of the toa to update [int]")
        parser_update.add_argument(
            "processing", metavar="PROC", type=int, help="id of the processing to which this toa applies [int]"
        )
        parser_update.add_argument(
            "folding", metavar="FOLD", type=int, help="id of the folding which is input to this toa [int]"
        )
        parser_update.add_argument(
            "ephemeris", metavar="EPH", type=int, help="id of the timing ephemeris used in this this toa [int]"
        )
        parser_update.add_argument(
            "template", metavar="TEMPL", type=int, help="id of the standard/template used in this this toa [int]"
        )
        parser_update.add_argument("flags", metavar="FLAGS", type=str, help="flags used in this toa [str]")
        parser_update.add_argument(
            "frequency", metavar="FREQ", type=float, help="frequency of this toa in MHz [float]]"
        )
        parser_update.add_argument(
            "mjd", metavar="MJD", type=str, help="modified julian data for this toa in days [str]"
        )
        parser_update.add_argument("site", metavar="SITE", type=str, help="site of code of this toa [str[1]]")
        parser_update.add_argument("uncertainty", metavar="ERR", type=float, help="uncertainty of this toa [float]")
        parser_update.add_argument("quality", metavar="QUAL", type=str, help="quality of this toa [nominal, bad]")
        parser_update.add_argument("comment", metavar="COMMENT", type=str, help="comment about the toa [str]")

        # create the parser for the "delete" command
        parser_delete = subs.add_parser("delete", help="delete an existing toa")
        parser_delete.add_argument("id", metavar="ID", type=int, help="id of the toa to update [int]")


if __name__ == "__main__":
    parser = Toa.get_parsers()
    args = parser.parse_args()

    from psrdb.graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    t = Toa(client, args.url, args.token)
    t.process(args)
