from psrdb.graphql_table import GraphQLTable
from psrdb.graphql_query import graphql_query_factory
from psrdb.utils.toa import toa_line_to_dict, toa_dict_to_line


class Toa(GraphQLTable):
    def __init__(self, client, token):
        GraphQLTable.__init__(self, client, token)
        self.record_name = "toa"

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
            {"field": "processingId", "value": processing_id},
            {"field": "inputFoldingId", "value": input_folding_id},
            {"field": "timingEphemerisId", "value": timing_ephemeris_id},
            {"field": "templateId", "value": template_id},
        ]
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def create(
        self,
        pipeline_run_id,
        ephemeris_id,
        template_id,
        toa_lines,
        dmCorrected,
        minimumNsubs,
        maximumNsubs,
    ):
        self.mutation_name = "createToa"
        self.mutation = """
        mutation (
            $pipelineRunId: Int!,
            $ephemerisId: Int!,
            $templateId: Int!,
            $toaLines: [String]!,
            $dmCorrected: Boolean!,
            $minimumNsubs: Boolean!,
            $maximumNsubs: Boolean!,
        ) {
            createToa (input: {
                pipelineRunId: $pipelineRunId,
                ephemerisId: $ephemerisId,
                templateId: $templateId,
                toaLines: $toaLines,
                dmCorrected: $dmCorrected,
                minimumNsubs: $minimumNsubs,
                maximumNsubs: $maximumNsubs,
            }) {
                toa {
                    id,
                }
            }
        }
        """
        # Upload the toa
        self.variables = {
            'pipelineRunId': pipeline_run_id,
            'ephemerisId': ephemeris_id,
            'templateId': template_id,
            'toaLines': toa_lines,
            'dmCorrected': dmCorrected,
            'minimumNsubs': minimumNsubs,
            'maximumNsubs': maximumNsubs,
        }
        return self.mutation_graphql()

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
        self.mutation_name = "updateToa"
        self.mutation = """
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
        self.mutation_name = "deleteToa"
        self.mutation = """
        mutation ($id: Int!) {
            deleteToa(id: $id) {
                ok
            }
        }
        """
        self.variables = {
            "id": id,
        }
        return self.mutation_graphql()

    def download(
        self,
        pulsar,
        id=None,
        pipeline_run_id=None,
        dm_corrected=None,
        minimum_nsubs=None,
        maximum_nsubs=None,
        obs_nchan=None,
    ):
        # Grab a dictionary of the toas
        filters = [
            {"field": "id", "value": id},
            {"field": "pulsar", "value": pulsar},
            {"field": "pipelineRunId", "value": pipeline_run_id},
            {"field": "dmCorrected", "value": dm_corrected},
            {"field": "minimumNsubs", "value": minimum_nsubs},
            {"field": "maximumNsubs", "value": maximum_nsubs},
            {"field": "obsNchan", "value": obs_nchan},
        ]
        self.get_dicts = True
        toa_dicts = GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

        # Create the output name
        output_name = f"toa_{pulsar}"
        if id is not None:
            output_name += f"_id{id}"
        if pipeline_run_id is not None:
            output_name += f"_pipeline_run_id{pipeline_run_id}"
        if dm_corrected is not None and dm_corrected:
            output_name += f"_dm_corrected"
        if minimum_nsubs is not None and minimum_nsubs:
            output_name += f"_minimum_nsubs"
        if maximum_nsubs is not None and maximum_nsubs:
            output_name += f"_maximum_nsubs"
        if obs_nchan is not None:
            output_name += f"_nchan{obs_nchan}"
        output_name += ".tim"

        # Loop over the toas and dump them as a file
        with open(output_name, "w") as f:
            f.write("FORMAT 1\n")
            for toa_dict in toa_dicts:
                # Convert to toa format
                # del toa_dict["id"]
                del toa_dict["pipelineRun"]
                del toa_dict["ephemeris"]
                del toa_dict["template"]
                toa_dict["freq_MHz"] = toa_dict["freqMhz"]
                toa_dict["mjd_err"] = toa_dict["mjdErr"]
                del toa_dict["freqMhz"]
                del toa_dict["mjdErr"]
                toa_line = toa_dict_to_line(toa_dict)
                f.write(f"{toa_line}\n")
        return output_name


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
        elif args.subcommand == "delete":
            return self.delete(args.id)
        elif args.subcommand == "list":
            return self.list(args.id, args.processing, args.folding, args.ephemeris, args.template)
        elif args.subcommand == "download":
            return self.download(
                args.pulsar,
                args.id,
                args.pipeline_run_id,
                args.dm_corrected,
                args.minimum_nsubs,
                args.maximum_nsubs,
                args.nchan,
            )
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

        # create the parser for the "download" command
        parser_download = subs.add_parser("download", help="Download TOAs for a pulsar to a .tim file")
        parser_download.add_argument("pulsar", type=str, help="Name of the pulsar [str]")
        parser_download.add_argument("--id", type=int, help="id of the toa [int]")
        parser_download.add_argument("--pipeline_run_id", type=int, help="pipeline_run_id of the toa [int]")
        parser_download.add_argument("--dm_corrected",  action="store_true", help="Return TOAs that have had their DM corrected for each observation [bool]")
        parser_download.add_argument("--minimum_nsubs", action="store_true", help="Only use TOAs with the minimum number of subints per observation (1) [bool]")
        parser_download.add_argument("--maximum_nsubs", action="store_true", help="Only use TOAs with the maximum number of subints per observation (can be 1 but is often more) [bool]")
        parser_download.add_argument("--nchan", type=int, help="Only use TOAs with this many subchans (common values are 1,4 and 16) [int]")


if __name__ == "__main__":
    parser = Toa.get_parsers()
    args = parser.parse_args()

    from psrdb.graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    t = Toa(client, args.url, args.token)
    t.process(args)
