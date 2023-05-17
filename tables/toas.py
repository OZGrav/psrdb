from tables.graphql_table import GraphQLTable
from tables.graphql_query import graphql_query_factory


class Toas(GraphQLTable):
    def __init__(self, client, url, token):
        GraphQLTable.__init__(self, client, url, token)
        self.record_name = "toa"

        # create a new record
        self.create_mutation = """
        mutation ($processing: Int!, $inputFolding: Int!, $timingEphemeris: Int!, $template: Int!, $flags: JSONString!, $frequency: Float!, $mjd: String!, $site: String!, $uncertainty: Float!, $quality: String!, $comment: String!) {
            createToa (input: {
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
        self.create_variables = {
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
        return self.create_graphql()

    def process(self, args):
        """Parse the arguments collected by the CLI."""
        self.print_stdout = True
        if args.subcommand == "create":
            return self.create(
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
        return "toas"

    @classmethod
    def get_description(cls):
        return "A pulsar toa/standard"

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Toas model parser")
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
        parser_create = subs.add_parser("create", help="create a new toa")
        parser_create.add_argument(
            "processing", metavar="PROC", type=int, help="id of the processing to which this toa applies [int]"
        )
        parser_create.add_argument(
            "folding", metavar="FOLD", type=int, help="id of the folding which is input to this toa [int]"
        )
        parser_create.add_argument(
            "ephemeris", metavar="EPH", type=int, help="id of the timing ephemeris used in this this toa [int]"
        )
        parser_create.add_argument(
            "template", metavar="TEMPL", type=int, help="id of the standard/template used in this this toa [int]"
        )
        parser_create.add_argument("flags", metavar="FLAGS", type=str, help="flags used in this toa [str]")
        parser_create.add_argument(
            "frequency", metavar="FREQ", type=float, help="frequency of this toa in MHz [float]]"
        )
        parser_create.add_argument(
            "mjd", metavar="MJD", type=str, help="modified julian data for this toa in days [str]"
        )
        parser_create.add_argument("site", metavar="SITE", type=str, help="site of code of this toa [str[1]]")
        parser_create.add_argument("uncertainty", metavar="ERR", type=float, help="uncertainty of this toa [float]")
        parser_create.add_argument("quality", metavar="QUAL", type=str, help="quality of this toa [nominal, bad]")
        parser_create.add_argument("comment", metavar="COMMENT", type=str, help="comment about the toa [str]")

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
    parser = Toas.get_parsers()
    args = parser.parse_args()

    GraphQLTable.configure_logging(args)

    from graphql_client import GraphQLClient

    client = GraphQLClient(args.url, args.very_verbose)

    t = Toas(client, args.url, args.token)
    t.process(args)
