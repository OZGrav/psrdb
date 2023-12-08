from datetime import datetime

from psrdb.graphql_table import GraphQLTable
from psrdb.utils.other import decode_id


def get_parsers():
    """Returns the default parser for this model"""
    parser = GraphQLTable.get_default_parser("The following options will allow you to interact with the Observation database object on the command line in different ways based on the sub-commands.")
    Observation.configure_parsers(parser)
    return parser


class Observation(GraphQLTable):
    """Class for interacting with the Observation database object.

    Parameters
    ----------
    client : GraphQLClient
        GraphQLClient class instance with the URL and Token already set.
    """
    def __init__(self, client, logger=None):
        GraphQLTable.__init__(self, client, logger)
        self.table_name = "observation"
        self.field_names = [
            "id",
            "pulsar { name }",
            "calibration { id }",
            "calibration { location }",
            "telescope { name }",
            "project { code }",
            "project { short }",
            "utcStart",
            "beam",
            "band",
            "duration",
        ]

    def list(
        self,
        id=None,
        pulsar_name=None,
        telescope_name=None,
        project_id=None,
        project_short=None,
        utcs=None,
        utce=None,
        obs_type='fold',
        unprocessed=None,
        incomplete=None,
    ):
        """Return a list of Observation information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        id : int, optional
            Filter by the database ID, by default None
        pulsar_name : str, optional
            Filter by the pulsar name, by default None
        telescope_name : str, optional
            Filter by the telescope name, by default None
        project_id : int, optional
            Filter by the project id, by default None
        project_short : str, optional
            Filter by the project short name, by default None
        utcs : str, optional
            Filter by the utc start time greater than or equal to the timestamp in the format YYYY-MM-DDTHH:MM:SS+00:00, by default None
        utce : str, optional
            Filter by the utc start time less than or equal to the timestamp in the format YYYY-MM-DDTHH:MM:SS+00:00, by default None
        obs_type : str, optional
            Filter by the observation type (fold, search or cal), by default 'fold'
        unprocessed : str, optional
            Filter to only returned unprocessed observations (no PulsarFoldResult)
        incomplete : str, optional
            Filter to only return incomplete observations (most recent job run is not "Completed)

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        # Convert dates to correct format
        if utcs == "":
            utcs = None
        elif utcs is not None:
            d = datetime.strptime(utcs, '%Y-%m-%d-%H:%M:%S')
            utcs = f"{d.date()}T{d.time()}+00:00"
        if utce == "":
            utce = None
        elif utce is not None:
            d = datetime.strptime(utce, '%Y-%m-%d-%H:%M:%S')
            utce = f"{d.date()}T{d.time()}+00:00"
        if project_short == "":
            project_short = None
        if pulsar_name == "":
            pulsar_name = None
        """Return a list of records matching the id and/or any of the arguments."""
        filters = [
            {"field": "id", "value": id},
            {"field": "pulsar_Name", "value": pulsar_name},
            {"field": "telescope_Name", "value": telescope_name},
            {"field": "project_Id", "value": project_id},
            {"field": "project_Short", "value": project_short},
            {"field": "utcStartGte", "value": utcs},
            {"field": "utcStartLte", "value": utce},
            {"field": "obsType", "value": obs_type},
        ]
        if unprocessed is not None:
            filters.append({"field": "unprocessed", "value": unprocessed})
        if incomplete is not None:
            filters.append({"field": "incomplete", "value": incomplete})
        return GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

    def download(
        self,
        id=None,
        pulsar_name=None,
        telescope_name=None,
        project_id=None,
        project_short=None,
        utcs=None,
        utce=None,
        obs_type='fold',
        unprocessed=None,
        incomplete=None,
    ):
        """Return a list of Observation information based on the `self.field_names` and filtered by the parameters.

        Parameters
        ----------
        id : int, optional
            Filter by the database ID, by default None
        pulsar_name : str, optional
            Filter by the pulsar name, by default None
        telescope_name : str, optional
            Filter by the telescope name, by default None
        project_id : int, optional
            Filter by the project id, by default None
        project_short : str, optional
            Filter by the project short name, by default None
        utcs : str, optional
            Filter by the utc start time greater than or equal to the timestamp in the format YYYY-MM-DDTHH:MM:SS+00:00, by default None
        utce : str, optional
            Filter by the utc start time less than or equal to the timestamp in the format YYYY-MM-DDTHH:MM:SS+00:00, by default None
        obs_type : str, optional
            Filter by the observation type (fold, search or cal), by default 'fold'
        unprocessed : str, optional
            Filter to only returned unprocessed observations (no PulsarFoldResult)
        incomplete : str, optional
            Filter to only return incomplete observations (most recent job run is not "Completed)

        Returns
        -------
        list of dicts
            If `self.get_dicts` is `True`, a list of dictionaries containing the results.
        client_response:
            Else a client response object.
        """
        # Convert dates to correct format
        if utcs == "":
            utcs = None
        elif utcs is not None:
            d = datetime.strptime(utcs, '%Y-%m-%d-%H:%M:%S')
            utcs = f"{d.date()}T{d.time()}+00:00"
        if utce == "":
            utce = None
        elif utce is not None:
            d = datetime.strptime(utce, '%Y-%m-%d-%H:%M:%S')
            utce = f"{d.date()}T{d.time()}+00:00"
        if project_short == "":
            project_short = None
        if pulsar_name == "":
            pulsar_name = None
        """Return a list of records matching the id and/or any of the arguments."""
        filters = [
            {"field": "id", "value": id},
            {"field": "pulsar_Name", "value": pulsar_name},
            {"field": "telescope_Name", "value": telescope_name},
            {"field": "project_Id", "value": project_id},
            {"field": "project_Short", "value": project_short},
            {"field": "utcStartGte", "value": utcs},
            {"field": "utcStartLte", "value": utce},
            {"field": "obsType", "value": obs_type},
        ]
        if unprocessed is not None:
            filters.append({"field": "unprocessed", "value": unprocessed})
        if incomplete is not None:
            filters.append({"field": "incomplete", "value": incomplete})

        self.get_dicts = True
        observations_dicts = GraphQLTable.list_graphql(self, self.table_name, filters, [], self.field_names)

        # Create the output name
        output_name = "observations"
        if pulsar_name:
            output_name += f"_{'_'.join(pulsar_name)}"
        if telescope_name:
            output_name += f"_{telescope_name}"
        if project_id:
            output_name += f"_{project_id}"
        if project_short:
            output_name += f"_{project_short}"
        if utcs:
            output_name += f"_utcs{utcs}"
        if utce:
            output_name += f"_utce{utce}"
        if obs_type:
            output_name += f"_{obs_type}"
        if unprocessed:
            output_name += "_unprocessed"
        if incomplete:
            output_name += "_incomplete"
        output_name += ".csv"

        # Loop over the pulsar_fold_results and dump them as a file
        with open(output_name, "w") as f:
            f.write("Obs ID,Pulsar Jname,UTC Start,Project Short Name,Beam #,Observing Band,Duration (s),Calibration Location\n")
            for observations_dict in observations_dicts:
                data_line = [
                    str(decode_id(observations_dict["id"])),
                    str(observations_dict["pulsar"]["name"]),
                    str(datetime.strptime(observations_dict['utcStart'], '%Y-%m-%dT%H:%M:%S+00:00').strftime('%Y-%m-%d-%H:%M:%S')),
                    str(observations_dict["project"]["short"]),
                    str(observations_dict["beam"]),
                    str(observations_dict["band"]),
                    str(observations_dict["duration"]),
                    str(observations_dict["calibration"]["location"]),
                ]
                f.write(f"{','.join(data_line)}\n")
        return output_name

    def create(
        self,
        pulsarName,
        telescopeName,
        projectCode,
        calibrationId,
        ephemerisText,
        frequency,
        bandwidth,
        nchan,
        beam,
        nant,
        nantEff,
        npol,
        obsType,
        utcStart,
        raj,
        decj,
        duration,
        nbit,
        tsamp,
        foldNbin,
        foldNchan,
        foldTsubint,
        filterbankNbit,
        filterbankNpol,
        filterbankNchan,
        filterbankTsamp,
        filterbankDm,
    ):
        """Create a new Observation database object.

        Parameters
        ----------
        pulsarName : str
            The pulsar name.
        telescopeName : str
            The telescope name.
        projectCode : str
            The project code.
        calibrationId : int
            The ID of the Calibration database object.
        ephemerisText : str
            The ephemeris text as a single string (includes new line characters).
        frequency : float
            The frequency of the observation in MHz.
        bandwidth : float
            The bandwidth of the observation in MHz.
        nchan : int
            The number of frequency channels.
        beam : int
            The beam number.
        nant : int
            The number of antennas used in the observation.
        nantEff : int
            The effective number of antennas used in the observation.
        npol : int
            The number of polarisations.
        obsType : str
            The type of observation (fold, search or cal).
        utcStart : `datetime`
            The UTC start time of the observation as a `datetime` object.
        raj : str
            The right ascension of the observation in HH:MM:SS.SS format.
        decj : str
            The declination of the observation in DD:MM:SS.SS format.
        duration : float
            The duration of the observation in seconds.
        nbit : int
            The number of bits per sample.
        tsamp : float
            The sampling time in microseconds.
        foldNbin : int
            The number of bins in the folded data (None for non fold observations).
        foldNchan : int
            The number of frequency channels in the folded data (None for non fold observations).
        foldTsubint : int
            The number of time samples in each sub-integration of the folded data (None for non fold observations).
        filterbankNbit : int
            The number of bits per sample in the filterbank data (None for non search observations).
        filterbankNpol : int
            The number of polarisations in the filterbank data (None for non search observations).
        filterbankNchan : int
            The number of frequency channels in the filterbank data (None for non search observations).
        filterbankTsamp : float
            The sampling time in microseconds in the filterbank data (None for non search observations).
        filterbankDm : float
            The dispersion measure in the filterbank data (None for non search observations).

        Returns
        -------
        client_response:
            A client response object.
        """
        # create a new record
        self.mutation_name = "createObservation"
        self.mutation = """
        mutation (
            $pulsarName: String!,
            $telescopeName: String!,
            $projectCode: String!,
            $calibrationId: Int!,
            $frequency: Float!,
            $bandwidth: Float!,
            $nchan: Int!,
            $beam: Int!,
            $nant: Int!,
            $nantEff: Int!,
            $npol: Int!,
            $obsType: String!,
            $utcStart: DateTime!,
            $raj: String!,
            $decj: String!,
            $duration: Float!,
            $nbit: Int!,
            $tsamp: Float!,
            # Fold options
            $ephemerisText: String,
            $foldNbin: Int,
            $foldNchan: Int,
            $foldTsubint: Int,
            # Search options
            $filterbankNbit: Int,
            $filterbankNpol: Int,
            $filterbankNchan: Int,
            $filterbankTsamp: Float,
            $filterbankDm: Float,
        ) {
            createObservation(input: {
                pulsarName: $pulsarName,
                telescopeName: $telescopeName,
                projectCode: $projectCode,
                calibrationId: $calibrationId,
                frequency: $frequency,
                bandwidth: $bandwidth,
                nchan: $nchan,
                beam: $beam,
                nant: $nant,
                nantEff: $nantEff,
                npol: $npol,
                obsType: $obsType,
                utcStart: $utcStart,
                raj: $raj,
                decj: $decj,
                duration: $duration,
                nbit: $nbit,
                tsamp: $tsamp,
                ephemerisText: $ephemerisText,
                foldNbin: $foldNbin,
                foldNchan: $foldNchan,
                foldTsubint: $foldTsubint,
                filterbankNbit: $filterbankNbit,
                filterbankNpol: $filterbankNpol,
                filterbankNchan: $filterbankNchan,
                filterbankTsamp: $filterbankTsamp,
                filterbankDm: $filterbankDm,
            }) {
                observation {
                    id
                }
            }
        }
        """
        self.variables = {
            "pulsarName": pulsarName,
            "telescopeName": telescopeName,
            "projectCode": projectCode,
            "calibrationId": calibrationId,
            "ephemerisText": ephemerisText,
            "frequency": frequency,
            "bandwidth": bandwidth,
            "nchan": nchan,
            "beam": beam,
            "nant": nant,
            "nantEff": nantEff,
            "npol": npol,
            "obsType": obsType,
            "utcStart": utcStart,
            "raj": raj,
            "decj": decj,
            "duration": duration,
            "nbit": nbit,
            "tsamp": tsamp,
            "foldNbin": foldNbin,
            "foldNchan": foldNchan,
            "foldTsubint": foldTsubint,
            "filterbankNbit": filterbankNbit,
            "filterbankNpol": filterbankNpol,
            "filterbankNchan": filterbankNchan,
            "filterbankTsamp": filterbankTsamp,
            "filterbankDm": filterbankDm,
        }
        return self.mutation_graphql()

    def update(
        self,
        id,
        pulsarName,
        telescopeName,
        projectCode,
        calibrationId,
        ephemerisText,
        frequency,
        bandwidth,
        nchan,
        beam,
        nant,
        nantEff,
        npol,
        obsType,
        utcStart,
        raj,
        decj,
        duration,
        nbit,
        tsamp,
        foldNbin,
        foldNchan,
        foldTsubint,
        filterbankNbit,
        filterbankNpol,
        filterbankNchan,
        filterbankTsamp,
        filterbankDm,
    ):
        """Update a Observation database object.

        Parameters
        ----------
        id : int
            The database ID
        pulsarName : str
            The pulsar name.
        telescopeName : str
            The telescope name.
        projectCode : str
            The project code.
        calibrationId : int
            The ID of the Calibration database object.
        ephemerisText : str
            The ephemeris text as a single string (includes new line characters).
        frequency : float
            The frequency of the observation in MHz.
        bandwidth : float
            The bandwidth of the observation in MHz.
        nchan : int
            The number of frequency channels.
        beam : int
            The beam number.
        nant : int
            The number of antennas used in the observation.
        nantEff : int
            The effective number of antennas used in the observation.
        npol : int
            The number of polarisations.
        obsType : str
            The type of observation (fold, search or cal).
        utcStart : `datetime`
            The UTC start time of the observation as a `datetime` object.
        raj : str
            The right ascension of the observation in HH:MM:SS.SS format.
        decj : str
            The declination of the observation in DD:MM:SS.SS format.
        duration : float
            The duration of the observation in seconds.
        nbit : int
            The number of bits per sample.
        tsamp : float
            The sampling time in microseconds.
        foldNbin : int
            The number of bins in the folded data (None for non fold observations).
        foldNchan : int
            The number of frequency channels in the folded data (None for non fold observations).
        foldTsubint : int
            The number of time samples in each sub-integration of the folded data (None for non fold observations).
        filterbankNbit : int
            The number of bits per sample in the filterbank data (None for non search observations).
        filterbankNpol : int
            The number of polarisations in the filterbank data (None for non search observations).
        filterbankNchan : int
            The number of frequency channels in the filterbank data (None for non search observations).
        filterbankTsamp : float
            The sampling time in microseconds in the filterbank data (None for non search observations).
        filterbankDm : float
            The dispersion measure in the filterbank data (None for non search observations).


        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "updateObservation"
        self.mutation = """
        mutation (
            $id: Int!,
            $pulsarName: String!,
            $telescopeName: String!,
            $projectCode: String!,
            $calibrationId: Int!,
            $frequency: Float!,
            $bandwidth: Float!,
            $nchan: Int!,
            $beam: Int!,
            $nant: Int!,
            $nantEff: Int!,
            $npol: Int!,
            $obsType: String!,
            $utcStart: DateTime!,
            $raj: String!,
            $decj: String!,
            $duration: Float!,
            $nbit: Int!,
            $tsamp: Float!,
            # Fold options
            $ephemerisText: String,
            $foldNbin: Int,
            $foldNchan: Int,
            $foldTsubint: Int,
            # Search options
            $filterbankNbit: Int,
            $filterbankNpol: Int,
            $filterbankNchan: Int,
            $filterbankTsamp: Float,
            $filterbankDm: Float,
        ) {
            updateObservation(input: {
                id: $id,
                pulsarName: $pulsarName,
                telescopeName: $telescopeName,
                projectCode: $projectCode,
                calibrationId: $calibrationId,
                frequency: $frequency,
                bandwidth: $bandwidth,
                nchan: $nchan,
                beam: $beam,
                nant: $nant,
                nantEff: $nantEff,
                npol: $npol,
                obsType: $obsType,
                utcStart: $utcStart,
                raj: $raj,
                decj: $decj,
                duration: $duration,
                nbit: $nbit,
                tsamp: $tsamp,
                ephemerisText: $ephemerisText,
                foldNbin: $foldNbin,
                foldNchan: $foldNchan,
                foldTsubint: $foldTsubint,
                filterbankNbit: $filterbankNbit,
                filterbankNpol: $filterbankNpol,
                filterbankNchan: $filterbankNchan,
                filterbankTsamp: $filterbankTsamp,
                filterbankDm: $filterbankDm,
            }) {
                observation {
                    id
                }
            }
        }
        """
        self.variables = {
            "id": id,
            "pulsarName": pulsarName,
            "telescopeName": telescopeName,
            "projectCode": projectCode,
            "calibrationId": calibrationId,
            "ephemerisText": ephemerisText,
            "frequency": frequency,
            "bandwidth": bandwidth,
            "nchan": nchan,
            "beam": beam,
            "nant": nant,
            "nantEff": nantEff,
            "npol": npol,
            "obsType": obsType,
            "utcStart": utcStart,
            "raj": raj,
            "decj": decj,
            "duration": duration,
            "nbit": nbit,
            "tsamp": tsamp,
            "foldNbin": foldNbin,
            "foldNchan": foldNchan,
            "foldTsubint": foldTsubint,
            "filterbankNbit": filterbankNbit,
            "filterbankNpol": filterbankNpol,
            "filterbankNchan": filterbankNchan,
            "filterbankTsamp": filterbankTsamp,
            "filterbankDm": filterbankDm,
        }
        return self.mutation_graphql()

    def delete(self, id):
        """Delete a Observation database object.

        Parameters
        ----------
        id : int
            The database ID

        Returns
        -------
        client_response:
            A client response object.
        """
        self.mutation_name = "deleteObservation"
        self.mutation = """
        mutation ($id: Int!) {
            deleteObservation(id: $id) {
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
        if args.subcommand == "list":
            return self.list(
                id=args.id,
                pulsar_name=args.pulsar,
                telescope_name=args.telescope_name,
                project_id=args.project_id,
                project_short=args.project_code,
                utcs=args.utcs,
                utce=args.utce,
                obs_type=args.obs_type,
                unprocessed=args.unprocessed,
                incomplete=args.incomplete,
            )
        elif args.subcommand == "download":
            return self.download(
                id=args.id,
                pulsar_name=args.pulsar,
                telescope_name=args.telescope_name,
                project_id=args.project_id,
                project_short=args.project_code,
                utcs=args.utcs,
                utce=args.utce,
                obs_type=args.obs_type,
                unprocessed=args.unprocessed,
                incomplete=args.incomplete,
            )
        else:
            raise RuntimeError(f"{args.subcommand} command is not implemented")

    @classmethod
    def get_name(cls):
        return "observation"

    @classmethod
    def get_description(cls):
        return "Observation details."

    @classmethod
    def get_parsers(cls):
        """Returns the default parser for this model"""
        parser = GraphQLTable.get_default_parser("Observations model parser")
        cls.configure_parsers(parser)
        return parser

    @classmethod
    def configure_parsers(cls, parser):
        """Add sub-parsers for each of the valid commands."""
        # create the parser for the "list" command
        parser.set_defaults(command=cls.get_name())
        subs = parser.add_subparsers(dest="subcommand")
        subs.required = True

        parser_list = subs.add_parser("list", help="list existing observations")
        parser_list.add_argument("--id", metavar="ID", type=int, help="list observations matching the id [int]")
        parser_list.add_argument(
            "--target_id", metavar="TGTID", type=int, help="list observations matching the target (pulsar) id [int]"
        )
        parser_list.add_argument(
            "--pulsar", metavar="TGTNAME", type=str, nargs='+', help="list observations matching the target (pulsar) name [str]"
        )
        parser_list.add_argument(
            "--telescope_id", metavar="TELID", type=int, help="list observations matching the telescope id [int]"
        )
        parser_list.add_argument(
            "--telescope_name", metavar="TELNAME", type=str, help="list observations matching the telescope name [int]"
        )
        parser_list.add_argument(
            "--instrumentconfig_id",
            metavar="ICID",
            type=int,
            help="list observations matching the instrument_config id [int]",
        )
        parser_list.add_argument(
            "--instrumentconfig_name",
            metavar="ICNAME",
            type=str,
            help="list observations matching the instrument_config name [str]",
        )
        parser_list.add_argument(
            "--project_id", metavar="PROJID", type=int, help="list observations matching the project id [id]"
        )
        parser_list.add_argument(
            "--project_code", metavar="PROJCODE", type=str, help="list observations matching the project code [str]"
        )
        parser_list.add_argument(
            "--utcs",
            metavar="UTCGTE",
            type=str,
            help="list observations with utc_start greater than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )
        parser_list.add_argument(
            "--utce",
            metavar="UTCLET",
            type=str,
            help="list observations with utc_start less than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )
        parser_list.add_argument(
            "--obs_type",
            metavar="OBSTYPE",
            type=str,
            help="An observation type from fold, search and cal",
        )
        parser_list.add_argument(
            "--unprocessed",
            action='store_true',
            default=None,
            help="list observations with utc_start less than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )
        parser_list.add_argument(
            "--incomplete",
            action='store_true',
            default=None,
            help="list observations with utc_start less than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )

        parser_download = subs.add_parser("download", help="download a csv with can be input to meerpipe based on the following filters")
        parser_download.add_argument("--id", metavar="ID", type=int, help="list observations matching the id [int]")
        parser_download.add_argument(
            "--target_id", metavar="TGTID", type=int, help="list observations matching the target (pulsar) id [int]"
        )
        parser_download.add_argument(
            "--pulsar", metavar="TGTNAME", type=str, nargs='+', help="list observations matching the target (pulsar) name [str]"
        )
        parser_download.add_argument(
            "--telescope_id", metavar="TELID", type=int, help="list observations matching the telescope id [int]"
        )
        parser_download.add_argument(
            "--telescope_name", metavar="TELNAME", type=str, help="list observations matching the telescope name [int]"
        )
        parser_download.add_argument(
            "--instrumentconfig_id",
            metavar="ICID",
            type=int,
            help="list observations matching the instrument_config id [int]",
        )
        parser_download.add_argument(
            "--instrumentconfig_name",
            metavar="ICNAME",
            type=str,
            help="list observations matching the instrument_config name [str]",
        )
        parser_download.add_argument(
            "--project_id", metavar="PROJID", type=int, help="list observations matching the project id [id]"
        )
        parser_download.add_argument(
            "--project_code", metavar="PROJCODE", type=str, help="list observations matching the project code [str]"
        )
        parser_download.add_argument(
            "--utcs",
            metavar="UTCGTE",
            type=str,
            help="list observations with utc_start greater than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )
        parser_download.add_argument(
            "--utce",
            metavar="UTCLET",
            type=str,
            help="list observations with utc_start less than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )
        parser_download.add_argument(
            "--obs_type",
            metavar="OBSTYPE",
            type=str,
            default="fold",
            help="An observation type from fold, search and cal",
        )
        parser_download.add_argument(
            "--unprocessed",
            action='store_true',
            default=None,
            help="list observations with utc_start less than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )
        parser_download.add_argument(
            "--incomplete",
            action='store_true',
            default=None,
            help="list observations with utc_start less than or equal to the timestamp [YYYY-MM-DDTHH:MM:SS+HH:MM]",
        )


