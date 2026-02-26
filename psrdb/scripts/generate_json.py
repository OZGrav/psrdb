#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import logging
import numpy as np
from dataclasses import dataclass, asdict
from typing import Optional

logger = logging.getLogger(__name__)
logging.basicConfig(
    encoding="utf-8",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


@dataclass
class ObservationMetadata:
    """Generic observation data class capturing all fields necessary for the
    JSON payload to ingest via psrdb API, with optional fields for fold and
    search mode parameters."""

    # Primary identification
    pulsar_name: str
    telescope_name: str
    project_code: str
    schedule_block_id: str

    # Frequency and channel information
    frequency: float
    bandwidth: float
    nchan: int

    # Observation parameters and type
    nant: int
    npol: int
    nbit: int
    tsamp: float
    obs_type: str
    utc_start: str

    # Telescope pointing position information
    raj: float | str
    decj: float | str
    tied_array_ra: Optional[float | str] = None
    tied_array_dec: Optional[float | str] = None

    # Optional observation parameters
    duration: Optional[float] = None
    nant_eff: Optional[int] = None
    beam: Optional[int] = None

    # Calibration metadata
    cal_type: Optional[str] = None
    cal_location: Optional[str] = None

    # Fold mode parameters
    fold_dm: Optional[float] = None
    fold_nbin: Optional[int] = None
    fold_nchan: Optional[int] = None
    fold_npol: Optional[int] = None
    fold_tsubint: Optional[float] = None

    # Filterbank/Search mode parameters
    filterbank_nbit: Optional[int] = None
    filterbank_npol: Optional[int] = None
    filterbank_nchan: Optional[int] = None
    filterbank_tsamp: Optional[float] = None
    filterbank_dm: Optional[float] = None
    filterbank_tsubint: Optional[float] = None

    # Ephemeris data
    ephemeris_text: Optional[str] = None

    # Required payload mapping fields for ingest into psrdb
    payload_mapping = {
        "pulsar_name": "pulsarName",
        "telescope_name": "telescopeName",
        "project_code": "projectCode",
        "schedule_block_id": "schedule_block_id",
        "cal_type": "cal_type",
        "cal_location": "cal_location",
        "obs_type": "obsType",
        "beam": "beam",
        "utc_start": "utcStart",
        "raj": "raj",
        "decj": "decj",
        "frequency": "frequency",
        "bandwidth": "bandwidth",
        "nchan": "nchan",
        "npol": "npol",
        "nbit": "nbit",
        "tsamp": "tsamp",
        "nant": "nant",
        "nant_eff": "nantEff",
        "duration": "duration",
        "fold_nbin": "foldNbin",
        "fold_nchan": "foldNchan",
        "fold_tsubint": "foldTsubint",
        "filterbank_nbit": "filterbankNbit",
        "filterbank_npol": "filterbankNpol",
        "filterbank_nchan": "filterbankNchan",
        "filterbank_tsamp": "filterbankTsamp",
        "filterbank_dm": "filterbankDm",
        "ephemeris_text": "ephemerisText",
    }

    def to_dict(self) -> dict:
        """Convert to dictionary with camelCase keys for JSON payload.
        Only includes fields defined in the required payload mapping for psrdb.

        Returns:
            Dictionary representation of the observation metadata.
        """
        data = asdict(self)

        # Map snake_case to camelCase for payload compatibility in JSON style
        payload = {}
        for key, value in data.items():
            try:
                camel_key = self.payload_mapping[key]
            except KeyError:
                pass  # Skip fields not in the payload mapping
            else:
                payload[camel_key] = value

        return payload

    def to_json_string(self, **kwargs) -> str:
        """Convert to JSON string with camelCase keys for JSON payload.
        Only includes fields defined in the required payload mapping for psrdb.

        Args:
            **kwargs: Additional arguments to pass to json.dumps()
                     (e.g., indent, sort_keys, etc.)

        Returns:
            JSON string representation of the observation metadata.
        """
        return json.dumps(self.to_dict(), **kwargs)

    def write_json(self, filepath: str, **kwargs) -> None:
        """Write observation metadata to a JSON file with camelCase keys.
        Only includes fields defined in the required payload mapping for psrdb.

        Args:
            filepath: Path to output JSON file.
            **kwargs: Additional arguments to pass to json.dumps()
                     (e.g., indent, sort_keys, etc.)

        Returns:
            None
        """
        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f, indent=1, **kwargs)

    @classmethod
    def from_json(cls, filepath: str) -> "ObservationMetadata":
        """Create an ObservationMetadata instance from a JSON file.

        Args:
            filepath: Path to JSON file containing observation metadata.

        Returns:
            ObservationMetadata instance populated from file data.

        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If the file is not valid JSON.
            KeyError: If required fields are missing from the JSON.
        """
        with open(filepath, "r") as f:
            data = json.load(f)

        # Map camelCase keys back to snake_case for dataclass instantiation
        reverse_payload_mapping = {
            v: k for k, v in cls.payload_mapping.items()
        }

        # Convert camelCase keys to snake_case
        snake_case_data = {}
        for key, value in data.items():
            snake_key = reverse_payload_mapping.get(key, key)
            snake_case_data[snake_key] = value

        return cls(**snake_case_data)

    @classmethod
    def from_text(
        cls,
        filepath: str,
        delimiter: str = "=",
    ) -> "ObservationMetadata":
        """Create an ObservationMetadata instance from a generic text file.

        Parses key-value pairs from a text file where each line contains
        a key and value separated by the specified delimiter.

        Args:
            filepath: Path to text file containing observation metadata.
            delimiter: Character(s) separating key from value (default: "=").
                      Supports "whitespace" to mean any amount of whitespace
                      as the delimiter.

        Returns:
            ObservationMetadata instance populated from file data.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If a line cannot be parsed or required fields are
            missing.
        """
        snake_case_data = {}

        # Map of possible key variations to standardized field names
        key_mapping = {
            "pulsar_name": "pulsar_name",
            "pulsarname": "pulsar_name",
            "pulsar": "pulsar_name",
            "psr": "pulsar_name",
            "psr_name": "pulsar_name",
            "psrname": "pulsar_name",
            "source": "pulsar_name",
            "source_name": "pulsar_name",
            "sourcename": "pulsar_name",
            "telescope_name": "telescope_name",
            "telescopename": "telescope_name",
            "telescope": "telescope_name",
            "project_code": "project_code",
            "projectcode": "project_code",
            "project": "project_code",
            "proposal_id": "project_code",
            "proposalid": "project_code",
            "schedule_block_id": "schedule_block_id",
            "scheduleblockid": "schedule_block_id",
            "sblock_id": "schedule_block_id",
            "block_id": "schedule_block_id",
            "blockid": "schedule_block_id",
            "cal_type": "cal_type",
            "caltype": "cal_type",
            "cal_location": "cal_location",
            "callocation": "cal_location",
            "frequency": "frequency",
            "freq": "frequency",
            "bandwidth": "bandwidth",
            "bw": "bandwidth",
            "nchan": "nchan",
            "n_chan": "nchan",
            "num_channels": "nchan",
            "num_chan": "nchan",
            "beam": "beam",
            "beam_num": "beam",
            "beam_number": "beam",
            "beam_id": "beam",
            "beamid": "beam",
            "nant": "nant",
            "n_ant": "nant",
            "num_antennas": "nant",
            "num_ant": "nant",
            "nant_eff": "nant_eff",
            "nanteff": "nant_eff",
            "nant_effective": "nant_eff",
            "npol": "npol",
            "n_pol": "npol",
            "num_pol": "npol",
            "obs_type": "obs_type",
            "obstype": "obs_type",
            "observation_type": "obs_type",
            "utc_start": "utc_start",
            "utcstart": "utc_start",
            "start_time": "utc_start",
            "raj": "raj",
            "ra": "raj",
            "decj": "decj",
            "dec": "decj",
            "nbit": "nbit",
            "n_bit": "nbit",
            "nbits": "nbit",
            "tsamp": "tsamp",
            "t_samp": "tsamp",
            "sample_time": "tsamp",
            "duration": "duration",
            "duration_s": "duration",
            "tobs": "duration",
            "t_obs": "duration",
            "fold_dm": "fold_dm",
            "folddm": "fold_dm",
            "fold_nbin": "fold_nbin",
            "foldnbin": "fold_nbin",
            "fold_outnbin": "fold_nbin",
            "fold_nchan": "fold_nchan",
            "foldnchan": "fold_nchan",
            "fold_outnchan": "fold_nchan",
            "fold_npol": "fold_npol",
            "foldnpol": "fold_npol",
            "fold_outnpol": "fold_npol",
            "fold_tsubint": "fold_tsubint",
            "foldtsubint": "fold_tsubint",
            "fold_outtsubint": "fold_tsubint",
            "filterbank_nbit": "filterbank_nbit",
            "filterbankbit": "filterbank_nbit",
            "search_nbit": "filterbank_nbit",
            "search_outnbit": "filterbank_nbit",
            "filterbank_npol": "filterbank_npol",
            "filterbankpol": "filterbank_npol",
            "search_npol": "filterbank_npol",
            "search_outnpol": "filterbank_npol",
            "filterbank_nchan": "filterbank_nchan",
            "filterbanknchan": "filterbank_nchan",
            "search_nchan": "filterbank_nchan",
            "search_outnchan": "filterbank_nchan",
            "filterbank_tsamp": "filterbank_tsamp",
            "filterbanktsamp": "filterbank_tsamp",
            "search_tsamp": "filterbank_tsamp",
            "search_outtsamp": "filterbank_tsamp",
            "filterbank_dm": "filterbank_dm",
            "filterbankdm": "filterbank_dm",
            "search_dm": "filterbank_dm",
            "filterbank_tsubint": "filterbank_tsubint",
            "filterbanksubint": "filterbank_tsubint",
            "search_tsubint": "filterbank_tsubint",
            "search_outtsubint": "filterbank_tsubint",
            "ephemeris_text": "ephemeris_text",
            "ephemeristext": "ephemeris_text",
            "ephemeris": "ephemeris_text",
            "antennas": "antenna_list",
            "antenna_list": "antenna_list",
            "tile_list": "antenna_list",
            "tiles": "antenna_list",
            "antenna": "antenna_list",
            "antennae": "antenna_list",
            "antenna_names": "antenna_list",
            "antennanames": "antenna_list",
            "tied_array_ra": "tied_array_ra",
            "tied_beam_ra": "tied_array_ra",
            "tied_array_dec": "tied_array_dec",
            "tied_beam_dec": "tied_array_dec",
        }

        # Track varius subsets separately for special processing after
        # initial parsing of the data header
        pol_weights = {}
        fold_params = {}
        search_params = {}
        fold_mode_enabled = False
        search_mode_enabled = False

        with open(filepath, "r") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith("#"):
                    continue

                # Handle different delimiters
                if delimiter == "whitespace":
                    parts = line.split()
                else:
                    parts = line.split(delimiter, 1)

                if len(parts) < 2:
                    raise ValueError(
                        f"Line {line_num} could not be parsed: '{line}'. "
                        f"Expected key{delimiter}value format."
                    )

                key, value = parts[0].strip(), parts[1].strip()

                # Track WEIGHTS_POL* keys separately for later processing
                if key.upper().startswith("WEIGHTS_POL"):
                    pol_weights[key.upper()] = value
                    continue

                if "obs_type" not in snake_case_data:
                    # Check for fold mode enabling keywords
                    # (only if obs_type not explicitly set)
                    if key.upper() in ["PERFORM_FOLD", "FOLD_MODE"]:
                        # Parse fold mode indicator (e.g., "true", "1")
                        fold_mode_enabled = str(value).lower() in [
                            "true",
                            "1",
                            "yes",
                            "enabled",
                            "on",
                        ]
                        logger.debug(
                            "Fold mode %s based on %s=%s "
                            "(obs_type not explicitly set)",
                            "enabled" if fold_mode_enabled else "disabled",
                            key.upper(),
                            value,
                        )
                        if fold_mode_enabled:
                            snake_case_data["obs_type"] = "fold"
                            logger.info(
                                "Parsed key '%s=%s' and setting '%s'=%s",
                                key,
                                str(value),
                                "obs_type",
                                snake_case_data["obs_type"],
                            )
                        continue

                    # Check for search mode enabling keywords
                    # (only if obs_type not explicitly set)
                    if key.upper() in ["PERFORM_SEARCH", "SEARCH_MODE"]:
                        # Parse search mode indicator (e.g., "true", "1")
                        search_mode_enabled = str(value).lower() in [
                            "true",
                            "1",
                            "yes",
                            "enabled",
                            "on",
                        ]
                        logger.debug(
                            "Search mode %s based on %s=%s "
                            "(obs_type not explicitly set)",
                            "enabled" if search_mode_enabled else "disabled",
                            key.upper(),
                            value,
                        )
                        if search_mode_enabled:
                            snake_case_data["obs_type"] = "search"
                            logger.info(
                                "Parsed key '%s=%s' and setting '%s'=%s",
                                key,
                                str(value),
                                "obs_type",
                                snake_case_data["obs_type"],
                            )
                        continue

                # Track fold parameters (regardless of mode flag order)
                if key.upper() in [
                    "FOLD_DM",
                    "FOLD_NBIN",
                    "FOLD_OUTNBIN",
                    "FOLD_NCHAN",
                    "FOLD_OUTNCHAN",
                    "FOLD_NPOL",
                    "FOLD_OUTNPOL",
                    "FOLD_TSUBINT",
                    "FOLD_OUTTSUBINT",
                ]:
                    fold_params[key.upper()] = value
                    continue

                # Ditto for search parameters
                if key.upper() in [
                    "SEARCH_NBIT",
                    "SEARCH_OUTNBIT",
                    "SEARCH_NPOL",
                    "SEARCH_OUTNPOL",
                    "SEARCH_NCHAN",
                    "SEARCH_OUTNCHAN",
                    "SEARCH_TSAMP",
                    "SEARCH_OUTTSAMP",
                    "SEARCH_DM",
                    "SEARCH_TSUBINT",
                    "SEARCH_OUTTSUBINT",
                ]:
                    search_params[key.upper()] = value
                    continue

                if key.lower() in key_mapping:
                    standardised_key = key_mapping.get(
                        key.lower(), key.lower()
                    )

                    # Type conversion for known possible numeric fields
                    try:
                        if standardised_key in [
                            "frequency",
                            "bandwidth",
                            "duration",
                            "raj",
                            "decj",
                            "tsamp",
                        ]:
                            value = float(value)
                        elif standardised_key in [
                            "nchan",
                            "beam",
                            "nant",
                            "nant_eff",
                            "npol",
                            "nbit",
                        ]:
                            value = int(value)
                    except ValueError:
                        if value not in [None, "", "None"]:
                            logger.warning(
                                "Failed to convert key '%s' value '%s' to "
                                "numeric type. Keeping as string.",
                                key,
                                value,
                            )
                        else:
                            logger.warning(
                                "Value for key '%s' is None or empty. "
                                "Keeping as None.",
                                key,
                            )
                            value = None

                    snake_case_data[standardised_key] = value
                    if standardised_key not in ["antenna_list"]:
                        logger.info(
                            "Parsed key '%s' as '%s' with value: %s",
                            key,
                            standardised_key,
                            value,
                        )
                else:
                    logger.debug(
                        "Line %d: Unknown key '%s' will be skipped.",
                        line_num,
                        key,
                    )
            # END OF PARSING FILE LINES
        # END OF OPEN FILE CONTEXT

        # Set the tied-array beam pointing to the RA/Dec if not
        # explicitly provided
        if "tied_array_ra" not in snake_case_data and "raj" in snake_case_data:
            snake_case_data["tied_array_ra"] = snake_case_data["raj"]
        if (
            "tied_array_dec" not in snake_case_data
            and "decj" in snake_case_data
        ):
            snake_case_data["tied_array_dec"] = snake_case_data["decj"]

        # Derive nant from antenna list if nant is missing
        if "nant" not in snake_case_data and "antenna_list" in snake_case_data:
            antenna_value = snake_case_data.pop("antenna_list")

            # Parse antenna list (comma-separated or space-separated)
            if isinstance(antenna_value, str):
                # Try comma-separated first, then space-separated
                if "," in antenna_value:
                    antennas = [
                        a.strip()
                        for a in antenna_value.split(",")
                        if a.strip()
                    ]
                else:
                    antennas = [
                        a.strip() for a in antenna_value.split() if a.strip()
                    ]

                snake_case_data["nant"] = len(antennas)
                logger.info(
                    "Derived nant=%d from antenna list",
                    len(antennas),
                )
                logger.debug("Antenna list: %s", antennas)

        # Derive nant_eff from WEIGHTS_POL* if nant_eff is missing
        if "nant_eff" not in snake_case_data and pol_weights:
            try:
                all_weights = []

                # Parse each WEIGHTS_POL* key
                for weight_key in sorted(pol_weights.keys()):
                    weight_value = pol_weights[weight_key]

                    # Parse weights array
                    if isinstance(weight_value, str):
                        # Remove brackets if present
                        if "," in weight_value:
                            weights = [
                                float(w.strip())
                                for w in weight_value.split(",")
                                if w.strip()
                            ]
                        else:
                            weights = [
                                float(w.strip())
                                for w in weight_value.split()
                                if w.strip()
                            ]
                        all_weights.append(weights)

                if all_weights:
                    # Convert to numpy array for easier computation
                    weights_array = np.array(all_weights)

                    # Sum along antenna axis (axis 1) and compute mean
                    pol_sums = weights_array.sum(axis=1)
                    nant_eff = int(pol_sums.mean())

                    snake_case_data["nant_eff"] = nant_eff
                    logger.info(
                        "Derived nant_eff=%d from WEIGHTS_POL* keys",
                        nant_eff,
                    )
                    logger.debug(
                        "Poln. Weights array shape: %s, poln. sums: %s",
                        weights_array.shape,
                        pol_sums,
                    )
            except (ValueError, IndexError) as e:
                logger.warning(
                    "Failed to derive nant_eff from WEIGHTS_POL*: %s",
                    str(e),
                )

        # Process fold parameters if fold mode is enabled
        if fold_mode_enabled and fold_params:
            # Mapping of fold parameter keys to standardized field names
            fold_param_mapping = {
                "FOLD_DM": "fold_dm",
                "FOLD_NBIN": "fold_nbin",
                "FOLD_OUTNBIN": "fold_nbin",
                "FOLD_NCHAN": "fold_nchan",
                "FOLD_OUTNCHAN": "fold_nchan",
                "FOLD_NPOL": "fold_npol",
                "FOLD_OUTNPOL": "fold_npol",
                "FOLD_TSUBINT": "fold_tsubint",
                "FOLD_OUTTSUBINT": "fold_tsubint",
            }

            for param_key, param_value in fold_params.items():
                standardised_key = fold_param_mapping.get(param_key)
                if standardised_key:
                    # Type conversion for fold parameters
                    try:
                        if standardised_key in ["fold_dm", "fold_tsubint"]:
                            param_value = float(param_value)
                        else:
                            param_value = int(param_value)
                    except ValueError:
                        logger.warning(
                            "Failed to convert fold param '%s' value '%s' "
                            "to numeric type",
                            param_key,
                            param_value,
                        )

                    snake_case_data[standardised_key] = param_value
                    logger.debug(
                        "Added fold parameter '%s' -> '%s' with value: %s",
                        param_key,
                        standardised_key,
                        param_value,
                    )
        elif fold_params and not fold_mode_enabled:
            logger.debug(
                "Fold parameters found but fold mode not enabled; "
                "skipping: %s",
                list(fold_params.keys()),
            )

        # Process search parameters if search mode is enabled
        if search_mode_enabled and search_params:
            # Mapping of search parameter keys to standardized field names
            search_param_mapping = {
                "SEARCH_NBIT": "filterbank_nbit",
                "SEARCH_OUTNBIT": "filterbank_nbit",
                "SEARCH_NPOL": "filterbank_npol",
                "SEARCH_OUTNPOL": "filterbank_npol",
                "SEARCH_NCHAN": "filterbank_nchan",
                "SEARCH_OUTNCHAN": "filterbank_nchan",
                "SEARCH_TSAMP": "filterbank_tsamp",
                "SEARCH_OUTTSAMP": "filterbank_tsamp",
                "SEARCH_DM": "filterbank_dm",
                "SEARCH_TSUBINT": "filterbank_tsubint",
                "SEARCH_OUTTSUBINT": "filterbank_tsubint",
            }

            for param_key, param_value in search_params.items():
                standardised_key = search_param_mapping.get(param_key)
                if standardised_key:
                    # Type conversion for search parameters
                    try:
                        if standardised_key in [
                            "filterbank_tsamp",
                            "filterbank_dm",
                            "filterbank_tsubint",
                        ]:
                            param_value = float(param_value)
                        else:
                            param_value = int(param_value)
                    except ValueError:
                        logger.warning(
                            "Failed to convert search param '%s' value "
                            "'%s' to numeric type",
                            param_key,
                            param_value,
                        )

                    snake_case_data[standardised_key] = param_value
                    logger.debug(
                        "Added search parameter '%s' -> "
                        "'%s' with value: %s",
                        param_key,
                        standardised_key,
                        param_value,
                    )
        elif search_params and not search_mode_enabled:
            logger.debug(
                "Search parameters found but search mode not enabled; "
                "skipping: %s",
                list(search_params.keys()),
            )

        return cls(**snake_case_data)
