import re
import csv
import json

from psrdb.load_data import LBAND_CALIBRATORS, UHFBAND_CALIBRATORS, SBAND_CALIBRATORS, POLARISATION_CALIBRATORS


class KeyValueStore:
    def __init__(self, fname):
        self.cfg = {}
        self.read_file(fname)

    def read_file(self, fname):
        with open(fname, 'r') as header_file:
            for line in header_file:
                # remove all comments
                line = line.strip()
                line = re.sub("#.*", "", line)
                if line:
                    line = re.sub("\s+", " ", line)
                    key, value = line.split(" ", 1)
                    self.cfg[key] = value.strip()

    def set(self, key, value):
        self.cfg[key] = str(value)

    def get(self, key):
        if key in self.cfg.keys():
            return self.cfg[key]
        else:
            return "None"


class Header(KeyValueStore):
    def __init__(self, fname):
        KeyValueStore.__init__(self, fname)

    def parse(self):
        self.source = self.cfg["SOURCE"]
        self.utc_start = self.cfg["UTC_START"]
        self.telescope = self.cfg["TELESCOPE"]
        if "BEAM" in self.cfg.keys():
            self.beam = int(self.cfg["BEAM"])
        else:
            self.beam = None
        if "DELAYCAL_ID" in self.cfg.keys():
            self.delaycal_id = self.cfg["DELAYCAL_ID"]
        else:
            self.delaycal_id = None
        if "PHASEUP_ID" in self.cfg.keys():
            self.phaseup_id = self.cfg["PHASEUP_ID"]
        else:
            self.phaseup_id = None
        if "SCHEDULE_BLOCK_ID" in self.cfg.keys():
            self.schedule_block_id = self.cfg["SCHEDULE_BLOCK_ID"]
        else:
            self.schedule_block_id = None

        self.ra = self.cfg["RA"]
        self.dec = self.cfg["DEC"]
        if "TIED_BEAM_RA" in self.cfg.keys():
            self.tied_beam_ra = self.cfg["TIED_BEAM_RA"]
        else:
            self.tied_beam_ra = self.ra
        if "TIED_BEAM_DEC" in self.cfg.keys():
            self.tied_beam_dec = self.cfg["TIED_BEAM_DEC"]
        else:
            self.tied_beam_dec = self.dec

        # Instrument Config
        self.bandwidth = float(self.cfg["BW"])
        self.frequency = float(self.cfg["FREQ"])
        self.nchan = int(self.cfg["NCHAN"])
        self.npol = int(self.cfg["NPOL"])
        self.nbit = int(self.cfg["NBIT"])
        self.tsamp = float(self.cfg["TSAMP"])

        # Additional numeric fields
        if "ADC_SAMPLE_RATE" in self.cfg:
            self.adc_sample_rate = float(self.cfg["ADC_SAMPLE_RATE"])
        if "ADC_SYNC_TIME" in self.cfg:
            self.adc_sync_time = float(self.cfg["ADC_SYNC_TIME"])
        if "CALFREQ" in self.cfg:
            self.calfreq = float(self.cfg["CALFREQ"])
        if "CAL_FREQ" in self.cfg:
            self.cal_freq = float(self.cfg["CAL_FREQ"])
        if "CAL_PHASE" in self.cfg:
            self.cal_phase = float(self.cfg["CAL_PHASE"])
        if "CAL_DUTY_CYCLE" in self.cfg:
            self.cal_duty_cycle = float(self.cfg["CAL_DUTY_CYCLE"])
        if "BYTES_PER_SECOND" in self.cfg:
            self.bytes_per_second = float(self.cfg["BYTES_PER_SECOND"])
        if "PICOSECONDS" in self.cfg:
            self.picoseconds = float(self.cfg["PICOSECONDS"])
        if "PRECISETIME_FRACTION" in self.cfg:
            self.precisetime_fraction = float(self.cfg["PRECISETIME_FRACTION"])
        if "PRECISETIME_FRACTION_POLH" in self.cfg:
            self.precisetime_fraction_polh = float(self.cfg["PRECISETIME_FRACTION_POLH"])
        if "PRECISETIME_FRACTION_POLV" in self.cfg:
            self.precisetime_fraction_polv = float(self.cfg["PRECISETIME_FRACTION_POLV"])
        if "PRECISETIME_UNCERTAINTY_POLH" in self.cfg:
            self.precisetime_uncertainty_polh = float(self.cfg["PRECISETIME_UNCERTAINTY_POLH"])
        if "PRECISETIME_UNCERTAINTY_POLV" in self.cfg:
            self.precisetime_uncertainty_polv = float(self.cfg["PRECISETIME_UNCERTAINTY_POLV"])
        if "TFR_KTT_GNSS" in self.cfg:
            self.tfr_ktt_gnss = float(self.cfg["TFR_KTT_GNSS"])


class PTUSEHeader(Header):
    def __init__(self, fname):
        Header.__init__(self, fname)

    def parse(self):
        Header.parse(self)

        self.proposal_id = self.get("PROPOSAL_ID")

        self.nant = len(self.get("ANTENNAE").split(","))

        if self.get("WEIGHTS_POLH") == "Unknown" or self.get("WEIGHTS_POLV") == "Unknown":
            self.nant_eff = self.nant
        else:
            h_weights = self.get("WEIGHTS_POLH")
            v_weights = self.get("WEIGHTS_POLV")
            if h_weights == "None" or v_weights == "None":
                # No weights given so return None
                self.nant_eff = None
            else:
                nant_eff_h = 0
                nant_eff_v = 0
                for w in h_weights.split(","):
                    nant_eff_h += float(w)
                for w in v_weights.rstrip(",").split(","):
                    nant_eff_v += float(w)
                self.nant_eff = int((nant_eff_h + nant_eff_v) / 2)
        self.configuration = json.dumps(self.cfg)

        self.machine = "PTUSE"
        self.machine_version = "1.0"
        machine_config = {"machine": "PTUSE", "version": 1.0}
        self.machine_config = json.dumps(machine_config)

        if self.get("PERFORM_FOLD") == "1":
            self.fold_dm = float(self.get("FOLD_DM"))
            self.fold_nchan = int(self.get("FOLD_OUTNCHAN"))
            if self.get("FOLD_OUTNPOL") != "None":
                self.fold_npol = int(self.get("FOLD_OUTNPOL"))
            self.fold_nbin = int(self.get("FOLD_OUTNBIN"))
            self.fold_tsubint = int(self.get("FOLD_OUTTSUBINT"))

            # Get all calibrator names from data files
            calibrator_names = ("J1939-6342", "J0408-6545")# Flux and bandpass calibration https://skaafrica.atlassian.net/wiki/spaces/ESDKB/pages/1481408634/Flux+and+bandpass+calibration
            for cal_file in [LBAND_CALIBRATORS, UHFBAND_CALIBRATORS, SBAND_CALIBRATORS, POLARISATION_CALIBRATORS]:
                with open(cal_file, 'r') as csv_file:
                    csv_reader = csv.reader(csv_file)
                    calibrator_names += tuple(row[0] for row in csv_reader)
            # Ends with are labels for calibrations and starts with are calibrator source names
            if self.source.endswith(("_N", "_S", "_O")) or self.source.endswith(calibrator_names):
                self.obs_type = "cal"
            else:
                self.obs_type = "fold"
        else:
            self.fold_dm = None
            self.fold_nchan = None
            self.fold_npol = None
            self.fold_nbin = None
            self.fold_tsubint = None
            self.fold_mode = None

        if self.get("PERFORM_SEARCH") == "1":
            self.search_nbit = int(self.get("SEARCH_OUTNBIT"))
            self.search_npol = int(self.get("SEARCH_OUTNPOL"))
            self.search_nchan = int(self.get("SEARCH_OUTNCHAN"))
            self.search_tsamp = float(self.get("SEARCH_OUTTSAMP"))
            self.search_dm = float(self.get("SEARCH_DM"))
            try:
                self.search_tsubint = float(self.get("SEARCH_OUTTSUBINT"))
            except:
                self.search_tsubint = float(10)
            self.obs_type = "search"
        else:
            self.search_nbit = None
            self.search_npol = None
            self.search_nchan = None
            self.search_tsamp = None
            self.search_dm = None
            self.search_tsubint = None