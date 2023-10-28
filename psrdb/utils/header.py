import re
import json


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
        if "DELAYCAL_ID" in self.cfg.keys():
            self.delaycal_id = self.cfg["DELAYCAL_ID"]
        else:
            self.delaycal_id = None
        if "PHASEUP_ID" in self.cfg.keys():
            self.phaseup_id  = self.cfg["PHASEUP_ID"]
        else:
            self.phaseup_id  = None
        if "SCHEDULE_BLOCK_ID" in self.cfg.keys():
            self.schedule_block_id  = self.cfg["SCHEDULE_BLOCK_ID"]
        else:
            self.schedule_block_id  = None

        self.ra  = self.cfg["RA"]
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


class PTUSEHeader(Header):
    def __init__(self, fname):
        Header.__init__(self, fname)

    def parse(self):
        Header.parse(self)

        self.proposal_id = self.get("PROPOSAL_ID")

        self.nant = len(self.get("ANTENNAE").split(","))

        h_weights = self.get("WEIGHTS_POLH").split(",")
        v_weights = self.get("WEIGHTS_POLV").rstrip(",").split(",")
        if self.get("WEIGHTS_POLH") == "Unknown" or self.get("WEIGHTS_POLV") == "Unknown":
            self.nant_eff = self.nant
        else:
            nant_eff_h = 0
            nant_eff_v = 0
            for w in h_weights:
                nant_eff_h += float(w)
            for w in v_weights:
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
            self.fold_npol = int(self.get("FOLD_OUTNPOL"))
            self.fold_nbin = int(self.get("FOLD_OUTNBIN"))
            self.fold_tsubint = int(self.get("FOLD_OUTTSUBINT"))
            if self.source.endswith(("_N", "_S", "_O")):
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