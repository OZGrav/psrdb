import re
import json


class KeyValueStore:
    def __init__(self, fname):
        self.cfg = {}
        self.read_file(fname)

    def read_file(self, fname):
        fptr = open(fname, "r")
        for line in fptr:
            # remove all comments
            line = line.strip()
            line = re.sub("#.*", "", line)
            if line:
                line = re.sub("\s+", " ", line)
                parts = line.split(" ", 1)
                if len(parts) == 2:
                    self.cfg[parts[0]] = parts[1].strip()
        fptr.close()

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
        self.telescope = self.cfg["TELESCOPE"]

        # Instrument Config
        self.bandwidth = float(self.cfg["BW"])
        self.frequency = float(self.cfg["FREQ"])
        self.nchan = int(self.cfg["NCHAN"])
        self.npol = int(self.cfg["NPOL"])
        self.beam = self.cfg["BEAM"]


class PTUSEHeader(Header):
    def __init__(self, fname):
        Header.__init__(self, fname)

    def parse(self):
        Header.parse(self)

        self.proposal_id = self.get("PROPOSAL_ID")

        self.nant = len(self.get("ANTENNAE").split(","))

        h_weights = self.get("WEIGHTS_POLH").split(",")
        v_weights = self.get("WEIGHTS_POLV").split(",")
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
            self.fold_mode = self.get("MODE")

        if self.get("PERFORM_SEARCH") == "1":
            self.search_nbit = int(self.get("SEARCH_OUTNBIT"))
            self.search_npol = int(self.get("SEARCH_OUTNPOL"))
            self.search_nchan = int(self.get("SEARCH_OUTNCHAN"))
            self.search_tsamp = float(self.get("SEARCH_OUTTSAMP"))
            self.search_dm = float(self.get("SEARCH_DM"))
            self.search_tsubint = float(10)
            try:
                self.search_tsubint = float(self.get("SEARCH_OUTTSUBINT"))
            except:
                self.search_tsubint = float(10)
