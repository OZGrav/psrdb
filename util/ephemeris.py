import re
import json
import subprocess


class Ephemeris:
    def __init__(self):
        self.ephem = {}
        self.jname = None
        self.p0 = 0
        self.dm = 0
        self.rm = 0
        self.configured = False

    def load_from_file(self, ephemeris_file):
        with open(ephemeris_file, "r") as file:
            data = file.read()
        self.load_from_string(data)

    def load_from_archive_as_str(self, archive_file):
        proc = subprocess.Popen(["vap", "-E", archive_file], stdout=subprocess.PIPE)
        output = proc.stdout.read()
        if type(output) == bytes:
            output = str(output, "utf-8")
        # first populate from string and parse to set all the attributes but then set to string as uploading via graphql expects a string
        self.load_from_string(output)
        # self.ephem = json.dumps(self.ephem)

    def load_from_json(self, ephemeris_json):
        self.ephem = json.loads(ephemeris_json)
        self.parse()

    def load_from_string(self, ephemeris_string):
        lines = ephemeris_string.split("\n")
        for line in lines:
            line = line.strip()
            line = re.sub("#.*", "", line)
            if line:
                line = re.sub("\s+", " ", line)
                parts = line.split(" ", 2)
                if len(parts) < 2:
                    continue
                if len(parts) == 2:
                    self.set_val(parts[0], parts[1])
                else:
                    self.set_val_err(parts[0], parts[1], parts[2])
        self.parse()

    def parse(self):
        self.jname = self.get_val("PSRJ")
        _f0 = self.get_val("F0")
        _dm = self.get_val("DM")
        _rm = self.get_val("RM")
        if _f0 is not None:
            self.p0 = float(1.0 / float(_f0))
        if _dm is not None:
            self.dm = float(_dm)
        if _rm is not None:
            self.rm = float(_rm)
        self.configured = True

    def set_val(self, key, val):
        self.ephem[key] = {"val": val}

    def set_val_err(self, key, val, err):
        self.ephem[key] = {"val": val, "err": err}

    def get(self, key):
        if key in self.ephem:
            return (self.ephem[key].get("val"), self.ephem[key].get("err"))
        else:
            return (None, None)

    def get_val(self, key):
        return self.get(key)[0]
