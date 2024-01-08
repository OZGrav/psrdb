import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

MOLONGLO_CALIBRATIONS = os.path.join(DATA_DIR, "molonglo_phasing.txt")

# The following are CSVs I manually created from the urls and may need to be updated in the future

# https://skaafrica.atlassian.net/wiki/spaces/ESDKB/pages/1452146701/L-band+gain+calibrators
LBAND_CALIBRATORS = os.path.join(DATA_DIR, "l_band_gain_calibrators.csv")

# https://skaafrica.atlassian.net/wiki/spaces/ESDKB/pages/1479802903/UHF+gain+calibrators
UHFBAND_CALIBRATORS = os.path.join(DATA_DIR, "uhf_band_gain_calibrators.csv")

# https://skaafrica.atlassian.net/wiki/spaces/ESDKB/pages/1589477389/S-band+gain+calibrators
SBAND_CALIBRATORS = os.path.join(DATA_DIR, "s_band_gain_calibrators.csv")

# https://skaafrica.atlassian.net/wiki/spaces/ESDKB/pages/1465548801/Polarisation+calibrators
POLARISATION_CALIBRATORS = os.path.join(DATA_DIR, "polarisation_calibrators.csv")
