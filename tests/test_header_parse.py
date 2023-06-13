import os

from psrdb.util import header

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data')


def test_get_obs_type():
    tests = [
        (os.path.join(TEST_DATA_DIR, "fold_cal.header"), "cal"),
        (os.path.join(TEST_DATA_DIR, "fold_obs.header"), "fold"),
        # TODO Add a search header test
    ]
    for header_file, obs_type in tests:
        obs_data = header.PTUSEHeader(header_file)
        obs_data.parse()
        assert obs_data.obs_type == obs_type