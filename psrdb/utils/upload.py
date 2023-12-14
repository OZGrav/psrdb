
import shlex

import subprocess

import psrchive as psr


def generate_obs_length(archive):
    """
    Determine the length of the observation from the input archive file
    """

    ar = psr.Archive_load(archive)
    ar = ar.total()
    return ar.get_first_Integration().get_duration()


def get_archive_ephemeris(freq_summed_archive):
    """
    Get the ephemeris from the archive file using the vap command.
    """
    comm = "vap -E {0}".format(freq_summed_archive)
    args = shlex.split(comm)
    proc = subprocess.Popen(args,stdout=subprocess.PIPE)
    proc.wait()
    ephemeris_text = proc.stdout.read().decode("utf-8")

    if ephemeris_text.startswith('\n'):
        # Remove newline character at start of output
        ephemeris_text = ephemeris_text.lstrip('\n')
    return ephemeris_text