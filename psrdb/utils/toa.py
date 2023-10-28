from decimal import Decimal, getcontext

def toa_line_to_dict(toa_line):
    """
    Parse a single line from a .toa file.

    Args:
        toa_line (str): A line from a .toa file.

    Returns:
        dict: A dictionary containing the parsed values.
    """
    toa_dict = {}
    toa_args = toa_line.split(" -")
    archive, freq_MHz, mjd, mjd_err, telescope = toa_args[0].split()
    toa_dict["archive"] = archive
    toa_dict["freq_MHz"] = float(freq_MHz)
    # MJDs are stored as Decimals as standard floats don't have enough precision
    getcontext().prec = 12
    toa_dict["mjd"] = Decimal(mjd)
    toa_dict["mjd_err"] = float(mjd_err)
    toa_dict["telescope"] = telescope

    for toa_arg in toa_args[1:]:
        arg, value = toa_arg.split()
        toa_dict[arg] = value

    return toa_dict

def toa_dict_to_line(toa_dict):
    """
    Convert a dictionary to a line in a .toa file.

    Args:
        toa_dict (dict): A dictionary containing the parsed values.

    Returns:
        str: A line from a .toa file.
    """
    toa_line = ""
    toa_line += f"{toa_dict['archive']} {toa_dict['freq_MHz']:.6f} {toa_dict['mjd']}{toa_dict['mjd_err']:>8.3f}  {toa_dict['telescope']} "
    for key, value in toa_dict.items():
        if key not in ["archive", "freq_MHz", "mjd", "mjd_err", "telescope"]:
            toa_line += f" -{key} {value}"
    return toa_line