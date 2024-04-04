from decimal import Decimal, getcontext

def format_float(value, threshold=1e3, decimal_places=2):
    if abs(value) >= threshold:
        e_format = "{:.{}e}".format(value, decimal_places)
        return e_format.replace("0e", "e").replace("0e", "e").replace(".e", "e")  # remove trailing zeros
    else:
        return f"{value}"


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
    telescope = toa_dict['telescope']
    if telescope == "meerkat":
        telescope = " meerkat "
    toa_line += f"{toa_dict['archive']} {toa_dict['freq_MHz']:.6f} {toa_dict['mjd']} {toa_dict['mjd_err']:>7.3f} {telescope}"  # noqa: E501
    for key, value in toa_dict.items():
        if key not in ["archive", "freq_MHz", "mjd", "mjd_err", "telescope"]:
            if isinstance(value, float) and key == "gof":
                toa_line += f" -{key} {format_float(value, threshold=1e3, decimal_places=2)}"
            else:
                toa_line += f" -{key} {value}"
    return toa_line