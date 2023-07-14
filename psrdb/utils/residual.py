import re
from decimal import Decimal, getcontext


def convert_to_int_or_float_if_possible(value):
    try:
        int_value = int(value)
        return int_value
    except ValueError:
        try:
            float_value = float(value)
            return float_value
        except ValueError:
            return value


def residual_line_to_dict(residual_line):
    """
    Parse a single line from a .residual file.

    Args:
        residual_line (str): A line from a .residual file.

    Returns:
        dict: A dictionary containing the parsed values.
    """
    residual_dict = {}
    residual_args = re.split(r"(?<= )-(?=[a-zA-Z])", residual_line)
    mjd, residual, residual_error, freq_MHz, residual_phase = residual_args[0].split()
    # MJDs are stored as Decimals as standard floats don't have enough precision
    getcontext().prec = 12
    residual_dict["mjd"] = Decimal(mjd)
    residual_dict["residual"] = float(residual)
    residual_dict["residual_error"] = float(residual_error)
    residual_dict["freq_MHz"] = float(freq_MHz)
    residual_dict["residual_phase"] = float(residual_phase)

    for residual_arg in residual_args[1:]:
        arg, value = residual_arg.split()
        value = convert_to_int_or_float_if_possible(value)
        residual_dict[arg] = value

    return residual_dict