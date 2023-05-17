from decimal import Decimal


def get_decimal_from_limits(key, val, limits):
    """Return a decimal value based on limits[keys] quantization"""
    try:
        deci_str = Decimal("1.".ljust(limits[key]["deci"] + 2, "0"))
        deci_val = Decimal(val).quantize(deci_str)
    except:
        deci_val = val
    return deci_val
