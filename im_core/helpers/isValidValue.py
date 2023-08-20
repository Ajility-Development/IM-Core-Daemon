import math


def isValidValue(num):
    try:
        if isinstance(num, bool):
            return True
        elif num is None:
            return False
        elif math.isnan(num):
            return False
        elif math.isinf(num):
            return False
        elif float(num):
            return True
        elif num == 0 or num == 0.0:
            return True
        else:
            return False
    except ValueError:
        return False
    except TypeError:
        return False
