from __future__ import absolute_import
import math


def percentile(values, percent):
    """
    Return percentile out of list of values
    """
    if not values:
        return

    k = (len(values) - 1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return values[int(k)]
    d0 = values[int(f)] * (c - k)
    d1 = values[int(c)] * (k - f)
    return d0 + d1
