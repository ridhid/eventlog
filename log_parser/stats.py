import functools
import math
from typing import Optional


def percentile(array, percent) -> Optional[int]:
    """
    Find the percentile of a list of values.

    @parameter array - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.

    @return - the percentile of the values
    """
    if not array:
        return None

    k = (len(array) - 1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return array[int(k)]

    d0 = array[int(f)] * (c - k)
    d1 = array[int(c)] * (k - f)

    return int(d0 + d1)


percentil_95 = functools.partial(percentile, percent=0.95)
