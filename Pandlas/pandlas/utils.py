import logging
from typing import Union

import numpy as np
import pandas as pd


def timestamp2long(timestamp: Union[pd.Timestamp, pd.DatetimeIndex], start_date: pd.Timestamp = None) -> Union[
    pd.Index, int]:
    """Convert timestamps to ns from the midnight of start date.

    Args:
        timestamp: List of timestamps to be converted.
        start_date: The date to count from. If no date is passed in then it will take the start of the timestamp as the
        first day. The timestamp will be rounded down to the nearest day and the ns outputted will be counted from midnight.

    Returns:
        List of floats representing ns passed since midnight of start date.

    Raises:
        OverflowError: If the output floats are larger than a C# long can handle
    """

    if start_date is None:
        if isinstance(timestamp, pd.Timestamp):
            start_date = timestamp
        elif isinstance(timestamp, pd.DatetimeIndex):
            start_date = timestamp[0]
        else:
            logging.warning('Incorrect input type for timestamp at timestamp2long')
            try:
                start_date = timestamp[0]
            except IndexError:
                start_date = timestamp
    ddays = (timestamp - start_date.floor('D')).days
    ns_in_day = (timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second) * 1e9 + timestamp.microsecond * 1e3 + timestamp.nanosecond
    long = ns_in_day + ddays * 24 * 3600 * 1e9

    if isinstance(timestamp, pd.Timestamp):
        long = np.int64(long)
    else:
        long = long.astype(np.int64)

    if np.array(long > 2 ** 63).max():
        logging.error("Timestamp is too large to be represented by long.")
        raise OverflowError("Timestamp is too large to be represented by long")
    return long
