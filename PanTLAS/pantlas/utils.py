import numpy as np

def timestamp2long(timestamp):
    return np.vectorize(timestamp2long_single)(timestamp)

def timestamp2long_single(timestamp):
    # TODO: doctring
    long = ((timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second) * 1e9 +
                 timestamp.microsecond * 1e3 + timestamp.nanosecond)
    return long