import pandas as pd
import pytest
import numpy as np

from pandlas.utils import timestamp2long


class Test_timestamp2long:
    def test_timestamp_input(self):
        time = pd.Timestamp("2021-01-01 10:10:10")
        long = timestamp2long(time)
        assert long == 36610000000000
        time = pd.Timestamp("2021-01-01 23:59:59.9999")
        long = timestamp2long(time)
        assert long == 86399999900000

    def test_datetimeindex_input(self):
        ts = pd.date_range("2021-01-01 10:10:10", "2021-01-01 23:59:59.9999", periods=2)
        long = timestamp2long(ts)
        assert long[0] == 36610000000000
        assert long[1] == 86399999900000

    def test_start_date(self):
        time = pd.Timestamp("2021-01-02 10:10:10")
        long = timestamp2long(time, start_date=pd.Timestamp("2021-01-01"))
        assert long == 36610000000000 + 24 * 3600 * 1e9
        time = pd.Timestamp("2021-01-02 23:59:59.9999")
        long = timestamp2long(time, start_date=pd.Timestamp("2021-01-01"))
        assert long == 86399999900000 + 24 * 3600 * 1e9

    def test_start_date_datetimeindex(self):
        ts = pd.date_range("2021-01-02 10:10:10", "2021-01-02 23:59:59.9999", periods=2)
        long = timestamp2long(ts, start_date=pd.Timestamp("2021-01-01"))
        assert long[0] == 36610000000000 + 24 * 3600 * 1e9
        assert long[1] == 86399999900000 + 24 * 3600 * 1e9
