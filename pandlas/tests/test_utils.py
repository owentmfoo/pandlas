import random
import pandas as pd
import pytest
import numpy as np

from pandlas.utils import timestamp2long, long2timestamp


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

    def test_round_trip(self):
        ts = pd.date_range("2021-01-02 10:10:10", "2021-01-02 23:59:59.9999", periods=2)
        start_date = pd.Timestamp("2021-01-01")
        long = timestamp2long(ts, start_date)
        ts2 = long2timestamp(long.to_numpy(), start_date)
        np.equal(ts, ts2)


class Test_long2timestamp:
    def test_long_input(self):
        long = np.array([36610000000000])
        timestamp = long2timestamp(long, np.datetime64("2021-01-01"))
        assert timestamp == np.datetime64("2021-01-01 10:10:10")

    def test_start_date(self):
        long = np.array([36610000000000]) + 3600 * 24 * int(1e9)
        timestamp = long2timestamp(long, np.datetime64("2021-01-01"))
        assert timestamp == np.datetime64("2021-01-02 10:10:10")

    def test_round_trip(self):
        long = np.array([36610000000000, 36710000000000])
        start_date = pd.Timestamp("2021-01-01")
        ts = long2timestamp(long, start_date)
        long2 = timestamp2long(ts, start_date)
        np.equal(long, long2)

    @pytest.mark.parametrize(
        "epoch", [random.randint(0, 2234860605000) for _ in range(5)]
    )
    def test_random_dates(self, epoch):
        ts = pd.Timestamp(np.datetime64(epoch, "ms"))
        long = timestamp2long(ts, ts)
        ts2 = long2timestamp(long, ts)
        print(ts, ts2)
        assert ts == ts2

    @pytest.mark.parametrize(
        "epoch", [random.randint(0, 2234860605000) for _ in range(5)]
    )
    def test_random_dates_numpy(self, epoch):
        ts = pd.Timestamp(np.datetime64(epoch, "ms"))
        long = timestamp2long(ts, pd.Timestamp(np.datetime64(epoch, "ms")))
        ts2 = long2timestamp(long, np.datetime64(epoch, "ms"))
        print(ts, ts2)
        assert ts == ts2
