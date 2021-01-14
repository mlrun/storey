from storey.table import ReadOnlyAggregationBuckets
from storey.dtypes import SlidingWindows
from datetime import datetime


def _assert_buckets(window, base_time, initial_data, expected_data):
    aggr_buckets = ReadOnlyAggregationBuckets("test", "count", window, None, base_time, None, initial_data=initial_data)

    actual = [aggr_value.get_value()[1] for aggr_value in aggr_buckets.buckets]
    assert actual == expected_data


def test_load_aggregation_bucket():
    test_base_time = int(datetime.fromisoformat("2020-07-21T21:40:00+00:00").timestamp() * 1000)
    window = SlidingWindows(["1h"], "10m")
    curr_bucket_time = int(test_base_time / window.max_window_millis) * window.max_window_millis
    initial_data = {curr_bucket_time - window.max_window_millis: [1, 0, 0, 0, 1, 2],
                    curr_bucket_time: [1, 0, 2, 1, 1, 0]}
    expected_data = [2, 1, 0, 2, 1, 1]
    _assert_buckets(window, test_base_time, initial_data, expected_data)


def test_load_aggregation_bucket_data_two_stored_buckets_requested_data_newer_than_both():
    test_base_time = int(datetime.fromisoformat("2020-07-21T21:40:00+00:00").timestamp() * 1000)
    window = SlidingWindows(["1h"], "10m")
    curr_bucket_time = int(test_base_time / window.max_window_millis) * window.max_window_millis
    initial_data = {curr_bucket_time - 3 * window.max_window_millis: [1, 0, 0, 0, 1, 2],
                    curr_bucket_time - 2 * window.max_window_millis: [1, 0, 2, 1, 1, 1]}
    expected_data = [0, 0, 0, 0, 0, 0]
    _assert_buckets(window, test_base_time, initial_data, expected_data)


def test_load_aggregation_bucket_data_two_stored_buckets_requested_data_newer():
    test_base_time = int(datetime.fromisoformat("2020-07-21T21:25:00+00:00").timestamp() * 1000)
    window = SlidingWindows(["1h"], "10m")
    curr_bucket_time = int(test_base_time / window.max_window_millis) * window.max_window_millis
    initial_data = {curr_bucket_time - 2 * window.max_window_millis: [1, 0, 0, 0, 1, 2],
                    curr_bucket_time - window.max_window_millis: [1, 0, 2, 2, 1, 1]}
    expected_data = [2, 1, 1, 0, 0, 0]
    _assert_buckets(window, test_base_time, initial_data, expected_data)


def test_load_aggregation_bucket_data_two_stored_buckets_requested_data_older():
    test_base_time = int(datetime.fromisoformat("2020-07-21T21:40:00+00:00").timestamp() * 1000)
    window = SlidingWindows(["1h"], "10m")
    curr_bucket_time = int(test_base_time / window.max_window_millis) * window.max_window_millis
    initial_data = {curr_bucket_time + window.max_window_millis: [1, 0, 0, 0, 1, 2],
                    curr_bucket_time + 2 * window.max_window_millis: [1, 0, 2, 1, 1, 0]}
    expected_data = [0, 0, 0, 0, 0, 0]
    _assert_buckets(window, test_base_time, initial_data, expected_data)


def test_load_aggregation_bucket_one_stored_bucket():
    test_base_time = int(datetime.fromisoformat("2020-07-21T21:40:00+00:00").timestamp() * 1000)
    window = SlidingWindows(["1h"], "10m")
    curr_bucket_time = int(test_base_time / window.max_window_millis) * window.max_window_millis
    initial_data = {curr_bucket_time: [1, 0, 2, 1, 1, 0]}
    expected_data = [0, 1, 0, 2, 1, 1]
    _assert_buckets(window, test_base_time, initial_data, expected_data)


def test_load_aggregation_bucket_one_stored_bucket_requested_data_newer():
    test_base_time = int(datetime.fromisoformat("2020-07-21T21:25:00+00:00").timestamp() * 1000)
    window = SlidingWindows(["1h"], "10m")
    curr_bucket_time = int(test_base_time / window.max_window_millis) * window.max_window_millis
    initial_data = {curr_bucket_time - window.max_window_millis: [1, 0, 2, 1, 1, 3]}
    expected_data = [1, 1, 3, 0, 0, 0]
    _assert_buckets(window, test_base_time, initial_data, expected_data)


def test_load_aggregation_bucket_one_stored_bucket_requested_data_older():
    test_base_time = int(datetime.fromisoformat("2020-07-21T21:40:00+00:00").timestamp() * 1000)
    window = SlidingWindows(["1h"], "10m")
    curr_bucket_time = int(test_base_time / window.max_window_millis) * window.max_window_millis
    initial_data = {curr_bucket_time + window.max_window_millis: [1, 0, 2, 1, 1, 0]}
    expected_data = [0, 0, 0, 0, 0, 0]
    _assert_buckets(window, test_base_time, initial_data, expected_data)
