import math
import queue
from datetime import datetime, timedelta, timezone
import pandas as pd

from storey import build_flow, SyncEmitSource, Reduce, Table, AggregateByKey, FieldAggregator, NoopDriver, \
    DataframeSource
from storey.dtypes import SlidingWindows, FixedWindows, EmitAfterMaxEvent, EmitEveryEvent

test_base_time = datetime.fromisoformat("2020-07-21T21:40:00+00:00")


def append_return(lst, x):
    lst.append(x)
    return lst


def test_sliding_window_simple_aggregation_flow():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0, 'number_of_stuff_sum_24h': 0, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 0, 'number_of_stuff_max_2h': 0,
         'number_of_stuff_max_24h': 0, 'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0, 'number_of_stuff_avg_24h': 0.0},
        {'col1': 1, 'number_of_stuff_sum_1h': 1, 'number_of_stuff_sum_2h': 1, 'number_of_stuff_sum_24h': 1, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 1, 'number_of_stuff_max_2h': 1,
         'number_of_stuff_max_24h': 1, 'number_of_stuff_avg_1h': 0.5, 'number_of_stuff_avg_2h': 0.5, 'number_of_stuff_avg_24h': 0.5},
        {'col1': 2, 'number_of_stuff_sum_1h': 3, 'number_of_stuff_sum_2h': 3, 'number_of_stuff_sum_24h': 3, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 2, 'number_of_stuff_max_2h': 2,
         'number_of_stuff_max_24h': 2, 'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0, 'number_of_stuff_avg_24h': 1.0},
        {'col1': 3, 'number_of_stuff_sum_1h': 6, 'number_of_stuff_sum_2h': 6, 'number_of_stuff_sum_24h': 6, 'number_of_stuff_min_1h': 1,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 3, 'number_of_stuff_max_2h': 3,
         'number_of_stuff_max_24h': 3, 'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 1.5, 'number_of_stuff_avg_24h': 1.5},
        {'col1': 4, 'number_of_stuff_sum_1h': 9, 'number_of_stuff_sum_2h': 10, 'number_of_stuff_sum_24h': 10, 'number_of_stuff_min_1h': 2,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 4, 'number_of_stuff_max_2h': 4,
         'number_of_stuff_max_24h': 4, 'number_of_stuff_avg_1h': 3.0, 'number_of_stuff_avg_2h': 2.0, 'number_of_stuff_avg_24h': 2.0},
        {'col1': 5, 'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 15, 'number_of_stuff_sum_24h': 15, 'number_of_stuff_min_1h': 3,
         'number_of_stuff_min_2h': 1, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 5, 'number_of_stuff_max_2h': 5,
         'number_of_stuff_max_24h': 5, 'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 3.0, 'number_of_stuff_avg_24h': 2.5},
        {'col1': 6, 'number_of_stuff_sum_1h': 15, 'number_of_stuff_sum_2h': 20, 'number_of_stuff_sum_24h': 21, 'number_of_stuff_min_1h': 4,
         'number_of_stuff_min_2h': 2, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 6, 'number_of_stuff_max_2h': 6,
         'number_of_stuff_max_24h': 6, 'number_of_stuff_avg_1h': 5.0, 'number_of_stuff_avg_2h': 4.0, 'number_of_stuff_avg_24h': 3.0},
        {'col1': 7, 'number_of_stuff_sum_1h': 18, 'number_of_stuff_sum_2h': 25, 'number_of_stuff_sum_24h': 28, 'number_of_stuff_min_1h': 5,
         'number_of_stuff_min_2h': 3, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 7, 'number_of_stuff_max_2h': 7,
         'number_of_stuff_max_24h': 7, 'number_of_stuff_avg_1h': 6.0, 'number_of_stuff_avg_2h': 5.0, 'number_of_stuff_avg_24h': 3.5},
        {'col1': 8, 'number_of_stuff_sum_1h': 21, 'number_of_stuff_sum_2h': 30, 'number_of_stuff_sum_24h': 36, 'number_of_stuff_min_1h': 6,
         'number_of_stuff_min_2h': 4, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 8, 'number_of_stuff_max_2h': 8,
         'number_of_stuff_max_24h': 8, 'number_of_stuff_avg_1h': 7.0, 'number_of_stuff_avg_2h': 6.0, 'number_of_stuff_avg_24h': 4.0},
        {'col1': 9, 'number_of_stuff_sum_1h': 24, 'number_of_stuff_sum_2h': 35, 'number_of_stuff_sum_24h': 45, 'number_of_stuff_min_1h': 7,
         'number_of_stuff_min_2h': 5, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 9, 'number_of_stuff_max_2h': 9,
         'number_of_stuff_max_24h': 9, 'number_of_stuff_avg_1h': 8.0, 'number_of_stuff_avg_2h': 7.0, 'number_of_stuff_avg_24h': 4.5}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_sliding_window_sparse_data():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey(
            [FieldAggregator("number_of_stuff1", "col1", ["sum", "avg", "min", "max"], SlidingWindows(['1h', '2h', '24h'], '10m')),
             FieldAggregator("number_of_stuff2", "col2", ["sum", "avg", "min", "max"], SlidingWindows(['1h', '2h', '24h'], '10m'))],
            Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        controller.emit({'col1': i}, 'tal', test_base_time + timedelta(minutes=25 * i))
        controller.emit({'col2': i}, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{'col1': 0, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': math.nan, 'number_of_stuff2_avg_24h': math.nan, 'number_of_stuff2_avg_2h': math.nan,
                         'number_of_stuff2_max_1h': math.nan, 'number_of_stuff2_max_24h': math.nan, 'number_of_stuff2_max_2h': math.nan,
                         'number_of_stuff2_min_1h': math.nan, 'number_of_stuff2_min_24h': math.nan, 'number_of_stuff2_min_2h': math.nan,
                         'number_of_stuff2_sum_1h': 0, 'number_of_stuff2_sum_24h': 0, 'number_of_stuff2_sum_2h': 0},
                        {'col2': 0, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': 0.0, 'number_of_stuff2_avg_24h': 0.0, 'number_of_stuff2_avg_2h': 0.0,
                         'number_of_stuff2_max_1h': 0, 'number_of_stuff2_max_24h': 0, 'number_of_stuff2_max_2h': 0,
                         'number_of_stuff2_min_1h': 0, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 0, 'number_of_stuff2_sum_24h': 0, 'number_of_stuff2_sum_2h': 0},
                        {'col1': 1, 'number_of_stuff1_avg_1h': 0.5, 'number_of_stuff1_avg_24h': 0.5, 'number_of_stuff1_avg_2h': 0.5,
                         'number_of_stuff1_max_1h': 1, 'number_of_stuff1_max_24h': 1, 'number_of_stuff1_max_2h': 1,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 1, 'number_of_stuff1_sum_24h': 1, 'number_of_stuff1_sum_2h': 1,
                         'number_of_stuff2_avg_1h': 0.0, 'number_of_stuff2_avg_24h': 0.0, 'number_of_stuff2_avg_2h': 0.0,
                         'number_of_stuff2_max_1h': 0, 'number_of_stuff2_max_24h': 0, 'number_of_stuff2_max_2h': 0,
                         'number_of_stuff2_min_1h': 0, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 0, 'number_of_stuff2_sum_24h': 0, 'number_of_stuff2_sum_2h': 0},
                        {'col2': 1, 'number_of_stuff1_avg_1h': 0.5, 'number_of_stuff1_avg_24h': 0.5, 'number_of_stuff1_avg_2h': 0.5,
                         'number_of_stuff1_max_1h': 1, 'number_of_stuff1_max_24h': 1, 'number_of_stuff1_max_2h': 1,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 1, 'number_of_stuff1_sum_24h': 1, 'number_of_stuff1_sum_2h': 1,
                         'number_of_stuff2_avg_1h': 0.5, 'number_of_stuff2_avg_24h': 0.5, 'number_of_stuff2_avg_2h': 0.5,
                         'number_of_stuff2_max_1h': 1, 'number_of_stuff2_max_24h': 1, 'number_of_stuff2_max_2h': 1,
                         'number_of_stuff2_min_1h': 0, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 1, 'number_of_stuff2_sum_24h': 1, 'number_of_stuff2_sum_2h': 1},
                        {'col1': 2, 'number_of_stuff1_avg_1h': 1.0, 'number_of_stuff1_avg_24h': 1.0, 'number_of_stuff1_avg_2h': 1.0,
                         'number_of_stuff1_max_1h': 2, 'number_of_stuff1_max_24h': 2, 'number_of_stuff1_max_2h': 2,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 3, 'number_of_stuff1_sum_24h': 3, 'number_of_stuff1_sum_2h': 3,
                         'number_of_stuff2_avg_1h': 0.5, 'number_of_stuff2_avg_24h': 0.5, 'number_of_stuff2_avg_2h': 0.5,
                         'number_of_stuff2_max_1h': 1, 'number_of_stuff2_max_24h': 1, 'number_of_stuff2_max_2h': 1,
                         'number_of_stuff2_min_1h': 0, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 1, 'number_of_stuff2_sum_24h': 1, 'number_of_stuff2_sum_2h': 1},
                        {'col2': 2, 'number_of_stuff1_avg_1h': 1.0, 'number_of_stuff1_avg_24h': 1.0, 'number_of_stuff1_avg_2h': 1.0,
                         'number_of_stuff1_max_1h': 2, 'number_of_stuff1_max_24h': 2, 'number_of_stuff1_max_2h': 2,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 3, 'number_of_stuff1_sum_24h': 3, 'number_of_stuff1_sum_2h': 3,
                         'number_of_stuff2_avg_1h': 1.0, 'number_of_stuff2_avg_24h': 1.0, 'number_of_stuff2_avg_2h': 1.0,
                         'number_of_stuff2_max_1h': 2, 'number_of_stuff2_max_24h': 2, 'number_of_stuff2_max_2h': 2,
                         'number_of_stuff2_min_1h': 0, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 3, 'number_of_stuff2_sum_24h': 3, 'number_of_stuff2_sum_2h': 3},
                        {'col1': 3, 'number_of_stuff1_avg_1h': 2.0, 'number_of_stuff1_avg_24h': 1.5, 'number_of_stuff1_avg_2h': 1.5,
                         'number_of_stuff1_max_1h': 3, 'number_of_stuff1_max_24h': 3, 'number_of_stuff1_max_2h': 3,
                         'number_of_stuff1_min_1h': 1, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 6, 'number_of_stuff1_sum_24h': 6, 'number_of_stuff1_sum_2h': 6,
                         'number_of_stuff2_avg_1h': 1.0, 'number_of_stuff2_avg_24h': 1.0, 'number_of_stuff2_avg_2h': 1.0,
                         'number_of_stuff2_max_1h': 2, 'number_of_stuff2_max_24h': 2, 'number_of_stuff2_max_2h': 2,
                         'number_of_stuff2_min_1h': 0, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 3, 'number_of_stuff2_sum_24h': 3, 'number_of_stuff2_sum_2h': 3},
                        {'col2': 3, 'number_of_stuff1_avg_1h': 2.0, 'number_of_stuff1_avg_24h': 1.5, 'number_of_stuff1_avg_2h': 1.5,
                         'number_of_stuff1_max_1h': 3, 'number_of_stuff1_max_24h': 3, 'number_of_stuff1_max_2h': 3,
                         'number_of_stuff1_min_1h': 1, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 6, 'number_of_stuff1_sum_24h': 6, 'number_of_stuff1_sum_2h': 6,
                         'number_of_stuff2_avg_1h': 2.0, 'number_of_stuff2_avg_24h': 1.5, 'number_of_stuff2_avg_2h': 1.5,
                         'number_of_stuff2_max_1h': 3, 'number_of_stuff2_max_24h': 3, 'number_of_stuff2_max_2h': 3,
                         'number_of_stuff2_min_1h': 1, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 6, 'number_of_stuff2_sum_24h': 6, 'number_of_stuff2_sum_2h': 6},
                        {'col1': 4, 'number_of_stuff1_avg_1h': 3.0, 'number_of_stuff1_avg_24h': 2.0, 'number_of_stuff1_avg_2h': 2.0,
                         'number_of_stuff1_max_1h': 4, 'number_of_stuff1_max_24h': 4, 'number_of_stuff1_max_2h': 4,
                         'number_of_stuff1_min_1h': 2, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 9, 'number_of_stuff1_sum_24h': 10, 'number_of_stuff1_sum_2h': 10,
                         'number_of_stuff2_avg_1h': 2.0, 'number_of_stuff2_avg_24h': 1.5, 'number_of_stuff2_avg_2h': 1.5,
                         'number_of_stuff2_max_1h': 3, 'number_of_stuff2_max_24h': 3, 'number_of_stuff2_max_2h': 3,
                         'number_of_stuff2_min_1h': 1, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 6, 'number_of_stuff2_sum_24h': 6, 'number_of_stuff2_sum_2h': 6},
                        {'col2': 4, 'number_of_stuff1_avg_1h': 3.0, 'number_of_stuff1_avg_24h': 2.0, 'number_of_stuff1_avg_2h': 2.0,
                         'number_of_stuff1_max_1h': 4, 'number_of_stuff1_max_24h': 4, 'number_of_stuff1_max_2h': 4,
                         'number_of_stuff1_min_1h': 2, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 9, 'number_of_stuff1_sum_24h': 10, 'number_of_stuff1_sum_2h': 10,
                         'number_of_stuff2_avg_1h': 3.0, 'number_of_stuff2_avg_24h': 2.0, 'number_of_stuff2_avg_2h': 2.0,
                         'number_of_stuff2_max_1h': 4, 'number_of_stuff2_max_24h': 4, 'number_of_stuff2_max_2h': 4,
                         'number_of_stuff2_min_1h': 2, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 9, 'number_of_stuff2_sum_24h': 10, 'number_of_stuff2_sum_2h': 10},
                        {'col1': 5, 'number_of_stuff1_avg_1h': 4.0, 'number_of_stuff1_avg_24h': 2.5, 'number_of_stuff1_avg_2h': 3.0,
                         'number_of_stuff1_max_1h': 5, 'number_of_stuff1_max_24h': 5, 'number_of_stuff1_max_2h': 5,
                         'number_of_stuff1_min_1h': 3, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 1,
                         'number_of_stuff1_sum_1h': 12, 'number_of_stuff1_sum_24h': 15, 'number_of_stuff1_sum_2h': 15,
                         'number_of_stuff2_avg_1h': 3.0, 'number_of_stuff2_avg_24h': 2.0, 'number_of_stuff2_avg_2h': 2.0,
                         'number_of_stuff2_max_1h': 4, 'number_of_stuff2_max_24h': 4, 'number_of_stuff2_max_2h': 4,
                         'number_of_stuff2_min_1h': 2, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 9, 'number_of_stuff2_sum_24h': 10, 'number_of_stuff2_sum_2h': 10},
                        {'col2': 5, 'number_of_stuff1_avg_1h': 4.0, 'number_of_stuff1_avg_24h': 2.5, 'number_of_stuff1_avg_2h': 3.0,
                         'number_of_stuff1_max_1h': 5, 'number_of_stuff1_max_24h': 5, 'number_of_stuff1_max_2h': 5,
                         'number_of_stuff1_min_1h': 3, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 1,
                         'number_of_stuff1_sum_1h': 12, 'number_of_stuff1_sum_24h': 15, 'number_of_stuff1_sum_2h': 15,
                         'number_of_stuff2_avg_1h': 4.0, 'number_of_stuff2_avg_24h': 2.5, 'number_of_stuff2_avg_2h': 3.0,
                         'number_of_stuff2_max_1h': 5, 'number_of_stuff2_max_24h': 5, 'number_of_stuff2_max_2h': 5,
                         'number_of_stuff2_min_1h': 3, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 1,
                         'number_of_stuff2_sum_1h': 12, 'number_of_stuff2_sum_24h': 15, 'number_of_stuff2_sum_2h': 15},
                        {'col1': 6, 'number_of_stuff1_avg_1h': 5.0, 'number_of_stuff1_avg_24h': 3.0, 'number_of_stuff1_avg_2h': 4.0,
                         'number_of_stuff1_max_1h': 6, 'number_of_stuff1_max_24h': 6, 'number_of_stuff1_max_2h': 6,
                         'number_of_stuff1_min_1h': 4, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 2,
                         'number_of_stuff1_sum_1h': 15, 'number_of_stuff1_sum_24h': 21, 'number_of_stuff1_sum_2h': 20,
                         'number_of_stuff2_avg_1h': 4.0, 'number_of_stuff2_avg_24h': 2.5, 'number_of_stuff2_avg_2h': 3.0,
                         'number_of_stuff2_max_1h': 5, 'number_of_stuff2_max_24h': 5, 'number_of_stuff2_max_2h': 5,
                         'number_of_stuff2_min_1h': 3, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 1,
                         'number_of_stuff2_sum_1h': 12, 'number_of_stuff2_sum_24h': 15, 'number_of_stuff2_sum_2h': 15},
                        {'col2': 6, 'number_of_stuff1_avg_1h': 5.0, 'number_of_stuff1_avg_24h': 3.0, 'number_of_stuff1_avg_2h': 4.0,
                         'number_of_stuff1_max_1h': 6, 'number_of_stuff1_max_24h': 6, 'number_of_stuff1_max_2h': 6,
                         'number_of_stuff1_min_1h': 4, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 2,
                         'number_of_stuff1_sum_1h': 15, 'number_of_stuff1_sum_24h': 21, 'number_of_stuff1_sum_2h': 20,
                         'number_of_stuff2_avg_1h': 5.0, 'number_of_stuff2_avg_24h': 3.0, 'number_of_stuff2_avg_2h': 4.0,
                         'number_of_stuff2_max_1h': 6, 'number_of_stuff2_max_24h': 6, 'number_of_stuff2_max_2h': 6,
                         'number_of_stuff2_min_1h': 4, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 2,
                         'number_of_stuff2_sum_1h': 15, 'number_of_stuff2_sum_24h': 21, 'number_of_stuff2_sum_2h': 20},
                        {'col1': 7, 'number_of_stuff1_avg_1h': 6.0, 'number_of_stuff1_avg_24h': 3.5, 'number_of_stuff1_avg_2h': 5.0,
                         'number_of_stuff1_max_1h': 7, 'number_of_stuff1_max_24h': 7, 'number_of_stuff1_max_2h': 7,
                         'number_of_stuff1_min_1h': 5, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 3,
                         'number_of_stuff1_sum_1h': 18, 'number_of_stuff1_sum_24h': 28, 'number_of_stuff1_sum_2h': 25,
                         'number_of_stuff2_avg_1h': 5.0, 'number_of_stuff2_avg_24h': 3.0, 'number_of_stuff2_avg_2h': 4.0,
                         'number_of_stuff2_max_1h': 6, 'number_of_stuff2_max_24h': 6, 'number_of_stuff2_max_2h': 6,
                         'number_of_stuff2_min_1h': 4, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 2,
                         'number_of_stuff2_sum_1h': 15, 'number_of_stuff2_sum_24h': 21, 'number_of_stuff2_sum_2h': 20},
                        {'col2': 7, 'number_of_stuff1_avg_1h': 6.0, 'number_of_stuff1_avg_24h': 3.5, 'number_of_stuff1_avg_2h': 5.0,
                         'number_of_stuff1_max_1h': 7, 'number_of_stuff1_max_24h': 7, 'number_of_stuff1_max_2h': 7,
                         'number_of_stuff1_min_1h': 5, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 3,
                         'number_of_stuff1_sum_1h': 18, 'number_of_stuff1_sum_24h': 28, 'number_of_stuff1_sum_2h': 25,
                         'number_of_stuff2_avg_1h': 6.0, 'number_of_stuff2_avg_24h': 3.5, 'number_of_stuff2_avg_2h': 5.0,
                         'number_of_stuff2_max_1h': 7, 'number_of_stuff2_max_24h': 7, 'number_of_stuff2_max_2h': 7,
                         'number_of_stuff2_min_1h': 5, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 3,
                         'number_of_stuff2_sum_1h': 18, 'number_of_stuff2_sum_24h': 28, 'number_of_stuff2_sum_2h': 25},
                        {'col1': 8, 'number_of_stuff1_avg_1h': 7.0, 'number_of_stuff1_avg_24h': 4.0, 'number_of_stuff1_avg_2h': 6.0,
                         'number_of_stuff1_max_1h': 8, 'number_of_stuff1_max_24h': 8, 'number_of_stuff1_max_2h': 8,
                         'number_of_stuff1_min_1h': 6, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 4,
                         'number_of_stuff1_sum_1h': 21, 'number_of_stuff1_sum_24h': 36, 'number_of_stuff1_sum_2h': 30,
                         'number_of_stuff2_avg_1h': 6.0, 'number_of_stuff2_avg_24h': 3.5, 'number_of_stuff2_avg_2h': 5.0,
                         'number_of_stuff2_max_1h': 7, 'number_of_stuff2_max_24h': 7, 'number_of_stuff2_max_2h': 7,
                         'number_of_stuff2_min_1h': 5, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 3,
                         'number_of_stuff2_sum_1h': 18, 'number_of_stuff2_sum_24h': 28, 'number_of_stuff2_sum_2h': 25},
                        {'col2': 8, 'number_of_stuff1_avg_1h': 7.0, 'number_of_stuff1_avg_24h': 4.0, 'number_of_stuff1_avg_2h': 6.0,
                         'number_of_stuff1_max_1h': 8, 'number_of_stuff1_max_24h': 8, 'number_of_stuff1_max_2h': 8,
                         'number_of_stuff1_min_1h': 6, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 4,
                         'number_of_stuff1_sum_1h': 21, 'number_of_stuff1_sum_24h': 36, 'number_of_stuff1_sum_2h': 30,
                         'number_of_stuff2_avg_1h': 7.0, 'number_of_stuff2_avg_24h': 4.0, 'number_of_stuff2_avg_2h': 6.0,
                         'number_of_stuff2_max_1h': 8, 'number_of_stuff2_max_24h': 8, 'number_of_stuff2_max_2h': 8,
                         'number_of_stuff2_min_1h': 6, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 4,
                         'number_of_stuff2_sum_1h': 21, 'number_of_stuff2_sum_24h': 36, 'number_of_stuff2_sum_2h': 30},
                        {'col1': 9, 'number_of_stuff1_avg_1h': 8.0, 'number_of_stuff1_avg_24h': 4.5, 'number_of_stuff1_avg_2h': 7.0,
                         'number_of_stuff1_max_1h': 9, 'number_of_stuff1_max_24h': 9, 'number_of_stuff1_max_2h': 9,
                         'number_of_stuff1_min_1h': 7, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 5,
                         'number_of_stuff1_sum_1h': 24, 'number_of_stuff1_sum_24h': 45, 'number_of_stuff1_sum_2h': 35,
                         'number_of_stuff2_avg_1h': 7.0, 'number_of_stuff2_avg_24h': 4.0, 'number_of_stuff2_avg_2h': 6.0,
                         'number_of_stuff2_max_1h': 8, 'number_of_stuff2_max_24h': 8, 'number_of_stuff2_max_2h': 8,
                         'number_of_stuff2_min_1h': 6, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 4,
                         'number_of_stuff2_sum_1h': 21, 'number_of_stuff2_sum_24h': 36, 'number_of_stuff2_sum_2h': 30},
                        {'col2': 9, 'number_of_stuff1_avg_1h': 8.0, 'number_of_stuff1_avg_24h': 4.5, 'number_of_stuff1_avg_2h': 7.0,
                         'number_of_stuff1_max_1h': 9, 'number_of_stuff1_max_24h': 9, 'number_of_stuff1_max_2h': 9,
                         'number_of_stuff1_min_1h': 7, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 5,
                         'number_of_stuff1_sum_1h': 24, 'number_of_stuff1_sum_24h': 45, 'number_of_stuff1_sum_2h': 35,
                         'number_of_stuff2_avg_1h': 8.0, 'number_of_stuff2_avg_24h': 4.5, 'number_of_stuff2_avg_2h': 7.0,
                         'number_of_stuff2_max_1h': 9, 'number_of_stuff2_max_24h': 9, 'number_of_stuff2_max_2h': 9,
                         'number_of_stuff2_min_1h': 7, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 5,
                         'number_of_stuff2_sum_1h': 24, 'number_of_stuff2_sum_24h': 45, 'number_of_stuff2_sum_2h': 35}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_sliding_window_sparse_data_uneven_feature_occurrence():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey(
            [FieldAggregator("number_of_stuff1", "col1", ["sum", "avg", "min", "max"], SlidingWindows(['1h', '2h', '24h'], '10m')),
             FieldAggregator("number_of_stuff2", "col2", ["sum", "avg", "min", "max"], SlidingWindows(['1h', '2h', '24h'], '10m'))],
            Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    controller.emit({'col1': 0}, 'tal', test_base_time)
    for i in range(10):
        controller.emit({'col2': i}, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{'col1': 0, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': math.nan, 'number_of_stuff2_avg_24h': math.nan, 'number_of_stuff2_avg_2h': math.nan,
                         'number_of_stuff2_max_1h': math.nan, 'number_of_stuff2_max_24h': math.nan, 'number_of_stuff2_max_2h': math.nan,
                         'number_of_stuff2_min_1h': math.nan, 'number_of_stuff2_min_24h': math.nan, 'number_of_stuff2_min_2h': math.nan,
                         'number_of_stuff2_sum_1h': 0, 'number_of_stuff2_sum_24h': 0, 'number_of_stuff2_sum_2h': 0},
                        {'col2': 0, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': 0.0, 'number_of_stuff2_avg_24h': 0.0, 'number_of_stuff2_avg_2h': 0.0,
                         'number_of_stuff2_max_1h': 0, 'number_of_stuff2_max_24h': 0, 'number_of_stuff2_max_2h': 0,
                         'number_of_stuff2_min_1h': 0, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 0, 'number_of_stuff2_sum_24h': 0, 'number_of_stuff2_sum_2h': 0},
                        {'col2': 1, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': 0.5, 'number_of_stuff2_avg_24h': 0.5, 'number_of_stuff2_avg_2h': 0.5,
                         'number_of_stuff2_max_1h': 1, 'number_of_stuff2_max_24h': 1, 'number_of_stuff2_max_2h': 1,
                         'number_of_stuff2_min_1h': 0, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 1, 'number_of_stuff2_sum_24h': 1, 'number_of_stuff2_sum_2h': 1},
                        {'col2': 2, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': 1.0, 'number_of_stuff2_avg_24h': 1.0, 'number_of_stuff2_avg_2h': 1.0,
                         'number_of_stuff2_max_1h': 2, 'number_of_stuff2_max_24h': 2, 'number_of_stuff2_max_2h': 2,
                         'number_of_stuff2_min_1h': 0, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 3, 'number_of_stuff2_sum_24h': 3, 'number_of_stuff2_sum_2h': 3},
                        {'col2': 3, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': 2.0, 'number_of_stuff2_avg_24h': 1.5, 'number_of_stuff2_avg_2h': 1.5,
                         'number_of_stuff2_max_1h': 3, 'number_of_stuff2_max_24h': 3, 'number_of_stuff2_max_2h': 3,
                         'number_of_stuff2_min_1h': 1, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 6, 'number_of_stuff2_sum_24h': 6, 'number_of_stuff2_sum_2h': 6},
                        {'col2': 4, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': 3.0, 'number_of_stuff2_avg_24h': 2.0, 'number_of_stuff2_avg_2h': 2.0,
                         'number_of_stuff2_max_1h': 4, 'number_of_stuff2_max_24h': 4, 'number_of_stuff2_max_2h': 4,
                         'number_of_stuff2_min_1h': 2, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 0,
                         'number_of_stuff2_sum_1h': 9, 'number_of_stuff2_sum_24h': 10, 'number_of_stuff2_sum_2h': 10},
                        {'col2': 5, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': 4.0, 'number_of_stuff2_avg_24h': 2.5, 'number_of_stuff2_avg_2h': 3.0,
                         'number_of_stuff2_max_1h': 5, 'number_of_stuff2_max_24h': 5, 'number_of_stuff2_max_2h': 5,
                         'number_of_stuff2_min_1h': 3, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 1,
                         'number_of_stuff2_sum_1h': 12, 'number_of_stuff2_sum_24h': 15, 'number_of_stuff2_sum_2h': 15},
                        {'col2': 6, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': 5.0, 'number_of_stuff2_avg_24h': 3.0, 'number_of_stuff2_avg_2h': 4.0,
                         'number_of_stuff2_max_1h': 6, 'number_of_stuff2_max_24h': 6, 'number_of_stuff2_max_2h': 6,
                         'number_of_stuff2_min_1h': 4, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 2,
                         'number_of_stuff2_sum_1h': 15, 'number_of_stuff2_sum_24h': 21, 'number_of_stuff2_sum_2h': 20},
                        {'col2': 7, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': 6.0, 'number_of_stuff2_avg_24h': 3.5, 'number_of_stuff2_avg_2h': 5.0,
                         'number_of_stuff2_max_1h': 7, 'number_of_stuff2_max_24h': 7, 'number_of_stuff2_max_2h': 7,
                         'number_of_stuff2_min_1h': 5, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 3,
                         'number_of_stuff2_sum_1h': 18, 'number_of_stuff2_sum_24h': 28, 'number_of_stuff2_sum_2h': 25},
                        {'col2': 8, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': 7.0, 'number_of_stuff2_avg_24h': 4.0, 'number_of_stuff2_avg_2h': 6.0,
                         'number_of_stuff2_max_1h': 8, 'number_of_stuff2_max_24h': 8, 'number_of_stuff2_max_2h': 8,
                         'number_of_stuff2_min_1h': 6, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 4,
                         'number_of_stuff2_sum_1h': 21, 'number_of_stuff2_sum_24h': 36, 'number_of_stuff2_sum_2h': 30},
                        {'col2': 9, 'number_of_stuff1_avg_1h': 0.0, 'number_of_stuff1_avg_24h': 0.0, 'number_of_stuff1_avg_2h': 0.0,
                         'number_of_stuff1_max_1h': 0, 'number_of_stuff1_max_24h': 0, 'number_of_stuff1_max_2h': 0,
                         'number_of_stuff1_min_1h': 0, 'number_of_stuff1_min_24h': 0, 'number_of_stuff1_min_2h': 0,
                         'number_of_stuff1_sum_1h': 0, 'number_of_stuff1_sum_24h': 0, 'number_of_stuff1_sum_2h': 0,
                         'number_of_stuff2_avg_1h': 8.0, 'number_of_stuff2_avg_24h': 4.5, 'number_of_stuff2_avg_2h': 7.0,
                         'number_of_stuff2_max_1h': 9, 'number_of_stuff2_max_24h': 9, 'number_of_stuff2_max_2h': 9,
                         'number_of_stuff2_min_1h': 7, 'number_of_stuff2_min_24h': 0, 'number_of_stuff2_min_2h': 5,
                         'number_of_stuff2_sum_1h': 24, 'number_of_stuff2_sum_24h': 45, 'number_of_stuff2_sum_2h': 35}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_sliding_window_multiple_keys_aggregation_flow():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, f'{i % 2}', test_base_time + timedelta(minutes=i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0, 'number_of_stuff_sum_24h': 0,
         'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0, 'number_of_stuff_avg_24h': 0.0},
        {'col1': 1, 'number_of_stuff_sum_1h': 1, 'number_of_stuff_sum_2h': 1, 'number_of_stuff_sum_24h': 1,
         'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0, 'number_of_stuff_avg_24h': 1.0},
        {'col1': 2, 'number_of_stuff_sum_1h': 2, 'number_of_stuff_sum_2h': 2, 'number_of_stuff_sum_24h': 2,
         'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0, 'number_of_stuff_avg_24h': 1.0},
        {'col1': 3, 'number_of_stuff_sum_1h': 4, 'number_of_stuff_sum_2h': 4, 'number_of_stuff_sum_24h': 4,
         'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 2.0, 'number_of_stuff_avg_24h': 2.0},
        {'col1': 4, 'number_of_stuff_sum_1h': 6, 'number_of_stuff_sum_2h': 6, 'number_of_stuff_sum_24h': 6,
         'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 2.0, 'number_of_stuff_avg_24h': 2.0},
        {'col1': 5, 'number_of_stuff_sum_1h': 9, 'number_of_stuff_sum_2h': 9, 'number_of_stuff_sum_24h': 9,
         'number_of_stuff_avg_1h': 3.0, 'number_of_stuff_avg_2h': 3.0, 'number_of_stuff_avg_24h': 3.0},
        {'col1': 6, 'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 12, 'number_of_stuff_sum_24h': 12,
         'number_of_stuff_avg_1h': 3.0, 'number_of_stuff_avg_2h': 3.0, 'number_of_stuff_avg_24h': 3.0},
        {'col1': 7, 'number_of_stuff_sum_1h': 16, 'number_of_stuff_sum_2h': 16, 'number_of_stuff_sum_24h': 16,
         'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 4.0, 'number_of_stuff_avg_24h': 4.0},
        {'col1': 8, 'number_of_stuff_sum_1h': 20, 'number_of_stuff_sum_2h': 20, 'number_of_stuff_sum_24h': 20,
         'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 4.0, 'number_of_stuff_avg_24h': 4.0},
        {'col1': 9, 'number_of_stuff_sum_1h': 25, 'number_of_stuff_sum_2h': 25, 'number_of_stuff_sum_24h': 25,
         'number_of_stuff_avg_1h': 5.0, 'number_of_stuff_avg_2h': 5.0, 'number_of_stuff_avg_24h': 5.0}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_sliding_window_aggregations_with_filters_flow():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'),
                                        aggr_filter=lambda element: element['is_valid'] == 0)],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        data = {'col1': i, 'is_valid': i % 2}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{'col1': 0, 'is_valid': 0, 'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0,
                         'number_of_stuff_sum_24h': 0, 'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0,
                         'number_of_stuff_avg_24h': 0.0},
                        {'col1': 1, 'is_valid': 1, 'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0,
                         'number_of_stuff_sum_24h': 0, 'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0,
                         'number_of_stuff_avg_24h': 0.0},
                        {'col1': 2, 'is_valid': 0, 'number_of_stuff_sum_1h': 2, 'number_of_stuff_sum_2h': 2,
                         'number_of_stuff_sum_24h': 2, 'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0,
                         'number_of_stuff_avg_24h': 1.0},
                        {'col1': 3, 'is_valid': 1, 'number_of_stuff_sum_1h': 2, 'number_of_stuff_sum_2h': 2,
                         'number_of_stuff_sum_24h': 2, 'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0,
                         'number_of_stuff_avg_24h': 1.0},
                        {'col1': 4, 'is_valid': 0, 'number_of_stuff_sum_1h': 6, 'number_of_stuff_sum_2h': 6,
                         'number_of_stuff_sum_24h': 6, 'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 2.0,
                         'number_of_stuff_avg_24h': 2.0},
                        {'col1': 5, 'is_valid': 1, 'number_of_stuff_sum_1h': 6, 'number_of_stuff_sum_2h': 6,
                         'number_of_stuff_sum_24h': 6, 'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 2.0,
                         'number_of_stuff_avg_24h': 2.0},
                        {'col1': 6, 'is_valid': 0, 'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 12,
                         'number_of_stuff_sum_24h': 12, 'number_of_stuff_avg_1h': 3.0, 'number_of_stuff_avg_2h': 3.0,
                         'number_of_stuff_avg_24h': 3.0},
                        {'col1': 7, 'is_valid': 1, 'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 12,
                         'number_of_stuff_sum_24h': 12, 'number_of_stuff_avg_1h': 3.0, 'number_of_stuff_avg_2h': 3.0,
                         'number_of_stuff_avg_24h': 3.0},
                        {'col1': 8, 'is_valid': 0, 'number_of_stuff_sum_1h': 20, 'number_of_stuff_sum_2h': 20,
                         'number_of_stuff_sum_24h': 20, 'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 4.0,
                         'number_of_stuff_avg_24h': 4.0},
                        {'col1': 9, 'is_valid': 1, 'number_of_stuff_sum_1h': 20, 'number_of_stuff_sum_2h': 20,
                         'number_of_stuff_sum_24h': 20, 'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 4.0,
                         'number_of_stuff_avg_24h': 4.0}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_sliding_window_aggregations_with_max_values_flow():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("num_hours_with_stuff_in_the_last_24h", "col1", ["count"],
                                        SlidingWindows(['24h'], '1h'),
                                        max_value=5)],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=10 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{'col1': 0, 'num_hours_with_stuff_in_the_last_24h_count_24h': 1},
                        {'col1': 1, 'num_hours_with_stuff_in_the_last_24h_count_24h': 2},
                        {'col1': 2, 'num_hours_with_stuff_in_the_last_24h_count_24h': 3},
                        {'col1': 3, 'num_hours_with_stuff_in_the_last_24h_count_24h': 4},
                        {'col1': 4, 'num_hours_with_stuff_in_the_last_24h_count_24h': 5},
                        {'col1': 5, 'num_hours_with_stuff_in_the_last_24h_count_24h': 5},
                        {'col1': 6, 'num_hours_with_stuff_in_the_last_24h_count_24h': 5},
                        {'col1': 7, 'num_hours_with_stuff_in_the_last_24h_count_24h': 5},
                        {'col1': 8, 'num_hours_with_stuff_in_the_last_24h_count_24h': 5},
                        {'col1': 9, 'num_hours_with_stuff_in_the_last_24h_count_24h': 5}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_sliding_window_simple_aggregation_flow_multiple_fields():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m')),
                        FieldAggregator("number_of_things", "col2", ["count"],
                                        SlidingWindows(['1h', '2h'], '15m')),
                        FieldAggregator("abc", "col3", ["sum"],
                                        SlidingWindows(['24h'], '10m'))],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        data = {'col1': i, 'col2': i * 1.2, 'col3': i * 2 + 4}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{'col1': 0, 'col2': 0.0, 'col3': 4, 'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0,
                         'number_of_stuff_sum_24h': 0, 'number_of_things_count_1h': 1, 'number_of_things_count_2h': 1,
                         'abc_sum_24h': 4, 'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0, 'number_of_stuff_avg_24h': 0.0},
                        {'col1': 1, 'col2': 1.2, 'col3': 6, 'number_of_stuff_sum_1h': 1, 'number_of_stuff_sum_2h': 1,
                         'number_of_stuff_sum_24h': 1, 'number_of_things_count_1h': 2, 'number_of_things_count_2h': 2,
                         'abc_sum_24h': 10, 'number_of_stuff_avg_1h': 0.5, 'number_of_stuff_avg_2h': 0.5, 'number_of_stuff_avg_24h': 0.5},
                        {'col1': 2, 'col2': 2.4, 'col3': 8, 'number_of_stuff_sum_1h': 3, 'number_of_stuff_sum_2h': 3,
                         'number_of_stuff_sum_24h': 3, 'number_of_things_count_1h': 3, 'number_of_things_count_2h': 3,
                         'abc_sum_24h': 18, 'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0, 'number_of_stuff_avg_24h': 1.0},
                        {'col1': 3, 'col2': 3.5999999999999996, 'col3': 10, 'number_of_stuff_sum_1h': 6,
                         'number_of_stuff_sum_2h': 6, 'number_of_stuff_sum_24h': 6, 'number_of_things_count_1h': 4,
                         'number_of_things_count_2h': 4, 'abc_sum_24h': 28, 'number_of_stuff_avg_1h': 1.5, 'number_of_stuff_avg_2h': 1.5,
                         'number_of_stuff_avg_24h': 1.5},
                        {'col1': 4, 'col2': 4.8, 'col3': 12, 'number_of_stuff_sum_1h': 10, 'number_of_stuff_sum_2h': 10,
                         'number_of_stuff_sum_24h': 10, 'number_of_things_count_1h': 5, 'number_of_things_count_2h': 5,
                         'abc_sum_24h': 40, 'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 2.0, 'number_of_stuff_avg_24h': 2.0},
                        {'col1': 5, 'col2': 6.0, 'col3': 14, 'number_of_stuff_sum_1h': 15, 'number_of_stuff_sum_2h': 15,
                         'number_of_stuff_sum_24h': 15, 'number_of_things_count_1h': 6, 'number_of_things_count_2h': 6,
                         'abc_sum_24h': 54, 'number_of_stuff_avg_1h': 2.5, 'number_of_stuff_avg_2h': 2.5, 'number_of_stuff_avg_24h': 2.5},
                        {'col1': 6, 'col2': 7.199999999999999, 'col3': 16, 'number_of_stuff_sum_1h': 21,
                         'number_of_stuff_sum_2h': 21, 'number_of_stuff_sum_24h': 21, 'number_of_things_count_1h': 7,
                         'number_of_things_count_2h': 7, 'abc_sum_24h': 70, 'number_of_stuff_avg_1h': 3.0,
                         'number_of_stuff_avg_2h': 3.0, 'number_of_stuff_avg_24h': 3.0},
                        {'col1': 7, 'col2': 8.4, 'col3': 18, 'number_of_stuff_sum_1h': 28, 'number_of_stuff_sum_2h': 28,
                         'number_of_stuff_sum_24h': 28, 'number_of_things_count_1h': 8, 'number_of_things_count_2h': 8,
                         'abc_sum_24h': 88, 'number_of_stuff_avg_1h': 3.5, 'number_of_stuff_avg_2h': 3.5, 'number_of_stuff_avg_24h': 3.5},
                        {'col1': 8, 'col2': 9.6, 'col3': 20, 'number_of_stuff_sum_1h': 36, 'number_of_stuff_sum_2h': 36,
                         'number_of_stuff_sum_24h': 36, 'number_of_things_count_1h': 9, 'number_of_things_count_2h': 9,
                         'abc_sum_24h': 108, 'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 4.0, 'number_of_stuff_avg_24h': 4.0},
                        {'col1': 9, 'col2': 10.799999999999999, 'col3': 22, 'number_of_stuff_sum_1h': 45,
                         'number_of_stuff_sum_2h': 45, 'number_of_stuff_sum_24h': 45,
                         'number_of_things_count_1h': 10, 'number_of_things_count_2h': 10, 'abc_sum_24h': 130,
                         'number_of_stuff_avg_1h': 4.5, 'number_of_stuff_avg_2h': 4.5, 'number_of_stuff_avg_24h': 4.5}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_fixed_window_simple_aggregation_flow():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["count"],
                                        FixedWindows(['1h', '2h', '3h', '24h']))],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{'col1': 0, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1, 'number_of_stuff_count_3h': 1,
                         'number_of_stuff_count_24h': 1},
                        {'col1': 1, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1, 'number_of_stuff_count_3h': 2,
                         'number_of_stuff_count_24h': 2},
                        {'col1': 2, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 2, 'number_of_stuff_count_3h': 3,
                         'number_of_stuff_count_24h': 3},
                        {'col1': 3, 'number_of_stuff_count_1h': 3, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 4,
                         'number_of_stuff_count_24h': 4},
                        {'col1': 4, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 5,
                         'number_of_stuff_count_24h': 5},
                        {'col1': 5, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 5, 'number_of_stuff_count_3h': 6,
                         'number_of_stuff_count_24h': 6},
                        {'col1': 6, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1, 'number_of_stuff_count_3h': 1,
                         'number_of_stuff_count_24h': 1},
                        {'col1': 7, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 2, 'number_of_stuff_count_3h': 2,
                         'number_of_stuff_count_24h': 2},
                        {'col1': 8, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 3,
                         'number_of_stuff_count_24h': 3},
                        {'col1': 9, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 4,
                         'number_of_stuff_count_24h': 4}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_fixed_window_aggregation_with_uncommon_windows_flow():
    time_format = '%Y-%m-%d %H:%M:%S.%f'
    columns = ['sample_time', 'signal', 'isotope']
    data = [[datetime.strptime('2021-05-30 16:42:15.797000', time_format).replace(tzinfo=timezone.utc), 790.235, 'U235'],
            [datetime.strptime('2021-05-30 16:45:15.798000', time_format).replace(tzinfo=timezone.utc), 498.491, 'U235'],
            [datetime.strptime('2021-05-30 16:48:15.799000', time_format).replace(tzinfo=timezone.utc), 34650.00343, 'U235'],
            [datetime.strptime('2021-05-30 16:51:15.800000', time_format).replace(tzinfo=timezone.utc), 189.823, 'U235'],
            [datetime.strptime('2021-05-30 16:54:15.801000', time_format).replace(tzinfo=timezone.utc), 379.524, 'U235'],
            [datetime.strptime('2021-05-30 16:57:15.802000', time_format).replace(tzinfo=timezone.utc), 2225.4952, 'U235'],
            [datetime.strptime('2021-05-30 17:00:15.803000', time_format).replace(tzinfo=timezone.utc), 1049.0903, 'U235'],
            [datetime.strptime('2021-05-30 17:03:15.804000', time_format).replace(tzinfo=timezone.utc), 41905.63447, 'U235'],
            [datetime.strptime('2021-05-30 17:06:15.805000', time_format).replace(tzinfo=timezone.utc), 4987.6764, 'U235'],
            [datetime.strptime('2021-05-30 17:09:15.806000', time_format).replace(tzinfo=timezone.utc), 67657.11975, 'U235'],
            [datetime.strptime('2021-05-30 17:12:15.807000', time_format).replace(tzinfo=timezone.utc), 56173.06327, 'U235'],
            [datetime.strptime('2021-05-30 17:15:15.808000', time_format).replace(tzinfo=timezone.utc), 14249.67394, 'U235'],
            [datetime.strptime('2021-05-30 17:18:15.809000', time_format).replace(tzinfo=timezone.utc), 656.831, 'U235'],
            [datetime.strptime('2021-05-30 17:21:15.810000', time_format).replace(tzinfo=timezone.utc), 5768.4822, 'U235'],
            [datetime.strptime('2021-05-30 17:24:15.811000', time_format).replace(tzinfo=timezone.utc), 929.028, 'U235'],
            [datetime.strptime('2021-05-30 17:27:15.812000', time_format).replace(tzinfo=timezone.utc), 2585.9646, 'U235'],
            [datetime.strptime('2021-05-30 17:30:15.813000', time_format).replace(tzinfo=timezone.utc), 358.918, 'U235']]

    df = pd.DataFrame(data, columns=columns)

    controller = build_flow([
        DataframeSource(df, time_field="sample_time", key_field="isotope"),
        AggregateByKey([FieldAggregator("samples", "signal", ["count"],
                                        FixedWindows(['15m', '25m', '45m', '1h']))], Table("U235_test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()
    termination_result = controller.await_termination()

    expected = [{'samples_count_15m': 1.0, 'samples_count_25m': 1.0, 'samples_count_45m': 1.0, 'samples_count_1h': 1.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:42:15.797000+0000', tz='UTC'), 'signal': 790.235,
                 'isotope': 'U235'},
                {'samples_count_15m': 1.0, 'samples_count_25m': 2.0, 'samples_count_45m': 2.0, 'samples_count_1h': 2.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:45:15.798000+0000', tz='UTC'), 'signal': 498.491,
                 'isotope': 'U235'},
                {'samples_count_15m': 2.0, 'samples_count_25m': 3.0, 'samples_count_45m': 3.0, 'samples_count_1h': 3.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:48:15.799000+0000', tz='UTC'), 'signal': 34650.00343,
                 'isotope': 'U235'},
                {'samples_count_15m': 3.0, 'samples_count_25m': 4.0, 'samples_count_45m': 4.0, 'samples_count_1h': 4.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:51:15.800000+0000', tz='UTC'), 'signal': 189.823,
                 'isotope': 'U235'},
                {'samples_count_15m': 4.0, 'samples_count_25m': 5.0, 'samples_count_45m': 5.0, 'samples_count_1h': 5.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:54:15.801000+0000', tz='UTC'), 'signal': 379.524,
                 'isotope': 'U235'},
                {'samples_count_15m': 5.0, 'samples_count_25m': 6.0, 'samples_count_45m': 6.0, 'samples_count_1h': 6.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:57:15.802000+0000', tz='UTC'), 'signal': 2225.4952,
                 'isotope': 'U235'},
                {'samples_count_15m': 1.0, 'samples_count_25m': 1.0, 'samples_count_45m': 7.0, 'samples_count_1h': 1.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:00:15.803000+0000', tz='UTC'), 'signal': 1049.0903,
                 'isotope': 'U235'},
                {'samples_count_15m': 2.0, 'samples_count_25m': 2.0, 'samples_count_45m': 8.0, 'samples_count_1h': 2.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:03:15.804000+0000', tz='UTC'), 'signal': 41905.63447,
                 'isotope': 'U235'},
                {'samples_count_15m': 3.0, 'samples_count_25m': 3.0, 'samples_count_45m': 9.0, 'samples_count_1h': 3.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:06:15.805000+0000', tz='UTC'), 'signal': 4987.6764,
                 'isotope': 'U235'},
                {'samples_count_15m': 4.0, 'samples_count_25m': 4.0, 'samples_count_45m': 10.0, 'samples_count_1h': 4.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:09:15.806000+0000', tz='UTC'), 'signal': 67657.11975,
                 'isotope': 'U235'},
                {'samples_count_15m': 5.0, 'samples_count_25m': 5.0, 'samples_count_45m': 11.0, 'samples_count_1h': 5.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:12:15.807000+0000', tz='UTC'), 'signal': 56173.06327,
                 'isotope': 'U235'},
                {'samples_count_15m': 1.0, 'samples_count_25m': 6.0, 'samples_count_45m': 1.0, 'samples_count_1h': 6.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:15:15.808000+0000', tz='UTC'), 'signal': 14249.67394,
                 'isotope': 'U235'},
                {'samples_count_15m': 2.0, 'samples_count_25m': 7.0, 'samples_count_45m': 2.0, 'samples_count_1h': 7.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:18:15.809000+0000', tz='UTC'), 'signal': 656.831,
                 'isotope': 'U235'},
                {'samples_count_15m': 3.0, 'samples_count_25m': 8.0, 'samples_count_45m': 3.0, 'samples_count_1h': 8.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:21:15.810000+0000', tz='UTC'), 'signal': 5768.4822,
                 'isotope': 'U235'},
                {'samples_count_15m': 4.0, 'samples_count_25m': 9.0, 'samples_count_45m': 4.0, 'samples_count_1h': 9.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:24:15.811000+0000', tz='UTC'), 'signal': 929.028,
                 'isotope': 'U235'},
                {'samples_count_15m': 5.0, 'samples_count_25m': 1.0, 'samples_count_45m': 5.0, 'samples_count_1h': 10.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:27:15.812000+0000', tz='UTC'), 'signal': 2585.9646,
                 'isotope': 'U235'},
                {'samples_count_15m': 1.0, 'samples_count_25m': 2.0, 'samples_count_45m': 6.0, 'samples_count_1h': 11.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:30:15.813000+0000', tz='UTC'), 'signal': 358.918,
                 'isotope': 'U235'}]

    assert termination_result == expected, \
        f'actual did not match expected. \n actual: {termination_result} \n expected: {expected}'


def test_fixed_window_aggregation_with_multiple_keys_flow():
    time_format = '%Y-%m-%d %H:%M:%S.%f'
    columns = ['sample_time', 'signal', 'isotope']
    data = [[datetime.strptime('2021-05-30 16:42:15.797000', time_format).replace(tzinfo=timezone.utc), 790.235, 'U235'],
            [datetime.strptime('2021-05-30 16:45:15.798000', time_format).replace(tzinfo=timezone.utc), 498.491, 'U235'],
            [datetime.strptime('2021-05-30 16:48:15.799000', time_format).replace(tzinfo=timezone.utc), 34650.00343, 'U238'],
            [datetime.strptime('2021-05-30 16:51:15.800000', time_format).replace(tzinfo=timezone.utc), 189.823, 'U238'],
            [datetime.strptime('2021-05-30 16:54:15.801000', time_format).replace(tzinfo=timezone.utc), 379.524, 'U238'],
            [datetime.strptime('2021-05-30 16:57:15.802000', time_format).replace(tzinfo=timezone.utc), 2225.4952, 'U238'],
            [datetime.strptime('2021-05-30 17:00:15.803000', time_format).replace(tzinfo=timezone.utc), 1049.0903, 'U235'],
            [datetime.strptime('2021-05-30 17:03:15.804000', time_format).replace(tzinfo=timezone.utc), 41905.63447, 'U238'],
            [datetime.strptime('2021-05-30 17:06:15.805000', time_format).replace(tzinfo=timezone.utc), 4987.6764, 'U235'],
            [datetime.strptime('2021-05-30 17:09:15.806000', time_format).replace(tzinfo=timezone.utc), 67657.11975, 'U235'],
            [datetime.strptime('2021-05-30 17:12:15.807000', time_format).replace(tzinfo=timezone.utc), 56173.06327, 'U235'],
            [datetime.strptime('2021-05-30 17:15:15.808000', time_format).replace(tzinfo=timezone.utc), 14249.67394, 'U238'],
            [datetime.strptime('2021-05-30 17:18:15.809000', time_format).replace(tzinfo=timezone.utc), 656.831, 'U238'],
            [datetime.strptime('2021-05-30 17:21:15.810000', time_format).replace(tzinfo=timezone.utc), 5768.4822, 'U235'],
            [datetime.strptime('2021-05-30 17:24:15.811000', time_format).replace(tzinfo=timezone.utc), 929.028, 'U235'],
            [datetime.strptime('2021-05-30 17:27:15.812000', time_format).replace(tzinfo=timezone.utc), 2585.9646, 'U238'],
            [datetime.strptime('2021-05-30 17:30:15.813000', time_format).replace(tzinfo=timezone.utc), 358.918, 'U238']]

    df = pd.DataFrame(data, columns=columns)

    controller = build_flow([
        DataframeSource(df, time_field="sample_time", key_field="isotope"),
        AggregateByKey([FieldAggregator("samples", "signal", ["count"],
                                        FixedWindows(['10m', '15m']))], Table("U235_test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()
    termination_result = controller.await_termination()

    expected = [
        {'samples_count_10m': 1.0, 'samples_count_15m': 1.0,
         'sample_time': pd.Timestamp('2021-05-30 16:42:15.797000+0000', tz='UTC'), 'signal': 790.235, 'isotope': 'U235'},
        {'samples_count_10m': 2.0, 'samples_count_15m': 1.0,
         'sample_time': pd.Timestamp('2021-05-30 16:45:15.798000+0000', tz='UTC'), 'signal': 498.491, 'isotope': 'U235'},
        {'samples_count_10m': 1.0, 'samples_count_15m': 1.0,
         'sample_time': pd.Timestamp('2021-05-30 16:48:15.799000+0000', tz='UTC'), 'signal': 34650.00343, 'isotope': 'U238'},
        {'samples_count_10m': 1.0, 'samples_count_15m': 2.0,
         'sample_time': pd.Timestamp('2021-05-30 16:51:15.800000+0000', tz='UTC'), 'signal': 189.823, 'isotope': 'U238'},
        {'samples_count_10m': 2.0, 'samples_count_15m': 3.0,
         'sample_time': pd.Timestamp('2021-05-30 16:54:15.801000+0000', tz='UTC'), 'signal': 379.524, 'isotope': 'U238'},
        {'samples_count_10m': 3.0, 'samples_count_15m': 4.0,
         'sample_time': pd.Timestamp('2021-05-30 16:57:15.802000+0000', tz='UTC'), 'signal': 2225.4952, 'isotope': 'U238'},
        {'samples_count_10m': 1.0, 'samples_count_15m': 1.0,
         'sample_time': pd.Timestamp('2021-05-30 17:00:15.803000+0000', tz='UTC'), 'signal': 1049.0903, 'isotope': 'U235'},
        {'samples_count_10m': 1.0, 'samples_count_15m': 1.0,
         'sample_time': pd.Timestamp('2021-05-30 17:03:15.804000+0000', tz='UTC'), 'signal': 41905.63447, 'isotope': 'U238'},
        {'samples_count_10m': 2.0, 'samples_count_15m': 2.0,
         'sample_time': pd.Timestamp('2021-05-30 17:06:15.805000+0000', tz='UTC'), 'signal': 4987.6764, 'isotope': 'U235'},
        {'samples_count_10m': 3.0, 'samples_count_15m': 3.0,
         'sample_time': pd.Timestamp('2021-05-30 17:09:15.806000+0000', tz='UTC'), 'signal': 67657.11975, 'isotope': 'U235'},
        {'samples_count_10m': 1.0, 'samples_count_15m': 4.0,
         'sample_time': pd.Timestamp('2021-05-30 17:12:15.807000+0000', tz='UTC'), 'signal': 56173.06327, 'isotope': 'U235'},
        {'samples_count_10m': 1.0, 'samples_count_15m': 1.0,
         'sample_time': pd.Timestamp('2021-05-30 17:15:15.808000+0000', tz='UTC'), 'signal': 14249.67394, 'isotope': 'U238'},
        {'samples_count_10m': 2.0, 'samples_count_15m': 2.0,
         'sample_time': pd.Timestamp('2021-05-30 17:18:15.809000+0000', tz='UTC'), 'signal': 656.831, 'isotope': 'U238'},
        {'samples_count_10m': 1.0, 'samples_count_15m': 1.0,
         'sample_time': pd.Timestamp('2021-05-30 17:21:15.810000+0000', tz='UTC'), 'signal': 5768.4822, 'isotope': 'U235'},
        {'samples_count_10m': 2.0, 'samples_count_15m': 2.0,
         'sample_time': pd.Timestamp('2021-05-30 17:24:15.811000+0000', tz='UTC'), 'signal': 929.028, 'isotope': 'U235'},
        {'samples_count_10m': 1.0, 'samples_count_15m': 3.0,
         'sample_time': pd.Timestamp('2021-05-30 17:27:15.812000+0000', tz='UTC'), 'signal': 2585.9646, 'isotope': 'U238'},
        {'samples_count_10m': 1.0, 'samples_count_15m': 1.0,
         'sample_time': pd.Timestamp('2021-05-30 17:30:15.813000+0000', tz='UTC'), 'signal': 358.918, 'isotope': 'U238'}]

    assert termination_result == expected, \
        f'actual did not match expected. \n actual: {termination_result} \n expected: {expected}'


def test_sliding_window_aggregation_with_uncommon_windows_flow():
    time_format = '%Y-%m-%d %H:%M:%S.%f'
    columns = ['sample_time', 'signal', 'isotope']
    data = [[datetime.strptime('2021-05-30 16:42:15.797000', time_format).replace(tzinfo=timezone.utc), 790.235, 'U235'],
            [datetime.strptime('2021-05-30 16:45:15.798000', time_format).replace(tzinfo=timezone.utc), 498.491, 'U235'],
            [datetime.strptime('2021-05-30 16:48:15.799000', time_format).replace(tzinfo=timezone.utc), 34650.00343, 'U235'],
            [datetime.strptime('2021-05-30 16:51:15.800000', time_format).replace(tzinfo=timezone.utc), 189.823, 'U235'],
            [datetime.strptime('2021-05-30 16:54:15.801000', time_format).replace(tzinfo=timezone.utc), 379.524, 'U235'],
            [datetime.strptime('2021-05-30 16:57:15.802000', time_format).replace(tzinfo=timezone.utc), 2225.4952, 'U235'],
            [datetime.strptime('2021-05-30 17:00:15.803000', time_format).replace(tzinfo=timezone.utc), 1049.0903, 'U235'],
            [datetime.strptime('2021-05-30 17:03:15.804000', time_format).replace(tzinfo=timezone.utc), 41905.63447, 'U235'],
            [datetime.strptime('2021-05-30 17:06:15.805000', time_format).replace(tzinfo=timezone.utc), 4987.6764, 'U235'],
            [datetime.strptime('2021-05-30 17:09:15.806000', time_format).replace(tzinfo=timezone.utc), 67657.11975, 'U235'],
            [datetime.strptime('2021-05-30 17:12:15.807000', time_format).replace(tzinfo=timezone.utc), 56173.06327, 'U235'],
            [datetime.strptime('2021-05-30 17:15:15.808000', time_format).replace(tzinfo=timezone.utc), 14249.67394, 'U235'],
            [datetime.strptime('2021-05-30 17:18:15.809000', time_format).replace(tzinfo=timezone.utc), 656.831, 'U235'],
            [datetime.strptime('2021-05-30 17:21:15.810000', time_format).replace(tzinfo=timezone.utc), 5768.4822, 'U235'],
            [datetime.strptime('2021-05-30 17:24:15.811000', time_format).replace(tzinfo=timezone.utc), 929.028, 'U235'],
            [datetime.strptime('2021-05-30 17:27:15.812000', time_format).replace(tzinfo=timezone.utc), 2585.9646, 'U235'],
            [datetime.strptime('2021-05-30 17:30:15.813000', time_format).replace(tzinfo=timezone.utc), 358.918, 'U235']]

    df = pd.DataFrame(data, columns=columns)

    controller = build_flow([
        DataframeSource(df, time_field="sample_time", key_field="isotope"),
        AggregateByKey([FieldAggregator("samples", "signal", ["count"],
                                        SlidingWindows(['15m', '25m', '45m', '1h'], '5m'))], Table("U235_test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()
    termination_result = controller.await_termination()

    expected = [{'samples_count_15m': 1.0, 'samples_count_25m': 1.0, 'samples_count_45m': 1.0, 'samples_count_1h': 1.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:42:15.797000+0000', tz='UTC'), 'signal': 790.235,
                 'isotope': 'U235'},
                {'samples_count_15m': 2.0, 'samples_count_25m': 2.0, 'samples_count_45m': 2.0, 'samples_count_1h': 2.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:45:15.798000+0000', tz='UTC'), 'signal': 498.491,
                 'isotope': 'U235'},
                {'samples_count_15m': 3.0, 'samples_count_25m': 3.0, 'samples_count_45m': 3.0, 'samples_count_1h': 3.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:48:15.799000+0000', tz='UTC'), 'signal': 34650.00343,
                 'isotope': 'U235'},
                {'samples_count_15m': 4.0, 'samples_count_25m': 4.0, 'samples_count_45m': 4.0, 'samples_count_1h': 4.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:51:15.800000+0000', tz='UTC'), 'signal': 189.823,
                 'isotope': 'U235'},
                {'samples_count_15m': 5.0, 'samples_count_25m': 5.0, 'samples_count_45m': 5.0, 'samples_count_1h': 5.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:54:15.801000+0000', tz='UTC'), 'signal': 379.524,
                 'isotope': 'U235'},
                {'samples_count_15m': 5.0, 'samples_count_25m': 6.0, 'samples_count_45m': 6.0, 'samples_count_1h': 6.0,
                 'sample_time': pd.Timestamp('2021-05-30 16:57:15.802000+0000', tz='UTC'), 'signal': 2225.4952,
                 'isotope': 'U235'},
                {'samples_count_15m': 4.0, 'samples_count_25m': 7.0, 'samples_count_45m': 7.0, 'samples_count_1h': 7.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:00:15.803000+0000', tz='UTC'), 'signal': 1049.0903,
                 'isotope': 'U235'},
                {'samples_count_15m': 5.0, 'samples_count_25m': 8.0, 'samples_count_45m': 8.0, 'samples_count_1h': 8.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:03:15.804000+0000', tz='UTC'), 'signal': 41905.63447,
                 'isotope': 'U235'},
                {'samples_count_15m': 4.0, 'samples_count_25m': 8.0, 'samples_count_45m': 9.0, 'samples_count_1h': 9.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:06:15.805000+0000', tz='UTC'), 'signal': 4987.6764,
                 'isotope': 'U235'},
                {'samples_count_15m': 5.0, 'samples_count_25m': 9.0, 'samples_count_45m': 10.0, 'samples_count_1h': 10.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:09:15.806000+0000', tz='UTC'), 'signal': 67657.11975,
                 'isotope': 'U235'},
                {'samples_count_15m': 5.0, 'samples_count_25m': 8.0, 'samples_count_45m': 11.0, 'samples_count_1h': 11.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:12:15.807000+0000', tz='UTC'), 'signal': 56173.06327,
                 'isotope': 'U235'},
                {'samples_count_15m': 4.0, 'samples_count_25m': 7.0, 'samples_count_45m': 12.0, 'samples_count_1h': 12.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:15:15.808000+0000', tz='UTC'), 'signal': 14249.67394,
                 'isotope': 'U235'},
                {'samples_count_15m': 5.0, 'samples_count_25m': 8.0, 'samples_count_45m': 13.0, 'samples_count_1h': 13.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:18:15.809000+0000', tz='UTC'),
                 'signal': 656.831, 'isotope': 'U235'},
                {'samples_count_15m': 4.0, 'samples_count_25m': 8.0, 'samples_count_45m': 14.0, 'samples_count_1h': 14.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:21:15.810000+0000', tz='UTC'), 'signal': 5768.4822,
                 'isotope': 'U235'},
                {'samples_count_15m': 5.0, 'samples_count_25m': 9.0, 'samples_count_45m': 15.0, 'samples_count_1h': 15.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:24:15.811000+0000', tz='UTC'), 'signal': 929.028,
                 'isotope': 'U235'},
                {'samples_count_15m': 5.0, 'samples_count_25m': 8.0, 'samples_count_45m': 15.0, 'samples_count_1h': 16.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:27:15.812000+0000', tz='UTC'), 'signal': 2585.9646,
                 'isotope': 'U235'},
                {'samples_count_15m': 4.0, 'samples_count_25m': 7.0, 'samples_count_45m': 14.0, 'samples_count_1h': 17.0,
                 'sample_time': pd.Timestamp('2021-05-30 17:30:15.813000+0000', tz='UTC'), 'signal': 358.918,
                 'isotope': 'U235'}]

    assert termination_result == expected, \
        f'actual did not match expected. \n actual: {termination_result} \n expected: {expected}'


def test_emit_max_event_sliding_window_multiple_keys_aggregation_flow():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       Table("test", NoopDriver()), emit_policy=EmitAfterMaxEvent(3)),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(12):
        data = {'col1': i}
        controller.emit(data, f'{i % 2}', test_base_time + timedelta(minutes=i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 4, 'number_of_stuff_sum_1h': 6, 'number_of_stuff_sum_2h': 6, 'number_of_stuff_sum_24h': 6, 'number_of_stuff_avg_1h': 2.0,
         'number_of_stuff_avg_2h': 2.0, 'number_of_stuff_avg_24h': 2.0},
        {'col1': 5, 'number_of_stuff_sum_1h': 9, 'number_of_stuff_sum_2h': 9, 'number_of_stuff_sum_24h': 9, 'number_of_stuff_avg_1h': 3.0,
         'number_of_stuff_avg_2h': 3.0, 'number_of_stuff_avg_24h': 3.0},
        {'col1': 10, 'number_of_stuff_sum_1h': 30, 'number_of_stuff_sum_2h': 30, 'number_of_stuff_sum_24h': 30,
         'number_of_stuff_avg_1h': 5.0, 'number_of_stuff_avg_2h': 5.0, 'number_of_stuff_avg_24h': 5.0},
        {'col1': 11, 'number_of_stuff_sum_1h': 36, 'number_of_stuff_sum_2h': 36, 'number_of_stuff_sum_24h': 36,
         'number_of_stuff_avg_1h': 6.0, 'number_of_stuff_avg_2h': 6.0, 'number_of_stuff_avg_24h': 6.0}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_error_on_bad_emit_policy():
    try:
        AggregateByKey([], Table("test", NoopDriver()), emit_policy=EmitEveryEvent),
        assert False
    except TypeError:
        pass


def test_emit_delay_aggregation_flow():
    q = queue.Queue(1)

    def reduce_fn(acc, x):
        if x['col1'] == 2:
            q.put(None)
        acc.append(x)
        return acc

    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "count"],
                                        SlidingWindows(['1h'], '10m'))],
                       Table("test", NoopDriver()), emit_policy=EmitAfterMaxEvent(4, 1)),
        Reduce([], reduce_fn),
    ]).run()

    for i in range(11):
        if i == 3:
            q.get()
        data = {'col1': i}
        controller.emit(data, 'katya', test_base_time + timedelta(seconds=i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 2, 'number_of_stuff_sum_1h': 3, 'number_of_stuff_count_1h': 3},
        {'col1': 6, 'number_of_stuff_sum_1h': 21, 'number_of_stuff_count_1h': 7},
        {'col1': 10, 'number_of_stuff_sum_1h': 55, 'number_of_stuff_count_1h': 11}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_aggregate_dict_simple_aggregation_flow():
    aggregations = [{'name': 'number_of_stuff',
                     'column': 'col1',
                     'operations': ["sum", "avg", "min", "max"],
                     'windows': ['1h', '2h', '24h'],
                     'period': '10m'}]
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey(aggregations, Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0, 'number_of_stuff_sum_24h': 0, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 0, 'number_of_stuff_max_2h': 0,
         'number_of_stuff_max_24h': 0, 'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0, 'number_of_stuff_avg_24h': 0.0},
        {'col1': 1, 'number_of_stuff_sum_1h': 1, 'number_of_stuff_sum_2h': 1, 'number_of_stuff_sum_24h': 1, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 1, 'number_of_stuff_max_2h': 1,
         'number_of_stuff_max_24h': 1, 'number_of_stuff_avg_1h': 0.5, 'number_of_stuff_avg_2h': 0.5, 'number_of_stuff_avg_24h': 0.5},
        {'col1': 2, 'number_of_stuff_sum_1h': 3, 'number_of_stuff_sum_2h': 3, 'number_of_stuff_sum_24h': 3, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 2, 'number_of_stuff_max_2h': 2,
         'number_of_stuff_max_24h': 2, 'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0, 'number_of_stuff_avg_24h': 1.0},
        {'col1': 3, 'number_of_stuff_sum_1h': 6, 'number_of_stuff_sum_2h': 6, 'number_of_stuff_sum_24h': 6, 'number_of_stuff_min_1h': 1,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 3, 'number_of_stuff_max_2h': 3,
         'number_of_stuff_max_24h': 3, 'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 1.5, 'number_of_stuff_avg_24h': 1.5},
        {'col1': 4, 'number_of_stuff_sum_1h': 9, 'number_of_stuff_sum_2h': 10, 'number_of_stuff_sum_24h': 10, 'number_of_stuff_min_1h': 2,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 4, 'number_of_stuff_max_2h': 4,
         'number_of_stuff_max_24h': 4, 'number_of_stuff_avg_1h': 3.0, 'number_of_stuff_avg_2h': 2.0, 'number_of_stuff_avg_24h': 2.0},
        {'col1': 5, 'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 15, 'number_of_stuff_sum_24h': 15, 'number_of_stuff_min_1h': 3,
         'number_of_stuff_min_2h': 1, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 5, 'number_of_stuff_max_2h': 5,
         'number_of_stuff_max_24h': 5, 'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 3.0, 'number_of_stuff_avg_24h': 2.5},
        {'col1': 6, 'number_of_stuff_sum_1h': 15, 'number_of_stuff_sum_2h': 20, 'number_of_stuff_sum_24h': 21, 'number_of_stuff_min_1h': 4,
         'number_of_stuff_min_2h': 2, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 6, 'number_of_stuff_max_2h': 6,
         'number_of_stuff_max_24h': 6, 'number_of_stuff_avg_1h': 5.0, 'number_of_stuff_avg_2h': 4.0, 'number_of_stuff_avg_24h': 3.0},
        {'col1': 7, 'number_of_stuff_sum_1h': 18, 'number_of_stuff_sum_2h': 25, 'number_of_stuff_sum_24h': 28, 'number_of_stuff_min_1h': 5,
         'number_of_stuff_min_2h': 3, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 7, 'number_of_stuff_max_2h': 7,
         'number_of_stuff_max_24h': 7, 'number_of_stuff_avg_1h': 6.0, 'number_of_stuff_avg_2h': 5.0, 'number_of_stuff_avg_24h': 3.5},
        {'col1': 8, 'number_of_stuff_sum_1h': 21, 'number_of_stuff_sum_2h': 30, 'number_of_stuff_sum_24h': 36, 'number_of_stuff_min_1h': 6,
         'number_of_stuff_min_2h': 4, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 8, 'number_of_stuff_max_2h': 8,
         'number_of_stuff_max_24h': 8, 'number_of_stuff_avg_1h': 7.0, 'number_of_stuff_avg_2h': 6.0, 'number_of_stuff_avg_24h': 4.0},
        {'col1': 9, 'number_of_stuff_sum_1h': 24, 'number_of_stuff_sum_2h': 35, 'number_of_stuff_sum_24h': 45, 'number_of_stuff_min_1h': 7,
         'number_of_stuff_min_2h': 5, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 9, 'number_of_stuff_max_2h': 9,
         'number_of_stuff_max_24h': 9, 'number_of_stuff_avg_1h': 8.0, 'number_of_stuff_avg_2h': 7.0, 'number_of_stuff_avg_24h': 4.5}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_aggregate_dict_fixed_window():
    aggregations = [{'name': 'number_of_stuff',
                     'column': 'col1',
                     'operations': ["count"],
                     'windows': ['1h', '2h', '3h', '24h']}]
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey(aggregations, Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{'col1': 0, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1, 'number_of_stuff_count_3h': 1,
                         'number_of_stuff_count_24h': 1},
                        {'col1': 1, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1, 'number_of_stuff_count_3h': 2,
                         'number_of_stuff_count_24h': 2},
                        {'col1': 2, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 2, 'number_of_stuff_count_3h': 3,
                         'number_of_stuff_count_24h': 3},
                        {'col1': 3, 'number_of_stuff_count_1h': 3, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 4,
                         'number_of_stuff_count_24h': 4},
                        {'col1': 4, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 5,
                         'number_of_stuff_count_24h': 5},
                        {'col1': 5, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 5, 'number_of_stuff_count_3h': 6,
                         'number_of_stuff_count_24h': 6},
                        {'col1': 6, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1, 'number_of_stuff_count_3h': 1,
                         'number_of_stuff_count_24h': 1},
                        {'col1': 7, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 2, 'number_of_stuff_count_3h': 2,
                         'number_of_stuff_count_24h': 2},
                        {'col1': 8, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 3,
                         'number_of_stuff_count_24h': 3},
                        {'col1': 9, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 4,
                         'number_of_stuff_count_24h': 4}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_sliding_window_old_event():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(3):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.emit({'col1': 3}, 'tal', test_base_time - timedelta(hours=25))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0, 'number_of_stuff_sum_24h': 0, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 0, 'number_of_stuff_max_2h': 0,
         'number_of_stuff_max_24h': 0, 'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0, 'number_of_stuff_avg_24h': 0.0},
        {'col1': 1, 'number_of_stuff_sum_1h': 1, 'number_of_stuff_sum_2h': 1, 'number_of_stuff_sum_24h': 1, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 1, 'number_of_stuff_max_2h': 1,
         'number_of_stuff_max_24h': 1, 'number_of_stuff_avg_1h': 0.5, 'number_of_stuff_avg_2h': 0.5, 'number_of_stuff_avg_24h': 0.5},
        {'col1': 2, 'number_of_stuff_sum_1h': 3, 'number_of_stuff_sum_2h': 3, 'number_of_stuff_sum_24h': 3, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 2, 'number_of_stuff_max_2h': 2,
         'number_of_stuff_max_24h': 2, 'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0, 'number_of_stuff_avg_24h': 1.0},
        {'col1': 3}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_fixed_window_old_event():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["count"],
                                        FixedWindows(['1h', '2h', '3h', '24h']))],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(3):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.emit({'col1': 3}, 'tal', test_base_time - timedelta(hours=25))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{'col1': 0, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1, 'number_of_stuff_count_3h': 1,
                         'number_of_stuff_count_24h': 1},
                        {'col1': 1, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1, 'number_of_stuff_count_3h': 2,
                         'number_of_stuff_count_24h': 2},
                        {'col1': 2, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 2, 'number_of_stuff_count_3h': 3,
                         'number_of_stuff_count_24h': 3},
                        {'col1': 3}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_fixed_window_out_of_order_event():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["count"],
                                        FixedWindows(['1h', '2h']))],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(3):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.emit({'col1': 3}, 'tal', test_base_time + timedelta(minutes=15))
    controller.emit({'col1': 4}, 'tal', test_base_time + timedelta(minutes=25 * 3))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{'col1': 0, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1},
                        {'col1': 1, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1},
                        {'col1': 2, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 2},
                        {'col1': 3, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 2},
                        {'col1': 4, 'number_of_stuff_count_1h': 3, 'number_of_stuff_count_2h': 3}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_fixed_window_roll_cached_buckets():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["count"],
                                        FixedWindows(['1h', '2h', '3h']))],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{'col1': 0, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1, 'number_of_stuff_count_3h': 1},
                        {'col1': 1, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1, 'number_of_stuff_count_3h': 2},
                        {'col1': 2, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 2, 'number_of_stuff_count_3h': 3},
                        {'col1': 3, 'number_of_stuff_count_1h': 3, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 4},
                        {'col1': 4, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 5},
                        {'col1': 5, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 5, 'number_of_stuff_count_3h': 6},
                        {'col1': 6, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 1, 'number_of_stuff_count_3h': 1},
                        {'col1': 7, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 2, 'number_of_stuff_count_3h': 2},
                        {'col1': 8, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 3},
                        {'col1': 9, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 4}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_sliding_window_roll_cached_buckets():
    controller = build_flow([
        SyncEmitSource(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                        SlidingWindows(['1h', '2h'], '10m'))],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0, 'number_of_stuff_min_1h': 0, 'number_of_stuff_min_2h': 0,
         'number_of_stuff_max_1h': 0, 'number_of_stuff_max_2h': 0, 'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0},
        {'col1': 1, 'number_of_stuff_sum_1h': 1, 'number_of_stuff_sum_2h': 1, 'number_of_stuff_min_1h': 0, 'number_of_stuff_min_2h': 0,
         'number_of_stuff_max_1h': 1, 'number_of_stuff_max_2h': 1, 'number_of_stuff_avg_1h': 0.5, 'number_of_stuff_avg_2h': 0.5},
        {'col1': 2, 'number_of_stuff_sum_1h': 3, 'number_of_stuff_sum_2h': 3, 'number_of_stuff_min_1h': 0, 'number_of_stuff_min_2h': 0,
         'number_of_stuff_max_1h': 2, 'number_of_stuff_max_2h': 2, 'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0},
        {'col1': 3, 'number_of_stuff_sum_1h': 6, 'number_of_stuff_sum_2h': 6, 'number_of_stuff_min_1h': 1, 'number_of_stuff_min_2h': 0,
         'number_of_stuff_max_1h': 3, 'number_of_stuff_max_2h': 3, 'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 1.5},
        {'col1': 4, 'number_of_stuff_sum_1h': 9, 'number_of_stuff_sum_2h': 10, 'number_of_stuff_min_1h': 2, 'number_of_stuff_min_2h': 0,
         'number_of_stuff_max_1h': 4, 'number_of_stuff_max_2h': 4, 'number_of_stuff_avg_1h': 3.0, 'number_of_stuff_avg_2h': 2.0},
        {'col1': 5, 'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 15, 'number_of_stuff_min_1h': 3, 'number_of_stuff_min_2h': 1,
         'number_of_stuff_max_1h': 5, 'number_of_stuff_max_2h': 5, 'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 3.0},
        {'col1': 6, 'number_of_stuff_sum_1h': 15, 'number_of_stuff_sum_2h': 20, 'number_of_stuff_min_1h': 4, 'number_of_stuff_min_2h': 2,
         'number_of_stuff_max_1h': 6, 'number_of_stuff_max_2h': 6, 'number_of_stuff_avg_1h': 5.0, 'number_of_stuff_avg_2h': 4.0},
        {'col1': 7, 'number_of_stuff_sum_1h': 18, 'number_of_stuff_sum_2h': 25, 'number_of_stuff_min_1h': 5, 'number_of_stuff_min_2h': 3,
         'number_of_stuff_max_1h': 7, 'number_of_stuff_max_2h': 7, 'number_of_stuff_avg_1h': 6.0, 'number_of_stuff_avg_2h': 5.0},
        {'col1': 8, 'number_of_stuff_sum_1h': 21, 'number_of_stuff_sum_2h': 30, 'number_of_stuff_min_1h': 6, 'number_of_stuff_min_2h': 4,
         'number_of_stuff_max_1h': 8, 'number_of_stuff_max_2h': 8, 'number_of_stuff_avg_1h': 7.0, 'number_of_stuff_avg_2h': 6.0},
        {'col1': 9, 'number_of_stuff_sum_1h': 24, 'number_of_stuff_sum_2h': 35, 'number_of_stuff_min_1h': 7, 'number_of_stuff_min_2h': 5,
         'number_of_stuff_max_1h': 9, 'number_of_stuff_max_2h': 9, 'number_of_stuff_avg_1h': 8.0, 'number_of_stuff_avg_2h': 7.0}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_aggregation_unique_fields():
    try:
        build_flow([
            SyncEmitSource(),
            AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                            SlidingWindows(['1h', '2h', '24h'], '10m')),
                            FieldAggregator("number_of_stuff", "col1", ["count"],
                                            SlidingWindows(['1h', '2h'], '15m'))],
                           Table("test", NoopDriver())),
            Reduce([], lambda acc, x: append_return(acc, x)), ]).run()
        assert False
    except TypeError:
        pass


def test_fixed_window_aggregation_with_first_and_last_aggregates():
    df = pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2021-07-13 06:43:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 06:46:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 06:49:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 06:52:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 06:55:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 06:58:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 07:01:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 07:04:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 07:07:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 07:10:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 07:13:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 07:16:01.084587+0000", tz="UTC"),
                pd.Timestamp("2021-07-13 07:19:01.084587+0000", tz="UTC"),
            ],
            "emission": [
                16.44200,
                64807.90231,
                413.90100,
                73621.21551,
                53936.62158,
                13582.52318,
                966.80400,
                450.40700,
                4965.28760,
                42982.57194,
                1594.40460,
                69601.73368,
                48038.65572,
            ],
            "sensor_id": [
                "0654-329-05",
                "0654-329-05",
                "0654-329-05",
                "0654-329-05",
                "0654-329-05",
                "0654-329-05",
                "0654-329-05",
                "0654-329-05",
                "0654-329-05",
                "0654-329-05",
                "0654-329-05",
                "0654-329-05",
                "0654-329-05",
            ],
        }
    )

    controller = build_flow(
        [
            DataframeSource(df, time_field="timestamp", key_field="sensor_id"),
            AggregateByKey(
                [
                    FieldAggregator(
                        "samples",
                        "emission",
                        ["last", "first", "count"],
                        FixedWindows(["10m"]),
                    )
                ],
                Table("MyTable", NoopDriver()),
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()
    termination_result = controller.await_termination()

    expected = [
        {
            "samples_last_10m": 16.442,
            "samples_count_10m": 1.0,
            "samples_first_10m": 16.442,
            "timestamp": pd.Timestamp("2021-07-13 06:43:01.084587+0000", tz="UTC"),
            "emission": 16.442,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 64807.90231,
            "samples_count_10m": 2.0,
            "samples_first_10m": 16.442,
            "timestamp": pd.Timestamp("2021-07-13 06:46:01.084587+0000", tz="UTC"),
            "emission": 64807.90231,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 413.901,
            "samples_count_10m": 3.0,
            "samples_first_10m": 16.442,
            "timestamp": pd.Timestamp("2021-07-13 06:49:01.084587+0000", tz="UTC"),
            "emission": 413.901,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 73621.21551,
            "samples_count_10m": 1.0,
            "samples_first_10m": 73621.21551,
            "timestamp": pd.Timestamp("2021-07-13 06:52:01.084587+0000", tz="UTC"),
            "emission": 73621.21551,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 53936.62158,
            "samples_count_10m": 2.0,
            "samples_first_10m": 73621.21551,
            "timestamp": pd.Timestamp("2021-07-13 06:55:01.084587+0000", tz="UTC"),
            "emission": 53936.62158,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 13582.52318,
            "samples_count_10m": 3.0,
            "samples_first_10m": 73621.21551,
            "timestamp": pd.Timestamp("2021-07-13 06:58:01.084587+0000", tz="UTC"),
            "emission": 13582.52318,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 966.804,
            "samples_count_10m": 1.0,
            "samples_first_10m": 966.804,
            "timestamp": pd.Timestamp("2021-07-13 07:01:01.084587+0000", tz="UTC"),
            "emission": 966.804,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 450.407,
            "samples_count_10m": 2.0,
            "samples_first_10m": 966.804,
            "timestamp": pd.Timestamp("2021-07-13 07:04:01.084587+0000", tz="UTC"),
            "emission": 450.407,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 4965.2876,
            "samples_count_10m": 3.0,
            "samples_first_10m": 966.804,
            "timestamp": pd.Timestamp("2021-07-13 07:07:01.084587+0000", tz="UTC"),
            "emission": 4965.2876,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 42982.57194,
            "samples_count_10m": 1.0,
            "samples_first_10m": 42982.57194,
            "timestamp": pd.Timestamp("2021-07-13 07:10:01.084587+0000", tz="UTC"),
            "emission": 42982.57194,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 1594.4046,
            "samples_count_10m": 2.0,
            "samples_first_10m": 42982.57194,
            "timestamp": pd.Timestamp("2021-07-13 07:13:01.084587+0000", tz="UTC"),
            "emission": 1594.4046,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 69601.73368,
            "samples_count_10m": 3.0,
            "samples_first_10m": 42982.57194,
            "timestamp": pd.Timestamp("2021-07-13 07:16:01.084587+0000", tz="UTC"),
            "emission": 69601.73368,
            "sensor_id": "0654-329-05",
        },
        {
            "samples_last_10m": 48038.65572,
            "samples_count_10m": 4.0,
            "samples_first_10m": 42982.57194,
            "timestamp": pd.Timestamp("2021-07-13 07:19:01.084587+0000", tz="UTC"),
            "emission": 48038.65572,
            "sensor_id": "0654-329-05",
        },
    ]

    assert (
        termination_result == expected
    ), f"actual did not match expected. \n actual: {termination_result} \n expected: {expected}"
