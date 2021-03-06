import math
import queue
from datetime import datetime, timedelta

from storey import build_flow, SyncEmitSource, Reduce, Table, AggregateByKey, FieldAggregator, NoopDriver
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
                                        max_value=1)],
                       Table("test", NoopDriver())),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=10 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{'col1': 0, 'num_hours_with_stuff_in_the_last_24h_count_24h': 1},
                        {'col1': 1, 'num_hours_with_stuff_in_the_last_24h_count_24h': 1},
                        {'col1': 2, 'num_hours_with_stuff_in_the_last_24h_count_24h': 1},
                        {'col1': 3, 'num_hours_with_stuff_in_the_last_24h_count_24h': 1},
                        {'col1': 4, 'num_hours_with_stuff_in_the_last_24h_count_24h': 1},
                        {'col1': 5, 'num_hours_with_stuff_in_the_last_24h_count_24h': 1},
                        {'col1': 6, 'num_hours_with_stuff_in_the_last_24h_count_24h': 2},
                        {'col1': 7, 'num_hours_with_stuff_in_the_last_24h_count_24h': 2},
                        {'col1': 8, 'num_hours_with_stuff_in_the_last_24h_count_24h': 2},
                        {'col1': 9, 'num_hours_with_stuff_in_the_last_24h_count_24h': 2}]

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
                        {'col1': 1, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 2, 'number_of_stuff_count_3h': 2,
                         'number_of_stuff_count_24h': 2},
                        {'col1': 2, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 3,
                         'number_of_stuff_count_24h': 3},
                        {'col1': 3, 'number_of_stuff_count_1h': 3, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 4,
                         'number_of_stuff_count_24h': 4},
                        {'col1': 4, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 5,
                         'number_of_stuff_count_24h': 5},
                        {'col1': 5, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 5, 'number_of_stuff_count_3h': 6,
                         'number_of_stuff_count_24h': 6},
                        {'col1': 6, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 6,
                         'number_of_stuff_count_24h': 7},
                        {'col1': 7, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 7,
                         'number_of_stuff_count_24h': 8},
                        {'col1': 8, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 5,
                         'number_of_stuff_count_24h': 9},
                        {'col1': 9, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 6,
                         'number_of_stuff_count_24h': 10}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


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
                        {'col1': 1, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 2, 'number_of_stuff_count_3h': 2,
                         'number_of_stuff_count_24h': 2},
                        {'col1': 2, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 3,
                         'number_of_stuff_count_24h': 3},
                        {'col1': 3, 'number_of_stuff_count_1h': 3, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 4,
                         'number_of_stuff_count_24h': 4},
                        {'col1': 4, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 5,
                         'number_of_stuff_count_24h': 5},
                        {'col1': 5, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 5, 'number_of_stuff_count_3h': 6,
                         'number_of_stuff_count_24h': 6},
                        {'col1': 6, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 6,
                         'number_of_stuff_count_24h': 7},
                        {'col1': 7, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 7,
                         'number_of_stuff_count_24h': 8},
                        {'col1': 8, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 5,
                         'number_of_stuff_count_24h': 9},
                        {'col1': 9, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 6,
                         'number_of_stuff_count_24h': 10}]

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
                        {'col1': 1, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 2, 'number_of_stuff_count_3h': 2,
                         'number_of_stuff_count_24h': 2},
                        {'col1': 2, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 3,
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
                        {'col1': 1, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 2},
                        {'col1': 2, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 3},
                        {'col1': 3, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 2},
                        {'col1': 4, 'number_of_stuff_count_1h': 3, 'number_of_stuff_count_2h': 5}]

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
                        {'col1': 1, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 2, 'number_of_stuff_count_3h': 2},
                        {'col1': 2, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 3},
                        {'col1': 3, 'number_of_stuff_count_1h': 3, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 4},
                        {'col1': 4, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 5},
                        {'col1': 5, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 5, 'number_of_stuff_count_3h': 6},
                        {'col1': 6, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 6},
                        {'col1': 7, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 7},
                        {'col1': 8, 'number_of_stuff_count_1h': 1, 'number_of_stuff_count_2h': 3, 'number_of_stuff_count_3h': 5},
                        {'col1': 9, 'number_of_stuff_count_1h': 2, 'number_of_stuff_count_2h': 4, 'number_of_stuff_count_3h': 6}]

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
