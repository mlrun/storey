import asyncio
from datetime import datetime, timedelta
import pytest
import math

from storey import build_flow, Source, Reduce, Table, V3ioDriver, MapWithState, AggregateByKey, FieldAggregator, \
    QueryByKey, WriteToTable, Context

from storey.dtypes import SlidingWindows, FixedWindows
from storey.utils import _split_path

from .integration_test_utils import setup_teardown_test, append_return, test_base_time


@pytest.mark.parametrize('partitioned_by_key', [True, False])
def test_aggregate_and_query_with_different_windows(setup_teardown_test, partitioned_by_key):
    table = Table(setup_teardown_test, V3ioDriver(), partitioned_by_key=partitioned_by_key)

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max", "sqr"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0, 'number_of_stuff_sum_24h': 0, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 0, 'number_of_stuff_max_2h': 0,
         'number_of_stuff_max_24h': 0, 'number_of_stuff_sqr_1h': 0, 'number_of_stuff_sqr_2h': 0, 'number_of_stuff_sqr_24h': 0,
         'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0, 'number_of_stuff_avg_24h': 0.0, 'col1': 0},
        {'number_of_stuff_sum_1h': 1, 'number_of_stuff_sum_2h': 1, 'number_of_stuff_sum_24h': 1, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 1, 'number_of_stuff_max_2h': 1,
         'number_of_stuff_max_24h': 1, 'number_of_stuff_sqr_1h': 1, 'number_of_stuff_sqr_2h': 1, 'number_of_stuff_sqr_24h': 1,
         'number_of_stuff_avg_1h': 0.5, 'number_of_stuff_avg_2h': 0.5, 'number_of_stuff_avg_24h': 0.5, 'col1': 1},
        {'number_of_stuff_sum_1h': 3, 'number_of_stuff_sum_2h': 3, 'number_of_stuff_sum_24h': 3, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 2, 'number_of_stuff_max_2h': 2,
         'number_of_stuff_max_24h': 2, 'number_of_stuff_sqr_1h': 5, 'number_of_stuff_sqr_2h': 5, 'number_of_stuff_sqr_24h': 5,
         'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0, 'number_of_stuff_avg_24h': 1.0, 'col1': 2},
        {'number_of_stuff_sum_1h': 6, 'number_of_stuff_sum_2h': 6, 'number_of_stuff_sum_24h': 6, 'number_of_stuff_min_1h': 1,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 3, 'number_of_stuff_max_2h': 3,
         'number_of_stuff_max_24h': 3, 'number_of_stuff_sqr_1h': 14, 'number_of_stuff_sqr_2h': 14, 'number_of_stuff_sqr_24h': 14,
         'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 1.5, 'number_of_stuff_avg_24h': 1.5, 'col1': 3},
        {'number_of_stuff_sum_1h': 9, 'number_of_stuff_sum_2h': 10, 'number_of_stuff_sum_24h': 10, 'number_of_stuff_min_1h': 2,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 4, 'number_of_stuff_max_2h': 4,
         'number_of_stuff_max_24h': 4, 'number_of_stuff_sqr_1h': 29, 'number_of_stuff_sqr_2h': 30, 'number_of_stuff_sqr_24h': 30,
         'number_of_stuff_avg_1h': 3.0, 'number_of_stuff_avg_2h': 2.0, 'number_of_stuff_avg_24h': 2.0, 'col1': 4},
        {'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 15, 'number_of_stuff_sum_24h': 15, 'number_of_stuff_min_1h': 3,
         'number_of_stuff_min_2h': 1, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 5, 'number_of_stuff_max_2h': 5,
         'number_of_stuff_max_24h': 5, 'number_of_stuff_sqr_1h': 50, 'number_of_stuff_sqr_2h': 55, 'number_of_stuff_sqr_24h': 55,
         'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 3.0, 'number_of_stuff_avg_24h': 2.5, 'col1': 5},
        {'number_of_stuff_sum_1h': 15, 'number_of_stuff_sum_2h': 20, 'number_of_stuff_sum_24h': 21, 'number_of_stuff_min_1h': 4,
         'number_of_stuff_min_2h': 2, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 6, 'number_of_stuff_max_2h': 6,
         'number_of_stuff_max_24h': 6, 'number_of_stuff_sqr_1h': 77, 'number_of_stuff_sqr_2h': 90, 'number_of_stuff_sqr_24h': 91,
         'number_of_stuff_avg_1h': 5.0, 'number_of_stuff_avg_2h': 4.0, 'number_of_stuff_avg_24h': 3.0, 'col1': 6},
        {'number_of_stuff_sum_1h': 18, 'number_of_stuff_sum_2h': 25, 'number_of_stuff_sum_24h': 28, 'number_of_stuff_min_1h': 5,
         'number_of_stuff_min_2h': 3, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 7, 'number_of_stuff_max_2h': 7,
         'number_of_stuff_max_24h': 7, 'number_of_stuff_sqr_1h': 110, 'number_of_stuff_sqr_2h': 135, 'number_of_stuff_sqr_24h': 140,
         'number_of_stuff_avg_1h': 6.0, 'number_of_stuff_avg_2h': 5.0, 'number_of_stuff_avg_24h': 3.5, 'col1': 7},
        {'number_of_stuff_sum_1h': 21, 'number_of_stuff_sum_2h': 30, 'number_of_stuff_sum_24h': 36, 'number_of_stuff_min_1h': 6,
         'number_of_stuff_min_2h': 4, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 8, 'number_of_stuff_max_2h': 8,
         'number_of_stuff_max_24h': 8, 'number_of_stuff_sqr_1h': 149, 'number_of_stuff_sqr_2h': 190, 'number_of_stuff_sqr_24h': 204,
         'number_of_stuff_avg_1h': 7.0, 'number_of_stuff_avg_2h': 6.0, 'number_of_stuff_avg_24h': 4.0, 'col1': 8},
        {'number_of_stuff_sum_1h': 24, 'number_of_stuff_sum_2h': 35, 'number_of_stuff_sum_24h': 45, 'number_of_stuff_min_1h': 7,
         'number_of_stuff_min_2h': 5, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 9, 'number_of_stuff_max_2h': 9,
         'number_of_stuff_max_24h': 9, 'number_of_stuff_sqr_1h': 194, 'number_of_stuff_sqr_2h': 255, 'number_of_stuff_sqr_24h': 285,
         'number_of_stuff_avg_1h': 8.0, 'number_of_stuff_avg_2h': 7.0, 'number_of_stuff_avg_24h': 4.5, 'col1': 9}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    other_table = Table(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        QueryByKey(["number_of_stuff_sum_1h", "number_of_stuff_avg_1h", "number_of_stuff_min_1h", "number_of_stuff_max_1h"],
                   other_table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)
    controller.emit(data, 'tal', base_time + timedelta(minutes=25))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 10, 'number_of_stuff_sum_1h': 17, 'number_of_stuff_min_1h': 8, 'number_of_stuff_max_1h': 9, 'number_of_stuff_avg_1h': 8.5},
        {'col1': 10, 'number_of_stuff_sum_1h': 9.0, 'number_of_stuff_min_1h': 9.0, 'number_of_stuff_max_1h': 9.0,
         'number_of_stuff_avg_1h': 9.0},
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_query_virtual_aggregations_flow(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["avg", "stddev", "stdvar"],
                                        SlidingWindows(['24h'], '10m'))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'dina', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'number_of_stuff_avg_24h': 0.0, 'number_of_stuff_stddev_24h': 0, 'number_of_stuff_stdvar_24h': 0},
        {'col1': 1, 'number_of_stuff_avg_24h': 0.5, 'number_of_stuff_stddev_24h': math.sqrt(0.5), 'number_of_stuff_stdvar_24h': 0.5},
        {'col1': 2, 'number_of_stuff_avg_24h': 1.0, 'number_of_stuff_stddev_24h': 1.0, 'number_of_stuff_stdvar_24h': 1.0},
        {'col1': 3, 'number_of_stuff_avg_24h': 1.5, 'number_of_stuff_stddev_24h': math.sqrt(1.6666666666666667),
         'number_of_stuff_stdvar_24h': 1.6666666666666667},
        {'col1': 4, 'number_of_stuff_avg_24h': 2.0, 'number_of_stuff_stddev_24h': math.sqrt(2.5), 'number_of_stuff_stdvar_24h': 2.5},
        {'col1': 5, 'number_of_stuff_avg_24h': 2.5, 'number_of_stuff_stddev_24h': math.sqrt(3.5), 'number_of_stuff_stdvar_24h': 3.5},
        {'col1': 6, 'number_of_stuff_avg_24h': 3.0, 'number_of_stuff_stddev_24h': math.sqrt(4.666666666666667),
         'number_of_stuff_stdvar_24h': 4.666666666666667},
        {'col1': 7, 'number_of_stuff_avg_24h': 3.5, 'number_of_stuff_stddev_24h': math.sqrt(6.0), 'number_of_stuff_stdvar_24h': 6.0},
        {'col1': 8, 'number_of_stuff_avg_24h': 4.0, 'number_of_stuff_stddev_24h': math.sqrt(7.5), 'number_of_stuff_stdvar_24h': 7.5},
        {'col1': 9, 'number_of_stuff_avg_24h': 4.5, 'number_of_stuff_stddev_24h': math.sqrt(9.166666666666666),
         'number_of_stuff_stdvar_24h': 9.166666666666666}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    other_table = Table(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        QueryByKey(["number_of_stuff_avg_1h", "number_of_stuff_stdvar_2h", "number_of_stuff_stddev_3h"],
                   other_table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'dina', base_time)
    controller.emit(data, 'dina', base_time + timedelta(minutes=25))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 10, 'number_of_stuff_avg_1h': 8.5, 'number_of_stuff_stdvar_2h': 1.6666666666666667,
         'number_of_stuff_stddev_3h': 1.8708286933869707},
        {'col1': 10, 'number_of_stuff_avg_1h': 9.0, 'number_of_stuff_stdvar_2h': 1.0, 'number_of_stuff_stddev_3h': 1.8708286933869707},
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


@pytest.mark.parametrize('partitioned_by_key', [True, False])
def test_query_aggregate_by_key(setup_teardown_test, partitioned_by_key):
    table = Table(setup_teardown_test, V3ioDriver(), partitioned_by_key=partitioned_by_key)

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max", "sqr"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0, 'number_of_stuff_sum_24h': 0, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 0, 'number_of_stuff_max_2h': 0,
         'number_of_stuff_max_24h': 0, 'number_of_stuff_sqr_1h': 0, 'number_of_stuff_sqr_2h': 0, 'number_of_stuff_sqr_24h': 0,
         'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0, 'number_of_stuff_avg_24h': 0.0, 'col1': 0},
        {'number_of_stuff_sum_1h': 1, 'number_of_stuff_sum_2h': 1, 'number_of_stuff_sum_24h': 1, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 1, 'number_of_stuff_max_2h': 1,
         'number_of_stuff_max_24h': 1, 'number_of_stuff_sqr_1h': 1, 'number_of_stuff_sqr_2h': 1, 'number_of_stuff_sqr_24h': 1,
         'number_of_stuff_avg_1h': 0.5, 'number_of_stuff_avg_2h': 0.5, 'number_of_stuff_avg_24h': 0.5, 'col1': 1},
        {'number_of_stuff_sum_1h': 3, 'number_of_stuff_sum_2h': 3, 'number_of_stuff_sum_24h': 3, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 2, 'number_of_stuff_max_2h': 2,
         'number_of_stuff_max_24h': 2, 'number_of_stuff_sqr_1h': 5, 'number_of_stuff_sqr_2h': 5, 'number_of_stuff_sqr_24h': 5,
         'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0, 'number_of_stuff_avg_24h': 1.0, 'col1': 2},
        {'number_of_stuff_sum_1h': 6, 'number_of_stuff_sum_2h': 6, 'number_of_stuff_sum_24h': 6, 'number_of_stuff_min_1h': 1,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 3, 'number_of_stuff_max_2h': 3,
         'number_of_stuff_max_24h': 3, 'number_of_stuff_sqr_1h': 14, 'number_of_stuff_sqr_2h': 14, 'number_of_stuff_sqr_24h': 14,
         'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 1.5, 'number_of_stuff_avg_24h': 1.5, 'col1': 3},
        {'number_of_stuff_sum_1h': 9, 'number_of_stuff_sum_2h': 10, 'number_of_stuff_sum_24h': 10, 'number_of_stuff_min_1h': 2,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 4, 'number_of_stuff_max_2h': 4,
         'number_of_stuff_max_24h': 4, 'number_of_stuff_sqr_1h': 29, 'number_of_stuff_sqr_2h': 30, 'number_of_stuff_sqr_24h': 30,
         'number_of_stuff_avg_1h': 3.0, 'number_of_stuff_avg_2h': 2.0, 'number_of_stuff_avg_24h': 2.0, 'col1': 4},
        {'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 15, 'number_of_stuff_sum_24h': 15, 'number_of_stuff_min_1h': 3,
         'number_of_stuff_min_2h': 1, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 5, 'number_of_stuff_max_2h': 5,
         'number_of_stuff_max_24h': 5, 'number_of_stuff_sqr_1h': 50, 'number_of_stuff_sqr_2h': 55, 'number_of_stuff_sqr_24h': 55,
         'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 3.0, 'number_of_stuff_avg_24h': 2.5, 'col1': 5},
        {'number_of_stuff_sum_1h': 15, 'number_of_stuff_sum_2h': 20, 'number_of_stuff_sum_24h': 21, 'number_of_stuff_min_1h': 4,
         'number_of_stuff_min_2h': 2, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 6, 'number_of_stuff_max_2h': 6,
         'number_of_stuff_max_24h': 6, 'number_of_stuff_sqr_1h': 77, 'number_of_stuff_sqr_2h': 90, 'number_of_stuff_sqr_24h': 91,
         'number_of_stuff_avg_1h': 5.0, 'number_of_stuff_avg_2h': 4.0, 'number_of_stuff_avg_24h': 3.0, 'col1': 6},
        {'number_of_stuff_sum_1h': 18, 'number_of_stuff_sum_2h': 25, 'number_of_stuff_sum_24h': 28, 'number_of_stuff_min_1h': 5,
         'number_of_stuff_min_2h': 3, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 7, 'number_of_stuff_max_2h': 7,
         'number_of_stuff_max_24h': 7, 'number_of_stuff_sqr_1h': 110, 'number_of_stuff_sqr_2h': 135, 'number_of_stuff_sqr_24h': 140,
         'number_of_stuff_avg_1h': 6.0, 'number_of_stuff_avg_2h': 5.0, 'number_of_stuff_avg_24h': 3.5, 'col1': 7},
        {'number_of_stuff_sum_1h': 21, 'number_of_stuff_sum_2h': 30, 'number_of_stuff_sum_24h': 36, 'number_of_stuff_min_1h': 6,
         'number_of_stuff_min_2h': 4, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 8, 'number_of_stuff_max_2h': 8,
         'number_of_stuff_max_24h': 8, 'number_of_stuff_sqr_1h': 149, 'number_of_stuff_sqr_2h': 190, 'number_of_stuff_sqr_24h': 204,
         'number_of_stuff_avg_1h': 7.0, 'number_of_stuff_avg_2h': 6.0, 'number_of_stuff_avg_24h': 4.0, 'col1': 8},
        {'number_of_stuff_sum_1h': 24, 'number_of_stuff_sum_2h': 35, 'number_of_stuff_sum_24h': 45, 'number_of_stuff_min_1h': 7,
         'number_of_stuff_min_2h': 5, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 9, 'number_of_stuff_max_2h': 9,
         'number_of_stuff_max_24h': 9, 'number_of_stuff_sqr_1h': 194, 'number_of_stuff_sqr_2h': 255, 'number_of_stuff_sqr_24h': 285,
         'number_of_stuff_avg_1h': 8.0, 'number_of_stuff_avg_2h': 7.0, 'number_of_stuff_avg_24h': 4.5, 'col1': 9}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    other_table = Table(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        QueryByKey(["number_of_stuff_sum_1h", "number_of_stuff_sum_2h", "number_of_stuff_sum_24h",
                    "number_of_stuff_avg_1h", "number_of_stuff_avg_2h", "number_of_stuff_avg_24h",
                    "number_of_stuff_min_1h", "number_of_stuff_min_2h", "number_of_stuff_min_24h",
                    "number_of_stuff_max_1h", "number_of_stuff_max_2h", "number_of_stuff_max_24h"],
                   other_table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 10, 'number_of_stuff_sum_1h': 17, 'number_of_stuff_sum_2h': 30, 'number_of_stuff_sum_24h': 45,
         'number_of_stuff_min_1h': 8, 'number_of_stuff_min_2h': 6, 'number_of_stuff_min_24h': 0,
         'number_of_stuff_max_1h': 9, 'number_of_stuff_max_2h': 9, 'number_of_stuff_max_24h': 9,
         'number_of_stuff_avg_1h': 8.5, 'number_of_stuff_avg_2h': 7.5, 'number_of_stuff_avg_24h': 4.5}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


@pytest.mark.parametrize('query_aggregations', [["number_of_stuff_sum_1h", "number_of_stuff_avg_2h"],
                                                ["number_of_stuff_avg_2h", "number_of_stuff_sum_1h"]])
def test_aggregate_and_query_with_dependent_aggrs_different_windows(setup_teardown_test, query_aggregations):
    table = Table(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                        SlidingWindows(['1h', '2h'], '10m'))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0,
         'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0, 'col1': 0},
        {'number_of_stuff_sum_1h': 1, 'number_of_stuff_sum_2h': 1,
         'number_of_stuff_avg_1h': 0.5, 'number_of_stuff_avg_2h': 0.5, 'col1': 1},
        {'number_of_stuff_sum_1h': 3, 'number_of_stuff_sum_2h': 3,
         'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0, 'col1': 2},
        {'number_of_stuff_sum_1h': 6, 'number_of_stuff_sum_2h': 6,
         'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 1.5, 'col1': 3},
        {'number_of_stuff_sum_1h': 9, 'number_of_stuff_sum_2h': 10,
         'number_of_stuff_avg_1h': 3.0, 'number_of_stuff_avg_2h': 2.0, 'col1': 4},
        {'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 15,
         'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 3.0, 'col1': 5},
        {'number_of_stuff_sum_1h': 15, 'number_of_stuff_sum_2h': 20,
         'number_of_stuff_avg_1h': 5.0, 'number_of_stuff_avg_2h': 4.0, 'col1': 6},
        {'number_of_stuff_sum_1h': 18, 'number_of_stuff_sum_2h': 25,
         'number_of_stuff_avg_1h': 6.0, 'number_of_stuff_avg_2h': 5.0, 'col1': 7},
        {'number_of_stuff_sum_1h': 21, 'number_of_stuff_sum_2h': 30,
         'number_of_stuff_avg_1h': 7.0, 'number_of_stuff_avg_2h': 6.0, 'col1': 8},
        {'number_of_stuff_sum_1h': 24, 'number_of_stuff_sum_2h': 35,
         'number_of_stuff_avg_1h': 8.0, 'number_of_stuff_avg_2h': 7.0, 'col1': 9}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    other_table = Table(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        QueryByKey(query_aggregations,
                   other_table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 10, 'number_of_stuff_sum_1h': 17, 'number_of_stuff_avg_2h': 7.5}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


@pytest.mark.parametrize('partitioned_by_key', [True, False])
def test_aggregate_by_key_one_underlying_window(setup_teardown_test, partitioned_by_key):
    expected = {1: [{'number_of_stuff_count_1h': 1, 'other_stuff_sum_1h': 0.0, 'col1': 0},
                    {'number_of_stuff_count_1h': 2, 'other_stuff_sum_1h': 1.0, 'col1': 1},
                    {'number_of_stuff_count_1h': 3, 'other_stuff_sum_1h': 3.0, 'col1': 2}],
                2: [{'number_of_stuff_count_1h': 4, 'other_stuff_sum_1h': 6.0, 'col1': 3},
                    {'number_of_stuff_count_1h': 5, 'other_stuff_sum_1h': 10.0, 'col1': 4},
                    {'number_of_stuff_count_1h': 6, 'other_stuff_sum_1h': 15.0, 'col1': 5}],
                3: [{'number_of_stuff_count_1h': 7, 'other_stuff_sum_1h': 21.0, 'col1': 6},
                    {'number_of_stuff_count_1h': 8, 'other_stuff_sum_1h': 28.0, 'col1': 7},
                    {'number_of_stuff_count_1h': 9, 'other_stuff_sum_1h': 36.0, 'col1': 8}]}

    items_in_ingest_batch = 3
    current_index = 0

    for current_expected in expected.values():

        table = Table(setup_teardown_test, V3ioDriver(), partitioned_by_key=partitioned_by_key)
        controller = build_flow([
            Source(),
            AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["count"],
                                            SlidingWindows(['1h'], '10m')),
                            FieldAggregator("other_stuff", "col1", ["sum"],
                                            SlidingWindows(['1h'], '10m'))],
                           table),
            WriteToTable(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]).run()

        for i in range(items_in_ingest_batch):
            data = {'col1': current_index}
            controller.emit(data, 'tal', test_base_time + timedelta(minutes=1 * current_index))
            current_index = current_index + 1

        controller.terminate()
        actual = controller.await_termination()

        assert actual == current_expected, \
            f'actual did not match expected. \n actual: {actual} \n expected: {current_expected}'


@pytest.mark.parametrize('partitioned_by_key', [True, False])
def test_aggregate_by_key_two_underlying_windows(setup_teardown_test, partitioned_by_key):
    expected = {1: [{'number_of_stuff_count_24h': 1, 'other_stuff_sum_24h': 0.0, 'col1': 0},
                    {'number_of_stuff_count_24h': 2, 'other_stuff_sum_24h': 1.0, 'col1': 1},
                    {'number_of_stuff_count_24h': 3, 'other_stuff_sum_24h': 3.0, 'col1': 2}],
                2: [{'number_of_stuff_count_24h': 4, 'other_stuff_sum_24h': 6.0, 'col1': 3},
                    {'number_of_stuff_count_24h': 5, 'other_stuff_sum_24h': 10.0, 'col1': 4},
                    {'number_of_stuff_count_24h': 6, 'other_stuff_sum_24h': 15.0, 'col1': 5}],
                3: [{'number_of_stuff_count_24h': 7, 'other_stuff_sum_24h': 21.0, 'col1': 6},
                    {'number_of_stuff_count_24h': 8, 'other_stuff_sum_24h': 28.0, 'col1': 7},
                    {'number_of_stuff_count_24h': 9, 'other_stuff_sum_24h': 36.0, 'col1': 8}]}

    items_in_ingest_batch = 3
    current_index = 0
    for current_expected in expected.values():

        table = Table(setup_teardown_test, V3ioDriver(), partitioned_by_key=partitioned_by_key)
        controller = build_flow([
            Source(),
            AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["count"],
                                            SlidingWindows(['24h'], '10m')),
                            FieldAggregator("other_stuff", "col1", ["sum"],
                                            SlidingWindows(['24h'], '10m'))],
                           table),
            WriteToTable(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]).run()

        for i in range(items_in_ingest_batch):
            data = {'col1': current_index}
            controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * current_index))
            current_index = current_index + 1

        controller.terminate()
        actual = controller.await_termination()

        assert actual == current_expected, \
            f'actual did not match expected. \n actual: {actual} \n expected: {current_expected}'


def test_aggregate_by_key_with_extra_aliases(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())

    table['tal'] = {'color': 'blue', 'age': 41, 'iss': True, 'sometime': test_base_time}

    def enrich(event, state):
        if 'first_activity' not in state:
            state['first_activity'] = event.time

        event.body['time_since_activity'] = (event.time - state['first_activity']).seconds
        state['last_event'] = event.time
        event.body['total_activities'] = state['total_activities'] = state.get('total_activities', 0) + 1
        event.body['color'] = state['color']
        return event, state

    controller = build_flow([
        Source(),
        MapWithState(table, enrich, group_by_key=True, full_event=True),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                        SlidingWindows(['2h'], '10m'))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'number_of_stuff_sum_2h': 0, 'number_of_stuff_avg_2h': 0.0, 'col1': 0, 'time_since_activity': 0, 'total_activities': 1,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 1, 'number_of_stuff_avg_2h': 0.5, 'col1': 1, 'time_since_activity': 1500, 'total_activities': 2,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 3, 'number_of_stuff_avg_2h': 1.0, 'col1': 2, 'time_since_activity': 3000, 'total_activities': 3,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 6, 'number_of_stuff_avg_2h': 1.5, 'col1': 3, 'time_since_activity': 4500, 'total_activities': 4,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 10, 'number_of_stuff_avg_2h': 2.0, 'col1': 4, 'time_since_activity': 6000, 'total_activities': 5,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 15, 'number_of_stuff_avg_2h': 3.0, 'col1': 5, 'time_since_activity': 7500, 'total_activities': 6,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 20, 'number_of_stuff_avg_2h': 4.0, 'col1': 6, 'time_since_activity': 9000, 'total_activities': 7,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 25, 'number_of_stuff_avg_2h': 5.0, 'col1': 7, 'time_since_activity': 10500, 'total_activities': 8,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 30, 'number_of_stuff_avg_2h': 6.0, 'col1': 8, 'time_since_activity': 12000, 'total_activities': 9,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 35, 'number_of_stuff_avg_2h': 7.0, 'col1': 9, 'time_since_activity': 13500, 'total_activities': 10,
         'color': 'blue'}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    other_table = Table(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        QueryByKey(['number_of_stuff_sum_2h', 'number_of_stuff_avg_2h', 'color', 'age', 'iss', 'sometime'],
                   other_table, aliases={'color': 'external.color', 'iss': 'external.iss'}),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'number_of_stuff_sum_2h': 30, 'number_of_stuff_avg_2h': 7.5, 'col1': 10, 'external.color': 'blue', 'age': 41, 'external.iss': True,
         'sometime': test_base_time}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_write_cache_with_aggregations(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())

    table['tal'] = {'color': 'blue', 'age': 41, 'iss': True, 'sometime': test_base_time}

    def enrich(event, state):
        if 'first_activity' not in state:
            state['first_activity'] = event.time

        event.body['time_since_activity'] = (event.time - state['first_activity']).seconds
        state['last_event'] = event.time
        event.body['total_activities'] = state['total_activities'] = state.get('total_activities', 0) + 1
        event.body['color'] = state['color']
        return event, state

    controller = build_flow([
        Source(),
        MapWithState(table, enrich, group_by_key=True, full_event=True),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                        SlidingWindows(['2h'], '10m'))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'number_of_stuff_sum_2h': 0, 'number_of_stuff_avg_2h': 0.0, 'col1': 0, 'time_since_activity': 0, 'total_activities': 1,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 1, 'number_of_stuff_avg_2h': 0.5, 'col1': 1, 'time_since_activity': 1500, 'total_activities': 2,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 3, 'number_of_stuff_avg_2h': 1.0, 'col1': 2, 'time_since_activity': 3000, 'total_activities': 3,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 6, 'number_of_stuff_avg_2h': 1.5, 'col1': 3, 'time_since_activity': 4500, 'total_activities': 4,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 10, 'number_of_stuff_avg_2h': 2.0, 'col1': 4, 'time_since_activity': 6000, 'total_activities': 5,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 15, 'number_of_stuff_avg_2h': 3.0, 'col1': 5, 'time_since_activity': 7500, 'total_activities': 6,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 20, 'number_of_stuff_avg_2h': 4.0, 'col1': 6, 'time_since_activity': 9000, 'total_activities': 7,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 25, 'number_of_stuff_avg_2h': 5.0, 'col1': 7, 'time_since_activity': 10500, 'total_activities': 8,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 30, 'number_of_stuff_avg_2h': 6.0, 'col1': 8, 'time_since_activity': 12000, 'total_activities': 9,
         'color': 'blue'},
        {'number_of_stuff_sum_2h': 35, 'number_of_stuff_avg_2h': 7.0, 'col1': 9, 'time_since_activity': 13500, 'total_activities': 10,
         'color': 'blue'}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    other_table = Table(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        QueryByKey(["number_of_stuff_sum_2h", "number_of_stuff_avg_2h", 'color', 'age', 'iss', 'sometime'],
                   other_table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'number_of_stuff_sum_2h': 30, 'number_of_stuff_avg_2h': 7.5, 'col1': 10, 'color': 'blue', 'age': 41, 'iss': True,
         'sometime': test_base_time}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_write_cache(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())

    table['tal'] = {'color': 'blue', 'age': 41, 'iss': True, 'sometime': datetime.now()}

    def enrich(event, state):
        if 'first_activity' not in state:
            state['first_activity'] = event.time

        event.body['time_since_activity'] = (event.time - state['first_activity']).seconds
        state['last_event'] = event.time
        event.body['total_activities'] = state['total_activities'] = state.get('total_activities', 0) + 1
        event.body['color'] = state['color']
        return event, state

    controller = build_flow([
        Source(),
        MapWithState(table, enrich, group_by_key=True, full_event=True),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'time_since_activity': 0, 'total_activities': 1, 'color': 'blue'},
        {'col1': 1, 'time_since_activity': 1500, 'total_activities': 2, 'color': 'blue'},
        {'col1': 2, 'time_since_activity': 3000, 'total_activities': 3, 'color': 'blue'},
        {'col1': 3, 'time_since_activity': 4500, 'total_activities': 4, 'color': 'blue'},
        {'col1': 4, 'time_since_activity': 6000, 'total_activities': 5, 'color': 'blue'},
        {'col1': 5, 'time_since_activity': 7500, 'total_activities': 6, 'color': 'blue'},
        {'col1': 6, 'time_since_activity': 9000, 'total_activities': 7, 'color': 'blue'},
        {'col1': 7, 'time_since_activity': 10500, 'total_activities': 8, 'color': 'blue'},
        {'col1': 8, 'time_since_activity': 12000, 'total_activities': 9, 'color': 'blue'},
        {'col1': 9, 'time_since_activity': 13500, 'total_activities': 10, 'color': 'blue'}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    other_table = Table(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        MapWithState(other_table, enrich, group_by_key=True, full_event=True),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 10, 'time_since_activity': 15000, 'total_activities': 11, 'color': 'blue'}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_aggregate_with_string_table(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())
    table_name = 'tals-table'
    context = Context(initial_tables={table_name: table})
    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max", "sqr"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       table_name, context=context),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'number_of_stuff_sum_1h': 0, 'number_of_stuff_sum_2h': 0, 'number_of_stuff_sum_24h': 0, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 0, 'number_of_stuff_max_2h': 0,
         'number_of_stuff_max_24h': 0, 'number_of_stuff_sqr_1h': 0, 'number_of_stuff_sqr_2h': 0, 'number_of_stuff_sqr_24h': 0,
         'number_of_stuff_avg_1h': 0.0, 'number_of_stuff_avg_2h': 0.0, 'number_of_stuff_avg_24h': 0.0, 'col1': 0},
        {'number_of_stuff_sum_1h': 1, 'number_of_stuff_sum_2h': 1, 'number_of_stuff_sum_24h': 1, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 1, 'number_of_stuff_max_2h': 1,
         'number_of_stuff_max_24h': 1, 'number_of_stuff_sqr_1h': 1, 'number_of_stuff_sqr_2h': 1, 'number_of_stuff_sqr_24h': 1,
         'number_of_stuff_avg_1h': 0.5, 'number_of_stuff_avg_2h': 0.5, 'number_of_stuff_avg_24h': 0.5, 'col1': 1},
        {'number_of_stuff_sum_1h': 3, 'number_of_stuff_sum_2h': 3, 'number_of_stuff_sum_24h': 3, 'number_of_stuff_min_1h': 0,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 2, 'number_of_stuff_max_2h': 2,
         'number_of_stuff_max_24h': 2, 'number_of_stuff_sqr_1h': 5, 'number_of_stuff_sqr_2h': 5, 'number_of_stuff_sqr_24h': 5,
         'number_of_stuff_avg_1h': 1.0, 'number_of_stuff_avg_2h': 1.0, 'number_of_stuff_avg_24h': 1.0, 'col1': 2},
        {'number_of_stuff_sum_1h': 6, 'number_of_stuff_sum_2h': 6, 'number_of_stuff_sum_24h': 6, 'number_of_stuff_min_1h': 1,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 3, 'number_of_stuff_max_2h': 3,
         'number_of_stuff_max_24h': 3, 'number_of_stuff_sqr_1h': 14, 'number_of_stuff_sqr_2h': 14, 'number_of_stuff_sqr_24h': 14,
         'number_of_stuff_avg_1h': 2.0, 'number_of_stuff_avg_2h': 1.5, 'number_of_stuff_avg_24h': 1.5, 'col1': 3},
        {'number_of_stuff_sum_1h': 9, 'number_of_stuff_sum_2h': 10, 'number_of_stuff_sum_24h': 10, 'number_of_stuff_min_1h': 2,
         'number_of_stuff_min_2h': 0, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 4, 'number_of_stuff_max_2h': 4,
         'number_of_stuff_max_24h': 4, 'number_of_stuff_sqr_1h': 29, 'number_of_stuff_sqr_2h': 30, 'number_of_stuff_sqr_24h': 30,
         'number_of_stuff_avg_1h': 3.0, 'number_of_stuff_avg_2h': 2.0, 'number_of_stuff_avg_24h': 2.0, 'col1': 4},
        {'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 15, 'number_of_stuff_sum_24h': 15, 'number_of_stuff_min_1h': 3,
         'number_of_stuff_min_2h': 1, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 5, 'number_of_stuff_max_2h': 5,
         'number_of_stuff_max_24h': 5, 'number_of_stuff_sqr_1h': 50, 'number_of_stuff_sqr_2h': 55, 'number_of_stuff_sqr_24h': 55,
         'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 3.0, 'number_of_stuff_avg_24h': 2.5, 'col1': 5},
        {'number_of_stuff_sum_1h': 15, 'number_of_stuff_sum_2h': 20, 'number_of_stuff_sum_24h': 21, 'number_of_stuff_min_1h': 4,
         'number_of_stuff_min_2h': 2, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 6, 'number_of_stuff_max_2h': 6,
         'number_of_stuff_max_24h': 6, 'number_of_stuff_sqr_1h': 77, 'number_of_stuff_sqr_2h': 90, 'number_of_stuff_sqr_24h': 91,
         'number_of_stuff_avg_1h': 5.0, 'number_of_stuff_avg_2h': 4.0, 'number_of_stuff_avg_24h': 3.0, 'col1': 6},
        {'number_of_stuff_sum_1h': 18, 'number_of_stuff_sum_2h': 25, 'number_of_stuff_sum_24h': 28, 'number_of_stuff_min_1h': 5,
         'number_of_stuff_min_2h': 3, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 7, 'number_of_stuff_max_2h': 7,
         'number_of_stuff_max_24h': 7, 'number_of_stuff_sqr_1h': 110, 'number_of_stuff_sqr_2h': 135, 'number_of_stuff_sqr_24h': 140,
         'number_of_stuff_avg_1h': 6.0, 'number_of_stuff_avg_2h': 5.0, 'number_of_stuff_avg_24h': 3.5, 'col1': 7},
        {'number_of_stuff_sum_1h': 21, 'number_of_stuff_sum_2h': 30, 'number_of_stuff_sum_24h': 36, 'number_of_stuff_min_1h': 6,
         'number_of_stuff_min_2h': 4, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 8, 'number_of_stuff_max_2h': 8,
         'number_of_stuff_max_24h': 8, 'number_of_stuff_sqr_1h': 149, 'number_of_stuff_sqr_2h': 190, 'number_of_stuff_sqr_24h': 204,
         'number_of_stuff_avg_1h': 7.0, 'number_of_stuff_avg_2h': 6.0, 'number_of_stuff_avg_24h': 4.0, 'col1': 8},
        {'number_of_stuff_sum_1h': 24, 'number_of_stuff_sum_2h': 35, 'number_of_stuff_sum_24h': 45, 'number_of_stuff_min_1h': 7,
         'number_of_stuff_min_2h': 5, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 9, 'number_of_stuff_max_2h': 9,
         'number_of_stuff_max_24h': 9, 'number_of_stuff_sqr_1h': 194, 'number_of_stuff_sqr_2h': 255, 'number_of_stuff_sqr_24h': 285,
         'number_of_stuff_avg_1h': 8.0, 'number_of_stuff_avg_2h': 7.0, 'number_of_stuff_avg_24h': 4.5, 'col1': 9}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def _assert_schema_equal(actual, expected):
    assert len(actual) == len(expected)
    for key, item in actual.items():
        current_expected = expected[key]
        assert item['period_millis'] == current_expected['period_millis']
        assert set(item['aggregates']) == set(current_expected['aggregates'])


async def load_schema(path):
    driver = V3ioDriver()
    container, table_path = _split_path(path)
    res = await driver._load_schema(container, table_path)
    await driver.close()
    return res


def test_modify_schema(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
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
        {'col1': 5, 'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 15, 'number_of_stuff_sum_24h': 15,
         'number_of_stuff_min_1h': 3, 'number_of_stuff_min_2h': 1, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 5,
         'number_of_stuff_max_2h': 5, 'number_of_stuff_max_24h': 5, 'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 3.0,
         'number_of_stuff_avg_24h': 2.5},
        {'col1': 6, 'number_of_stuff_sum_1h': 15, 'number_of_stuff_sum_2h': 20, 'number_of_stuff_sum_24h': 21,
         'number_of_stuff_min_1h': 4, 'number_of_stuff_min_2h': 2, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 6,
         'number_of_stuff_max_2h': 6, 'number_of_stuff_max_24h': 6, 'number_of_stuff_avg_1h': 5.0, 'number_of_stuff_avg_2h': 4.0,
         'number_of_stuff_avg_24h': 3.0},
        {'col1': 7, 'number_of_stuff_sum_1h': 18, 'number_of_stuff_sum_2h': 25, 'number_of_stuff_sum_24h': 28,
         'number_of_stuff_min_1h': 5, 'number_of_stuff_min_2h': 3, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 7,
         'number_of_stuff_max_2h': 7, 'number_of_stuff_max_24h': 7, 'number_of_stuff_avg_1h': 6.0, 'number_of_stuff_avg_2h': 5.0,
         'number_of_stuff_avg_24h': 3.5},
        {'col1': 8, 'number_of_stuff_sum_1h': 21, 'number_of_stuff_sum_2h': 30, 'number_of_stuff_sum_24h': 36,
         'number_of_stuff_min_1h': 6, 'number_of_stuff_min_2h': 4, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 8,
         'number_of_stuff_max_2h': 8, 'number_of_stuff_max_24h': 8, 'number_of_stuff_avg_1h': 7.0, 'number_of_stuff_avg_2h': 6.0,
         'number_of_stuff_avg_24h': 4.0},
        {'col1': 9, 'number_of_stuff_sum_1h': 24, 'number_of_stuff_sum_2h': 35, 'number_of_stuff_sum_24h': 45,
         'number_of_stuff_min_1h': 7, 'number_of_stuff_min_2h': 5, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 9,
         'number_of_stuff_max_2h': 9, 'number_of_stuff_max_24h': 9, 'number_of_stuff_avg_1h': 8.0, 'number_of_stuff_avg_2h': 7.0,
         'number_of_stuff_avg_24h': 4.5}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    schema = asyncio.run(load_schema(setup_teardown_test))
    expected_schema = {"number_of_stuff": {"period_millis": 600000, "aggregates": ['max', 'min', 'sum', 'count']}}
    _assert_schema_equal(schema, expected_schema)

    other_table = Table(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m')),
                        FieldAggregator("new_aggr", "col1", ["min", "max"],
                                        SlidingWindows(['3h'], '10m'))],
                       other_table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 10, 'number_of_stuff_sum_1h': 27, 'number_of_stuff_sum_2h': 40, 'number_of_stuff_sum_24h': 55, 'number_of_stuff_min_1h': 8,
         'number_of_stuff_min_2h': 6, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 10, 'number_of_stuff_max_2h': 10,
         'number_of_stuff_max_24h': 10, 'new_aggr_min_3h': 10, 'new_aggr_max_3h': 10, 'number_of_stuff_avg_1h': 9.0,
         'number_of_stuff_avg_2h': 8.0, 'number_of_stuff_avg_24h': 5.0, 'col1': 10}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    schema = asyncio.run(load_schema(setup_teardown_test))
    expected_schema = {"number_of_stuff": {"period_millis": 600000, "aggregates": ["sum", "max", "min", "count"]},
                       "new_aggr": {"period_millis": 600000, "aggregates": ["min", "max"]}}
    _assert_schema_equal(schema, expected_schema)


def test_invalid_modify_schema(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
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
        {'col1': 5, 'number_of_stuff_sum_1h': 12, 'number_of_stuff_sum_2h': 15, 'number_of_stuff_sum_24h': 15,
         'number_of_stuff_min_1h': 3, 'number_of_stuff_min_2h': 1, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 5,
         'number_of_stuff_max_2h': 5, 'number_of_stuff_max_24h': 5, 'number_of_stuff_avg_1h': 4.0, 'number_of_stuff_avg_2h': 3.0,
         'number_of_stuff_avg_24h': 2.5},
        {'col1': 6, 'number_of_stuff_sum_1h': 15, 'number_of_stuff_sum_2h': 20, 'number_of_stuff_sum_24h': 21,
         'number_of_stuff_min_1h': 4, 'number_of_stuff_min_2h': 2, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 6,
         'number_of_stuff_max_2h': 6, 'number_of_stuff_max_24h': 6, 'number_of_stuff_avg_1h': 5.0, 'number_of_stuff_avg_2h': 4.0,
         'number_of_stuff_avg_24h': 3.0},
        {'col1': 7, 'number_of_stuff_sum_1h': 18, 'number_of_stuff_sum_2h': 25, 'number_of_stuff_sum_24h': 28,
         'number_of_stuff_min_1h': 5, 'number_of_stuff_min_2h': 3, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 7,
         'number_of_stuff_max_2h': 7, 'number_of_stuff_max_24h': 7, 'number_of_stuff_avg_1h': 6.0, 'number_of_stuff_avg_2h': 5.0,
         'number_of_stuff_avg_24h': 3.5},
        {'col1': 8, 'number_of_stuff_sum_1h': 21, 'number_of_stuff_sum_2h': 30, 'number_of_stuff_sum_24h': 36,
         'number_of_stuff_min_1h': 6, 'number_of_stuff_min_2h': 4, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 8,
         'number_of_stuff_max_2h': 8, 'number_of_stuff_max_24h': 8, 'number_of_stuff_avg_1h': 7.0, 'number_of_stuff_avg_2h': 6.0,
         'number_of_stuff_avg_24h': 4.0},
        {'col1': 9, 'number_of_stuff_sum_1h': 24, 'number_of_stuff_sum_2h': 35, 'number_of_stuff_sum_24h': 45,
         'number_of_stuff_min_1h': 7, 'number_of_stuff_min_2h': 5, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 9,
         'number_of_stuff_max_2h': 9, 'number_of_stuff_max_24h': 9, 'number_of_stuff_avg_1h': 8.0, 'number_of_stuff_avg_2h': 7.0,
         'number_of_stuff_avg_24h': 4.5}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    schema = asyncio.run(load_schema(setup_teardown_test))
    expected_schema = {"number_of_stuff": {"period_millis": 600000, "aggregates": ['max', 'min', 'sum', 'count']}}
    _assert_schema_equal(schema, expected_schema)

    other_table = Table(setup_teardown_test, V3ioDriver())

    try:
        controller = build_flow([
            Source(),
            AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                            SlidingWindows(['1h', '24h'], '3m'))],
                           other_table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]).run()

        base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
        data = {'col1': items_in_ingest_batch}
        controller.emit(data, 'tal', base_time)

        controller.terminate()
        controller.await_termination()
    except ValueError:
        pass


def test_query_aggregate_by_key_sliding_window_new_time_exceeds_stored_window(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["count"],
                                        SlidingWindows(['30m', '2h'], '1m'))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(hours=i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 1},
        {'col1': 1, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 2},
        {'col1': 2, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 2},
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    other_table = Table(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        QueryByKey(["number_of_stuff_count_30m", "number_of_stuff_count_2h"],
                   other_table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(hours=items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 3, 'number_of_stuff_count_30m': 0, 'number_of_stuff_count_2h': 1}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_query_aggregate_by_key_fixed_window_new_time_exceeds_stored_window(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["count"],
                                        FixedWindows(['30m', '2h']))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=45 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 1},
        {'col1': 1, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 2},
        {'col1': 2, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 3},
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    other_table = Table(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        QueryByKey(["number_of_stuff_count_30m", "number_of_stuff_count_2h"],
                   other_table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(hours=items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 3, 'number_of_stuff_count_30m': 0, 'number_of_stuff_count_2h': 1}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_sliding_query_time_exceeds_stored_window_by_more_than_window(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["count"],
                                        SlidingWindows(['30m', '2h'], '1m'))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(hours=i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 1},
        {'col1': 1, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 2},
        {'col1': 2, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 2},
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    other_table = Table(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        QueryByKey(["number_of_stuff_count_30m", "number_of_stuff_count_2h"],
                   other_table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(days=10)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 3, 'number_of_stuff_count_30m': 0, 'number_of_stuff_count_2h': 0}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_fixed_query_time_exceeds_stored_window_by_more_than_window(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["count"],
                                        FixedWindows(['30m', '2h']))],
                       table),
        WriteToTable(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=45 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 1},
        {'col1': 1, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 2},
        {'col1': 2, 'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 3},
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    other_table = Table(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        QueryByKey(["number_of_stuff_count_30m", "number_of_stuff_count_2h"],
                   other_table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(days=10)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 3, 'number_of_stuff_count_30m': 0, 'number_of_stuff_count_2h': 0}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'


def test_write_to_table_reuse(setup_teardown_test):
    table = Table(setup_teardown_test, V3ioDriver())
    flow = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["count"], FixedWindows(['30m', '2h']))], table),
        WriteToTable(table), Reduce([], lambda acc, x: append_return(acc, x))
    ])
    items_in_ingest_batch = 3

    expected_results = [
        [{'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 1, 'col1': 0},
         {'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 2, 'col1': 1},
         {'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 3, 'col1': 2}],
        [{'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 1, 'col1': 0},
         {'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 2, 'col1': 1},
         {'number_of_stuff_count_30m': 1, 'number_of_stuff_count_2h': 3, 'col1': 2},
         {'number_of_stuff_count_30m': 2, 'number_of_stuff_count_2h': 2, 'col1': 0},
         {'number_of_stuff_count_30m': 2, 'number_of_stuff_count_2h': 4, 'col1': 1},
         {'number_of_stuff_count_30m': 2, 'number_of_stuff_count_2h': 6, 'col1': 2}],
    ]

    for iteration in range(2):
        controller = flow.run()
        for i in range(items_in_ingest_batch):
            data = {'col1': i}
            controller.emit(data, 'tal', test_base_time + timedelta(minutes=45 * i))

        controller.terminate()
        actual = controller.await_termination()
        assert actual == expected_results[iteration]
