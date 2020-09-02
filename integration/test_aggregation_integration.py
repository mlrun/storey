import asyncio
import random
import string
from datetime import datetime, timedelta

import pytest

from storey import build_flow, Source, Reduce, V3ioTable
from storey.aggregations import AggregateByKey, FieldAggregator, QueryAggregationByKey
from storey.dtypes import SlidingWindows

test_base_time = datetime.fromisoformat("2020-07-21T21:40:00+00:00")


def _generate_table_name(prefix='aggr_test'):
    random_table = ''.join([random.choice(string.ascii_letters) for i in range(10)])
    return f'{prefix}/{random_table}'


def append_return(lst, x):
    lst.append(x)
    return lst


@pytest.fixture()
def setup_teardown_test():
    # Setup
    table_name = _generate_table_name()
    v3io_table = V3ioTable(table_name)

    # Test runs
    yield v3io_table

    # Teardown
    asyncio.run(v3io_table.delete())


def test_query_aggregate_by_key(setup_teardown_test):
    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       setup_teardown_test),
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

    controller = build_flow([
        Source(),
        QueryAggregationByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                               SlidingWindows(['1h', '2h', '24h'], '10m'))],
                              setup_teardown_test),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 10, 'number_of_stuff_sum_1h': 17, 'number_of_stuff_sum_2h': 30, 'number_of_stuff_sum_24h': 45,
         'number_of_stuff_min_1h': 8, 'number_of_stuff_min_2h': 6, 'number_of_stuff_min_24h': 0, 'number_of_stuff_max_1h': 9,
         'number_of_stuff_max_2h': 9, 'number_of_stuff_max_24h': 9, 'number_of_stuff_avg_1h': 8.5, 'number_of_stuff_avg_2h': 7.5,
         'number_of_stuff_avg_24h': 4.5}
    ]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'
