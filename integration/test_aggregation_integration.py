import asyncio
from datetime import datetime, timedelta

import pytest

from storey import build_flow, Source, Reduce, Cache, V3ioDriver, FlowError, MapWithState, AggregateByKey, FieldAggregator, \
    QueryAggregationByKey, Persist
from storey.dtypes import SlidingWindows
from storey.flow import _split_path

from .integration_test_utils import setup_teardown_test

test_base_time = datetime.fromisoformat("2020-07-21T21:40:00+00:00")


def append_return(lst, x):
    lst.append(x)
    return lst


@pytest.mark.parametrize('partitioned_by_key', [True, False])
def test_query_aggregate_by_key(setup_teardown_test, partitioned_by_key):
    cache = Cache(setup_teardown_test, V3ioDriver(), partitioned_by_key=partitioned_by_key)

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max", "sqr"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       cache),
        Persist(cache),
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

    other_cache = Cache(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        QueryAggregationByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                               SlidingWindows(['1h', '2h', '24h'], '10m'))],
                              other_cache),
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


def test_write_cache(setup_teardown_test):
    cache = Cache(setup_teardown_test, V3ioDriver())

    cache._cache['tal'] = {'color': 'blue', 'age': 41, 'iss': True, 'sometime': datetime.now()}

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
        MapWithState(cache, enrich, group_by_key=True, full_event=True),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                        SlidingWindows(['2h'], '10m'))],
                       cache),
        Persist(cache),
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

    other_cache = Cache(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        MapWithState(cache, enrich, group_by_key=True, full_event=True),
        QueryAggregationByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg"],
                                               SlidingWindows(['2h'], '10m'))],
                              other_cache),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {'col1': items_in_ingest_batch}
    controller.emit(data, 'tal', base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'number_of_stuff_sum_2h': 30, 'number_of_stuff_avg_2h': 7.5, 'col1': 10, 'time_since_activity': 15000, 'total_activities': 11,
         'color': 'blue'}]

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
    cache = Cache(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       cache),
        Persist(cache),
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

    other_cache = Cache(setup_teardown_test, V3ioDriver())
    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m')),
                        FieldAggregator("new_aggr", "col1", ["min", "max"],
                                        SlidingWindows(['3h'], '10m'))],
                       other_cache),
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
    cache = Cache(setup_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       cache),
        Persist(cache),
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

    other_cache = Cache(setup_teardown_test, V3ioDriver())

    try:
        controller = build_flow([
            Source(),
            AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                            SlidingWindows(['1h', '24h'], '3m'))],
                           other_cache),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]).run()

        base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
        data = {'col1': items_in_ingest_batch}
        controller.emit(data, 'tal', base_time)

        controller.terminate()
        controller.await_termination()
    except FlowError as flow_ex:
        assert isinstance(flow_ex.__cause__, ValueError)
