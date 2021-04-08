import asyncio
import math
import os
import queue
import uuid
from datetime import datetime
from random import choice

import pandas as pd
from aiohttp import InvalidURL

from storey import build_flow, Source, Map, Filter, FlatMap, Reduce, MapWithState, ReadCSV, Complete, \
    AsyncSource, Choice, \
    Event, Batch, Table, WriteToCSV, DataframeSource, MapClass, JoinWithTable, ReduceToDataFrame, ToDataFrame, \
    WriteToParquet, QueryByKey, \
    WriteToTSDB, Extend, SendToHttp, HttpRequest, WriteToTable, NoopDriver, Driver, Recover, V3ioDriver, ReadParquet


class ATestException(Exception):
    pass


class RaiseEx:
    _counter = 0

    def __init__(self, raise_on_nth):
        self._raise_after = raise_on_nth

    def raise_ex(self, element):
        self._counter += 1
        if self._counter == self._raise_after:
            raise ATestException("test")
        return element


def test_functional_flow():
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 3),
        FlatMap(lambda x: [x, x * 10]),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    for _ in range(100):
        for i in range(10):
            controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 3300


def test_multiple_upstreams():
    source = Source()
    map1 = Map(lambda x: x + 1)
    map2 = Map(lambda x: x * 10)
    reduce = Reduce(0, lambda x, y: x + y)
    source.to(map1)
    source.to(map2)
    map1.to(reduce)
    map2.to(reduce)
    controller = source.run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 55 + 450


def test_recover():
    def increment_maybe_boom(x):
        inc = x + 1
        if inc == 7:
            raise ValueError('boom')
        return inc

    reduce = Reduce(0, lambda x, y: x + y)
    controller = build_flow([
        Source(),
        Recover({ValueError: reduce}),
        Map(increment_maybe_boom),
        reduce
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 54


def test_csv_reader():
    controller = build_flow([
        ReadCSV('tests/test.csv', header=True),
        FlatMap(lambda x: x),
        Map(lambda x: int(x)),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    termination_result = controller.await_termination()
    assert termination_result == 21


def test_csv_reader_error_on_file_not_found():
    controller = build_flow([
        ReadCSV('tests/idontexist.csv', header=True),
    ]).run()

    try:
        controller.await_termination()
        assert False
    except FileNotFoundError:
        pass


def test_csv_reader_as_dict():
    controller = build_flow([
        ReadCSV('tests/test.csv', header=True, build_dict=True),
        FlatMap(lambda x: [x['n1'], x['n2'], x['n3']]),
        Map(lambda x: int(x)),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    termination_result = controller.await_termination()
    assert termination_result == 21


def append_and_return(lst, x):
    lst.append(x)
    return lst


def test_csv_reader_as_dict_with_key_and_timestamp():
    controller = build_flow([
        ReadCSV('tests/test-with-timestamp.csv', header=True, build_dict=True, key_field='k',
                time_field='t', timestamp_format='%d/%m/%Y %H:%M:%S'),
        Reduce([], append_and_return, full_event=True),
    ]).run()

    termination_result = controller.await_termination()

    assert len(termination_result) == 2
    assert termination_result[0].key == 'm1'
    assert termination_result[0].time == datetime(2020, 2, 15, 2, 0)
    assert termination_result[0].body == {'k': 'm1', 't': datetime(2020, 2, 15, 2, 0), 'v': 8, 'b': True}
    assert termination_result[1].key == 'm2'
    assert termination_result[1].time == datetime(2020, 2, 16, 2, 0)
    assert termination_result[1].body == {'k': 'm2', 't': datetime(2020, 2, 16, 2, 0), 'v': 14, 'b': False}


def test_csv_reader_as_dict_with_compact_timestamp():
    controller = build_flow([
        ReadCSV('tests/test-with-compact-timestamp.csv', header=True, build_dict=True, time_field='t', timestamp_format='%Y%m%d%H'),
        Reduce([], append_and_return, full_event=True),
    ]).run()

    termination_result = controller.await_termination()

    assert len(termination_result) == 2
    assert termination_result[0].key is None
    assert termination_result[0].time == datetime(2020, 2, 15, 2, 0)
    assert termination_result[0].body == {'k': 'm1', 't': datetime(2020, 2, 15, 2, 0), 'v': 8, 'b': True}
    assert termination_result[1].key is None
    assert termination_result[1].time == datetime(2020, 2, 16, 2, 0)
    assert termination_result[1].body == {'k': 'm2', 't': datetime(2020, 2, 16, 2, 0), 'v': 14, 'b': False}


def test_csv_reader_with_key_and_timestamp():
    controller = build_flow([
        ReadCSV('tests/test-with-timestamp.csv', header=True, key_field='k',
                time_field='t', timestamp_format='%d/%m/%Y %H:%M:%S'),
        Reduce([], append_and_return, full_event=True),
    ]).run()

    termination_result = controller.await_termination()

    assert len(termination_result) == 2
    assert termination_result[0].key == 'm1'
    assert termination_result[0].time == datetime(2020, 2, 15, 2, 0)
    assert termination_result[0].body == ['m1', datetime(2020, 2, 15, 2, 0), 8, True]
    assert termination_result[1].key == 'm2'
    assert termination_result[1].time == datetime(2020, 2, 16, 2, 0)
    assert termination_result[1].body == ['m2', datetime(2020, 2, 16, 2, 0), 14, False]


def test_csv_reader_as_dict_no_header():
    controller = build_flow([
        ReadCSV('tests/test-no-header.csv', header=False, build_dict=True),
        FlatMap(lambda x: [x[0], x[1], x[2]]),
        Map(lambda x: int(x)),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    termination_result = controller.await_termination()
    assert termination_result == 21


def test_dataframe_source():
    df = pd.DataFrame([['hello', 1, 1.5], ['world', 2, 2.5]], columns=['string', 'int', 'float'])
    controller = build_flow([
        DataframeSource(df),
        Reduce([], append_and_return),
    ]).run()

    termination_result = controller.await_termination()
    expected = [{'string': 'hello', 'int': 1, 'float': 1.5}, {'string': 'world', 'int': 2, 'float': 2.5}]
    assert termination_result == expected


def test_indexed_dataframe_source():
    df = pd.DataFrame([['hello', 1, 1.5], ['world', 2, 2.5]], columns=['string', 'int', 'float'])
    df.set_index(['string', 'int'], inplace=True)
    controller = build_flow([
        DataframeSource(df),
        Reduce([], append_and_return),
    ]).run()

    termination_result = controller.await_termination()
    expected = [{'string': 'hello', 'int': 1, 'float': 1.5}, {'string': 'world', 'int': 2, 'float': 2.5}]
    assert termination_result == expected


def test_dataframe_source_with_metadata():
    t1 = datetime(2020, 2, 15)
    t2 = datetime(2020, 2, 16)
    df = pd.DataFrame([['key1', t1, 'id1', 1.1], ['key2', t2, 'id2', 2.2]],
                      columns=['my_key', 'my_time', 'my_id', 'my_value'])
    controller = build_flow([
        DataframeSource(df, key_field='my_key', time_field='my_time', id_field='my_id'),
        Reduce([], append_and_return, full_event=True),
    ]).run()

    termination_result = controller.await_termination()
    expected = [
        Event({'my_key': 'key1', 'my_time': t1, 'my_id': 'id1', 'my_value': 1.1}, key='key1', time=t1, id='id1'),
        Event({'my_key': 'key2', 'my_time': t2, 'my_id': 'id2', 'my_value': 2.2}, key='key2', time=t2, id='id2')]
    assert termination_result == expected


async def async_dataframe_source():
    df = pd.DataFrame([['hello', 1, 1.5], ['world', 2, 2.5]], columns=['string', 'int', 'float'])
    controller = await build_flow([
        DataframeSource(df),
        Reduce([], append_and_return),
    ]).run_async()

    termination_result = await controller.await_termination()
    expected = [{'string': 'hello', 'int': 1, 'float': 1.5}, {'string': 'world', 'int': 2, 'float': 2.5}]
    assert termination_result == expected


def test_async_dataframe_source():
    asyncio.run(async_test_async_source())


def test_write_parquet_timestamp_nanosecs(tmpdir):
    out_dir = f'{tmpdir}/test_write_parquet_timestamp_nanosecs/{uuid.uuid4().hex}/'
    columns = ['string', 'timestamp1', 'timestamp2']
    df = pd.DataFrame([['hello', pd.Timestamp('2020-01-26 14:52:37.12325679'), pd.Timestamp('2020-01-26 12:41:37.123456789')],
                       ['world', pd.Timestamp('2018-05-11 13:52:37.333421789'), pd.Timestamp('2020-01-14 14:52:37.987654321')]],
                      columns=columns)
    df.set_index(keys=['timestamp1'], inplace=True)
    controller = build_flow([
        DataframeSource(df),
        WriteToParquet(out_dir, columns=['string', 'timestamp2'], partition_cols=[], index_cols='timestamp1')
    ]).run()
    controller.await_termination()

    controller = build_flow([
        ReadParquet(out_dir), Reduce([], append_and_return),
    ]).run()

    termination_result = controller.await_termination()
    expected = [{'string': 'hello', 'timestamp1': pd.Timestamp('2020-01-26 14:52:37.123256'),
                 'timestamp2': pd.Timestamp('2020-01-26 12:41:37.123456')},
                {'string': 'world', 'timestamp1': pd.Timestamp('2018-05-11 13:52:37.333421'),
                 'timestamp2': pd.Timestamp('2020-01-14 14:52:37.987654')}]
    assert termination_result == expected


def test_read_parquet():
    controller = build_flow([
        ReadParquet('tests/test.parquet'),
        Reduce([], append_and_return),
    ]).run()

    termination_result = controller.await_termination()
    expected = [{'string': 'hello', 'int': 1, 'float': 1.5}, {'string': 'world', 'int': 2, 'float': 2.5}]
    assert termination_result == expected


def test_read_parquet_files():
    controller = build_flow([
        ReadParquet(['tests/test.parquet', 'tests/test.parquet']),
        Reduce([], append_and_return),
    ]).run()

    termination_result = controller.await_termination()
    expected = [
        {'string': 'hello', 'int': 1, 'float': 1.5}, {'string': 'world', 'int': 2, 'float': 2.5},
        {'string': 'hello', 'int': 1, 'float': 1.5}, {'string': 'world', 'int': 2, 'float': 2.5}
    ]
    assert termination_result == expected


def test_write_parquet_read_parquet(tmpdir):
    out_dir = f'{tmpdir}/test_write_parquet_read_parquet/{uuid.uuid4().hex}/'
    columns = ['my_int', 'my_string']
    controller = build_flow([
        Source(),
        WriteToParquet(out_dir, columns=columns, partition_cols=[])
    ]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f'this is {i}'])
        expected.append({'my_int': i, 'my_string': f'this is {i}'})
    controller.terminate()
    controller.await_termination()

    controller = build_flow([
        ReadParquet(out_dir),
        Reduce([], append_and_return),
    ]).run()
    read_back_result = controller.await_termination()

    assert read_back_result == expected


def test_write_parquet_read_parquet_partitioned(tmpdir):
    out_dir = f'{tmpdir}/test_write_parquet_read_parquet_partitioned/{uuid.uuid4().hex}/'
    columns = ['my_int', 'my_string']
    controller = build_flow([
        Source(),
        WriteToParquet(out_dir, partition_cols='my_int', columns=columns)
    ]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f'this is {i}'])
        expected.append({'my_int': i, 'my_string': f'this is {i}'})
    controller.terminate()
    controller.await_termination()

    controller = build_flow([
        ReadParquet(out_dir),
        Reduce([], append_and_return),
    ]).run()
    read_back_result = controller.await_termination()

    assert read_back_result == expected


def test_error_flow():
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Map(RaiseEx(500).raise_ex),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    try:
        for i in range(1000):
            controller.emit(i)
        controller.terminate()
        controller.await_termination()
        assert False
    except ATestException:
        pass


def test_error_recovery():
    reduce = Reduce(0, lambda acc, x: acc + x)
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Map(RaiseEx(5).raise_ex, recovery_step=reduce),
        reduce,
    ]).run()

    for i in range(10):
        controller.emit(i)

    controller.terminate()
    result = controller.await_termination()
    assert result == 55


def test_set_recovery_step():
    reduce = Reduce(0, lambda acc, x: acc + x)
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Map(RaiseEx(5).raise_ex).set_recovery_step(reduce),
        reduce,
    ]).run()

    for i in range(10):
        controller.emit(i)

    controller.terminate()
    result = controller.await_termination()
    assert result == 55


def test_error_specific_recovery():
    reduce = Reduce(0, lambda acc, x: acc + x)
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Map(RaiseEx(5).raise_ex, recovery_step={ATestException: reduce}),
        reduce,
    ]).run()

    for i in range(10):
        controller.emit(i)

    controller.terminate()
    result = controller.await_termination()
    assert result == 55


def test_error_specific_recovery_check_exception():
    reduce = Reduce([], lambda acc, event: append_and_return(acc, type(event.error)), full_event=True)
    controller = build_flow([
        Source(),
        Map(RaiseEx(2).raise_ex, recovery_step={ATestException: reduce}),
        reduce
    ]).run()

    for i in range(3):
        controller.emit(i)

    controller.terminate()
    result = controller.await_termination()
    assert result == [type(None), ATestException, type(None)]


def test_error_nonrecovery():
    reduce = Reduce(0, lambda acc, x: acc + x)
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Map(RaiseEx(5).raise_ex, recovery_step={ValueError: reduce}),
        reduce,
    ]).run()

    try:
        for i in range(10):
            controller.emit(i)
        controller.terminate()
        controller.await_termination()
        assert False
    except ATestException:
        pass


def test_error_recovery_containment():
    reduce = Reduce(0, lambda acc, x: acc + x)
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1, recovery_step=reduce),
        Map(RaiseEx(5).raise_ex),
        reduce,
    ]).run()

    try:
        for i in range(10):
            controller.emit(i)
        controller.terminate()
        controller.await_termination()
        assert False
    except ATestException:
        pass


def test_broadcast():
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 3, termination_result_fn=lambda x, y: x + y),
        [
            Reduce(0, lambda acc, x: acc + x)
        ],
        [
            Reduce(0, lambda acc, x: acc + x)
        ]
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 6


def test_broadcast_complex():
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 3, termination_result_fn=lambda x, y: x + y),
        [
            Reduce(0, lambda acc, x: acc + x),
        ],
        [
            Map(lambda x: x * 100),
            Reduce(0, lambda acc, x: acc + x)
        ],
        [
            Map(lambda x: x * 1000),
            Reduce(0, lambda acc, x: acc + x)
        ]
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 3303


# Same as test_broadcast_complex but without using build_flow
def test_broadcast_complex_no_sugar():
    source = Source()
    filter = Filter(lambda x: x < 3, termination_result_fn=lambda x, y: x + y)
    source.to(Map(lambda x: x + 1)).to(filter)
    filter.to(Reduce(0, lambda acc, x: acc + x), )
    filter.to(Map(lambda x: x * 100)).to(Reduce(0, lambda acc, x: acc + x))
    filter.to(Map(lambda x: x * 1000)).to(Reduce(0, lambda acc, x: acc + x))
    controller = source.run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 3303


def test_nested_branching():
    controller = build_flow([
        Source(),
        [
            [
                Reduce(0, lambda acc, x: acc + x)
            ]
        ],
        [
            [
                Map(lambda x: x * 100),
                Reduce(0, lambda acc, x: acc + x)
            ],
            [
                Map(lambda x: x * 1000),
                Reduce(0, lambda acc, x: acc + x)
            ]
        ]
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 45


def test_map_with_state_flow():
    controller = build_flow([
        Source(),
        MapWithState(1000, lambda x, state: (state, x)),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 1036


def test_map_with_cache_state_flow():
    table_object = Table("table", NoopDriver())
    table_object['tal'] = {'color': 'blue'}
    table_object['dina'] = {'color': 'red'}

    def enrich(event, state):
        event['color'] = state['color']
        state['counter'] = state.get('counter', 0) + 1
        return event, state

    controller = build_flow([
        Source(),
        MapWithState(table_object, lambda x, state: enrich(x, state), group_by_key=True),
        Reduce([], append_and_return),
    ]).run()

    for i in range(10):
        key = 'tal'
        if i % 3 == 0:
            key = 'dina'
        controller.emit(Event(body={'col1': i}, key=key))
    controller.terminate()

    termination_result = controller.await_termination()
    expected = [{'col1': 0, 'color': 'red'},
                {'col1': 1, 'color': 'blue'},
                {'col1': 2, 'color': 'blue'},
                {'col1': 3, 'color': 'red'},
                {'col1': 4, 'color': 'blue'},
                {'col1': 5, 'color': 'blue'},
                {'col1': 6, 'color': 'red'},
                {'col1': 7, 'color': 'blue'},
                {'col1': 8, 'color': 'blue'},
                {'col1': 9, 'color': 'red'}]
    expected_cache = {'tal': {'color': 'blue', 'counter': 6}, 'dina': {'color': 'red', 'counter': 4}}

    assert termination_result == expected
    assert len(table_object._attrs_cache) == len(expected_cache)
    assert table_object['tal'] == expected_cache['tal']
    assert table_object['dina'] == expected_cache['dina']


def test_map_with_empty_cache_state_flow():
    table_object = Table("table", NoopDriver())

    def enrich(event, state):
        if 'first_value' not in state:
            state['first_value'] = event['col1']
        event['diff_from_first'] = event['col1'] - state['first_value']
        state['counter'] = state.get('counter', 0) + 1
        return event, state

    controller = build_flow([
        Source(),
        MapWithState(table_object, lambda x, state: enrich(x, state), group_by_key=True),
        Reduce([], append_and_return),
    ]).run()

    for i in range(10):
        key = 'tal'
        if i % 3 == 0:
            key = 'dina'
        controller.emit(Event(body={'col1': i}, key=key))
    controller.terminate()

    termination_result = controller.await_termination()
    expected = [{'col1': 0, 'diff_from_first': 0},
                {'col1': 1, 'diff_from_first': 0},
                {'col1': 2, 'diff_from_first': 1},
                {'col1': 3, 'diff_from_first': 3},
                {'col1': 4, 'diff_from_first': 3},
                {'col1': 5, 'diff_from_first': 4},
                {'col1': 6, 'diff_from_first': 6},
                {'col1': 7, 'diff_from_first': 6},
                {'col1': 8, 'diff_from_first': 7},
                {'col1': 9, 'diff_from_first': 9}]
    expected_cache = {'dina': {'first_value': 0, 'counter': 4}, 'tal': {'first_value': 1, 'counter': 6}}
    assert len(table_object._attrs_cache) == len(expected_cache)
    assert table_object['tal'] == expected_cache['tal']
    assert table_object['dina'] == expected_cache['dina']


def test_awaitable_result():
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1, termination_result_fn=lambda _, x: x),
        [
            Complete()
        ],
        [
            Reduce(0, lambda acc, x: acc + x)
        ]
    ]).run()

    for i in range(10):
        awaitable_result = controller.emit(i)
        assert awaitable_result.await_result() == i + 1
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 55


def test_double_completion():
    controller = build_flow([
        Source(),
        Complete(),
        Complete(),
        Reduce(0, lambda acc, x: acc + x)
    ]).run()

    for i in range(10):
        awaitable_result = controller.emit(i)
        assert awaitable_result.await_result() == i
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 45


async def async_test_async_double_completion():
    controller = await build_flow([
        AsyncSource(),
        Complete(),
        Complete(),
        Reduce(0, lambda acc, x: acc + x)
    ]).run()

    for i in range(10):
        result = await controller.emit(i)
        assert result == i
    await controller.terminate()
    termination_result = await controller.await_termination()
    assert termination_result == 45


def test_async_double_completion():
    asyncio.run(async_test_async_double_completion())


def test_awaitable_result_error():
    def boom(_):
        raise ValueError('boom')

    controller = build_flow([
        Source(),
        Map(boom),
        Complete()
    ]).run()

    awaitable_result = controller.emit(0)
    try:
        awaitable_result.await_result()
        assert False
    except ValueError:
        pass


async def async_test_async_awaitable_result_error():
    def boom(_):
        raise ValueError('boom')

    controller = await build_flow([
        AsyncSource(),
        Map(boom),
        Complete()
    ]).run()

    awaitable_result = controller.emit(0)
    try:
        await awaitable_result
        assert False
    except ValueError:
        pass


def test_async_awaitable_result_error():
    asyncio.run(async_test_async_awaitable_result_error())


async def async_test_async_source():
    controller = await build_flow([
        AsyncSource(),
        Map(lambda x: x + 1, termination_result_fn=lambda _, x: x),
        [
            Complete()
        ],
        [
            Reduce(0, lambda acc, x: acc + x)
        ]
    ]).run()

    for i in range(10):
        result = await controller.emit(i)
        assert result == i + 1
    await controller.terminate()
    termination_result = await controller.await_termination()
    assert termination_result == 55


def test_async_source():
    loop = asyncio.new_event_loop()
    loop.run_until_complete(async_test_async_source())


async def async_test_error_async_flow():
    controller = await build_flow([
        AsyncSource(),
        Map(lambda x: x + 1),
        Map(RaiseEx(5).raise_ex),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    try:
        for i in range(10):
            await controller.emit(i)
    except ATestException:
        pass


def test_awaitable_result_error_in_async_downstream():
    controller = build_flow([
        Source(),
        SendToHttp(lambda _: HttpRequest('GET', 'bad_url', ''), lambda _, response: response.status),
        Complete()
    ]).run()
    try:
        controller.emit(1).await_result()
        assert False
    except InvalidURL:
        pass


async def async_test_async_awaitable_result_error_in_async_downstream():
    controller = await build_flow([
        AsyncSource(),
        SendToHttp(lambda _: HttpRequest('GET', 'bad_url', ''), lambda _, response: response.status),
        Complete()
    ]).run()
    try:
        await controller.emit(1)
        assert False
    except InvalidURL:
        pass


def test_async_awaitable_result_error_in_async_downstream():
    asyncio.run(async_test_async_awaitable_result_error_in_async_downstream())


def test_awaitable_result_error_in_by_key_async_downstream():
    class DriverBoom(Driver):
        async def _save_key(self, container, table_path, key, aggr_item, partitioned_by_key, additional_data):
            raise ValueError('boom')

    controller = build_flow([
        Source(),
        WriteToTable(Table('test', DriverBoom())),
        Complete()
    ]).run()
    try:
        controller.emit({'col1': 0}, 'key').await_result()
        controller.terminate()
        controller.await_termination()
        assert False
    except ValueError:
        pass


def test_error_async_flow():
    loop = asyncio.new_event_loop()
    loop.run_until_complete(async_test_error_async_flow())


def test_choice():
    small_reduce = Reduce(0, lambda acc, x: acc + x)

    big_reduce = build_flow([
        Map(lambda x: x * 100),
        Reduce(0, lambda acc, x: acc + x)
    ])

    controller = build_flow([
        Source(),
        Choice([(big_reduce, lambda x: x % 2 == 0)],
               default=small_reduce,
               termination_result_fn=lambda x, y: x + y)
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 2025


def test_metadata():
    def mapf(x):
        x.key = x.key + 1
        return x

    def redf(acc, x):
        if x.key not in acc:
            acc[x.key] = []
        acc[x.key].append(x.body)
        return acc

    controller = build_flow([
        Source(),
        Map(mapf, full_event=True),
        Reduce({}, redf, full_event=True)
    ]).run()

    for i in range(10):
        controller.emit(Event(i, key=i % 3))
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == {1: [0, 3, 6, 9], 2: [1, 4, 7], 3: [2, 5, 8]}


def test_metadata_immutability():
    def mapf(x):
        x.key = 'new key'
        return x

    controller = build_flow([
        Source(),
        Map(lambda x: 'new body'),
        Map(mapf, full_event=True),
        Complete(full_event=True)
    ]).run()

    event = Event('original body', key='original key')
    result = controller.emit(event).await_result()
    controller.terminate()
    controller.await_termination()

    assert event.key == 'original key'
    assert event.body == 'original body'
    assert result.key == 'new key'
    assert result.body == 'new body'


def test_batch():
    controller = build_flow([
        Source(),
        Batch(4, 100),
        Reduce([], lambda acc, x: append_and_return(acc, x)),
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]


def test_batch_full_event():
    def append_body_and_return(lst, x):
        ll = []
        for item in x:
            ll.append(item.body)
        lst.append(ll)
        return lst

    controller = build_flow([
        Source(),
        Batch(4, 100, full_event=True),
        Reduce([], lambda acc, x: append_body_and_return(acc, x)),
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]


def test_batch_by_user_key():
    def append_and_return(lst, x):
        lst.append(x)
        return lst

    controller = build_flow(
        [
            Source(),
            Batch(2, 100, "value"),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    values_1 = [i for i in range(4)]
    values_2 = [i for i in range(4)]
    values_3 = [i for i in range(4)]
    values_4 = [i for i in range(4)]

    for _ in range(4):
        rand_val_1 = choice(values_1)
        rand_val_2 = choice(values_2)
        rand_val_3 = choice(values_3)
        rand_val_4 = choice(values_4)

        values_1.remove(rand_val_1)
        values_2.remove(rand_val_2)
        values_3.remove(rand_val_3)
        values_4.remove(rand_val_4)

        controller.emit({"value": rand_val_1})
        controller.emit({"value": rand_val_2})
        controller.emit({"value": rand_val_3})
        controller.emit({"value": rand_val_4})

    controller.terminate()
    termination_result = controller.await_termination()

    assert len(termination_result) == 8

    for element in termination_result:
        assert len(element) == 2
        numbers = [e["value"] for e in element]
        assert numbers[0] == numbers[1]


def test_batch_by_event_key():
    def append_and_return(lst, x):
        lst.append(x)
        return lst

    controller = build_flow(
        [
            Source(),
            Batch(5, 100, "$key"),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    controller.emit(1, key="key1")
    controller.emit(2, key="key1")
    controller.emit(3, key="key1")
    controller.emit(4, key="key1")

    controller.emit(8, key="key2")
    controller.emit(9, key="key2")
    controller.emit(10, key="key2")

    controller.emit(5, key="key1")
    controller.emit(6, key="key1")
    controller.emit(7, key="key1")

    controller.terminate()
    termination_result = controller.await_termination()

    assert termination_result[0] == [1, 2, 3, 4, 5]  # Emitted first due to max_events
    assert termination_result[1] == [8, 9, 10]
    assert termination_result[2] == [6, 7]


def test_batch_by_field_value_key_extractor():
    def append_and_return(lst, x):
        lst.append(x)
        return lst

    controller = build_flow(
        [
            Source(),
            Batch(3, 100, "field"),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    controller.emit({"field": "name_1", "field_data": 10})
    controller.emit({"field": "name_2", "field_data": 9})
    controller.emit({"field": "name_1", "field_data": 8})
    controller.emit({"field": "name_2", "field_data": 7})
    controller.emit({"field": "name_1", "field_data": 6})
    controller.emit({"field": "name_2", "field_data": 5})
    controller.emit({"field": "name_1", "field_data": 4})
    controller.emit({"field": "name_2", "field_data": 3})
    controller.emit({"field": "name_1", "field_data": 2})
    controller.emit({"field": "name_2", "field_data": 1})
    controller.emit({"field": "name_1", "field_data": 0})

    controller.terminate()
    termination_result = controller.await_termination()

    # Grouped with same field value, emitted after 3 events due to configuration
    assert termination_result[0] == [{'field': 'name_1', 'field_data': 10}, {'field': 'name_1', 'field_data': 8},
                                     {'field': 'name_1', 'field_data': 6}]
    assert termination_result[1] == [{'field': 'name_2', 'field_data': 9}, {'field': 'name_2', 'field_data': 7},
                                     {'field': 'name_2', 'field_data': 5}]
    assert termination_result[2] == [{'field': 'name_1', 'field_data': 4}, {'field': 'name_1', 'field_data': 2},
                                     {'field': 'name_1', 'field_data': 0}]
    assert termination_result[3] == [{'field': 'name_2', 'field_data': 3}, {'field': 'name_2', 'field_data': 1}]


def test_batch_by_function_key_extractor():
    def append_and_return(lst, x):
        lst.append(x)
        return lst

    controller = build_flow(
        [
            Source(),
            Batch(10, 100, lambda event: event.body % 3 == 0),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    controller.emit(1)
    controller.emit(2)
    controller.emit(3)
    controller.emit(4)
    controller.emit(5)
    controller.emit(6)
    controller.emit(7)
    controller.emit(8)
    controller.emit(9)

    controller.terminate()
    termination_result = controller.await_termination()

    assert termination_result[0] == [1, 2, 4, 5, 7, 8]
    assert termination_result[1] == [3, 6, 9]  # Group all numbers that return true on Event.body % 3 == 0


def test_batch_grouping_with_timeout():
    q = queue.Queue(1)

    def reduce_fn(acc, event):
        if event == [1]:
            q.put(None)
        acc.append(event)
        return acc

    controller = build_flow(
        [
            Source(),
            Batch(3, 1, "$key"),
            Reduce([], lambda acc, x: reduce_fn(acc, x)),
        ]
    ).run()

    controller.emit(1, key=1)
    q.get()
    controller.emit(2, key=2)
    controller.emit(2, key=2)
    controller.emit(2, key=2)
    controller.emit(3, key=2)
    controller.emit(3, key=2)
    controller.emit(3, key=2)

    controller.terminate()
    termination_result = controller.await_termination()

    assert termination_result[0] == [1]  # Emitted first due to timeout
    assert termination_result[1] == [2, 2, 2]  # Emitted second due to max_events configuration
    assert termination_result[2] == [3, 3, 3]


def test_batch_with_timeout():
    q = queue.Queue(1)

    def reduce_fn(acc, x):
        if x[0] == 0:
            q.put(None)
        acc.append(x)
        return acc

    controller = build_flow([
        Source(),
        Batch(4, 1),
        Reduce([], reduce_fn),
    ]).run()

    for i in range(10):
        if i == 3:
            q.get()
        controller.emit(i, event_time=datetime(2020, 2, 15, 2, 0))
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [[0, 1, 2], [3, 4, 5, 6], [7, 8, 9]]


async def async_test_write_csv(tmpdir):
    file_path = f'{tmpdir}/test_write_csv/out.csv'
    controller = await build_flow([
        AsyncSource(),
        WriteToCSV(file_path, columns=['n', 'n*10'], header=True)
    ]).run()

    for i in range(10):
        await controller.emit([i, 10 * i])

    await controller.terminate()
    await controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result == expected


def test_write_csv(tmpdir):
    asyncio.run(async_test_write_csv(tmpdir))


async def async_test_write_csv_error(tmpdir):
    file_path = f'{tmpdir}/test_write_csv_error.csv'

    write_csv = WriteToCSV(file_path)
    controller = await build_flow([
        AsyncSource(),
        write_csv
    ]).run()

    try:
        for i in range(10):
            await controller.emit(i)
        await controller.terminate()
        await controller.await_termination()
        assert False
    except TypeError:
        pass


def test_write_csv_error(tmpdir):
    asyncio.run(async_test_write_csv_error(tmpdir))


def test_write_csv_with_dict(tmpdir):
    file_path = f'{tmpdir}/test_write_csv_with_dict.csv'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, columns=['n', 'n*10'], header=True)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i})

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result == expected


def test_write_csv_infer_columns(tmpdir):
    file_path = f'{tmpdir}/test_write_csv_infer_columns.csv'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, header=True)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i})

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result == expected


def test_write_csv_infer_columns_without_header(tmpdir):
    file_path = f'{tmpdir}/test_write_csv_infer_columns_without_header.csv'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i})

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result == expected


def test_write_csv_with_metadata(tmpdir):
    file_path = f'{tmpdir}/test_write_csv_with_metadata.csv'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, columns=['event_key=$key', 'n', 'n*10'], header=True)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i}, key=f'key{i}')

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = \
        "event_key,n,n*10\nkey0,0,0\nkey1,1,10\nkey2,2,20\nkey3,3,30\nkey4,4,40\nkey5,5,50\nkey6,6,60\nkey7,7,70\nkey8,8,80\nkey9,9,90\n"
    assert result == expected


def test_write_csv_with_metadata_no_rename(tmpdir):
    file_path = f'{tmpdir}/test_write_csv_with_metadata_no_rename.csv'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, columns=['$key', 'n', 'n*10'], header=True)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i}, key=f'key{i}')

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = \
        "key,n,n*10\nkey0,0,0\nkey1,1,10\nkey2,2,20\nkey3,3,30\nkey4,4,40\nkey5,5,50\nkey6,6,60\nkey7,7,70\nkey8,8,80\nkey9,9,90\n"
    assert result == expected


def test_write_csv_with_rename(tmpdir):
    file_path = f'{tmpdir}/test_write_csv_with_rename.csv'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, columns=['n', 'n x 10=n*10'], header=True)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i})

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "n,n x 10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result == expected


def test_write_csv_from_lists_with_metadata(tmpdir):
    file_path = f'{tmpdir}/test_write_csv_with_metadata.csv'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, columns=['event_key=$key', 'n', 'n*10'], header=True)
    ]).run()

    for i in range(10):
        controller.emit([i, 10 * i], key=f'key{i}')

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = \
        "event_key,n,n*10\nkey0,0,0\nkey1,1,10\nkey2,2,20\nkey3,3,30\nkey4,4,40\nkey5,5,50\nkey6,6,60\nkey7,7,70\nkey8,8,80\nkey9,9,90\n"
    assert result == expected


def test_write_csv_from_lists_with_metadata_and_column_pruning(tmpdir):
    file_path = f'{tmpdir}/test_write_csv_from_lists_with_metadata_and_column_pruning.csv'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, columns=['event_key=$key', 'n*10'], header=True)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i}, key=f'key{i}')

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "event_key,n*10\nkey0,0\nkey1,10\nkey2,20\nkey3,30\nkey4,40\nkey5,50\nkey6,60\nkey7,70\nkey8,80\nkey9,90\n"
    assert result == expected


def test_write_csv_infer_with_metadata_columns(tmpdir):
    file_path = f'{tmpdir}/test_write_csv_infer_with_metadata_columns.csv'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, columns=['event_key=$key'], header=True, infer_columns_from_data=True)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i}, key=f'key{i}')

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = \
        "event_key,n,n*10\nkey0,0,0\nkey1,1,10\nkey2,2,20\nkey3,3,30\nkey4,4,40\nkey5,5,50\nkey6,6,60\nkey7,7,70\nkey8,8,80\nkey9,9,90\n"
    assert result == expected


def test_write_csv_fail_to_infer_columns(tmpdir):
    file_path = f'{tmpdir}/test_write_csv_fail_to_infer_columns.csv'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, header=True)
    ]).run()

    try:
        controller.emit([0])
        controller.terminate()
        controller.await_termination()
        assert False
    except BaseException:
        pass


def test_reduce_to_dataframe():
    controller = build_flow([
        Source(),
        ReduceToDataFrame()
    ]).run()

    expected = []
    for i in range(10):
        controller.emit({'my_int': i, 'my_string': f'this is {i}'})
        expected.append({'my_int': i, 'my_string': f'this is {i}'})
    expected = pd.DataFrame(expected)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result.equals(expected), f"{termination_result}\n!=\n{expected}"


def test_reduce_to_dataframe_with_index():
    index = 'my_int'
    controller = build_flow([
        Source(),
        ReduceToDataFrame(index=index)
    ]).run()

    expected = []
    for i in range(10):
        controller.emit({'my_int': i, 'my_string': f'this is {i}'})
        expected.append({'my_int': i, 'my_string': f'this is {i}'})
    expected = pd.DataFrame(expected)
    expected.set_index(index, inplace=True)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result.equals(expected), f"{termination_result}\n!=\n{expected}"


def test_reduce_to_dataframe_with_index_from_lists():
    index = 'my_int'
    controller = build_flow([
        Source(),
        ReduceToDataFrame(index=index, columns=['my_int', 'my_string'])
    ]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f'this is {i}'])
        expected.append({'my_int': i, 'my_string': f'this is {i}'})
    expected = pd.DataFrame(expected)
    expected.set_index(index, inplace=True)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result.equals(expected), f"{termination_result}\n!=\n{expected}"


def test_reduce_to_dataframe_indexed_by_key():
    index = 'my_key'
    controller = build_flow([
        Source(),
        ReduceToDataFrame(index=index, insert_key_column_as=index)
    ]).run()

    expected = []
    for i in range(10):
        controller.emit({'my_int': i, 'my_string': f'this is {i}'}, key=f'key{i}')
        expected.append({'my_int': i, 'my_string': f'this is {i}', 'my_key': f'key{i}'})
    expected = pd.DataFrame(expected)
    expected.set_index(index, inplace=True)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result.equals(expected), f"{termination_result}\n!=\n{expected}"


def test_to_dataframe_with_index():
    index = 'my_int'
    controller = build_flow([
        Source(),
        Batch(5),
        ToDataFrame(index=index),
        Reduce([], append_and_return, full_event=True)
    ]).run()

    expected1 = []
    for i in range(5):
        data = {'my_int': i, 'my_string': f'this is {i}'}
        controller.emit(data)
        expected1.append(data)

    expected2 = []
    for i in range(5, 10):
        data = {'my_int': i, 'my_string': f'this is {i}'}
        controller.emit(data)
        expected2.append(data)

    expected1 = pd.DataFrame(expected1)
    expected2 = pd.DataFrame(expected2)
    expected1.set_index(index, inplace=True)
    expected2.set_index(index, inplace=True)

    controller.terminate()
    termination_result = controller.await_termination()

    assert len(termination_result) == 2
    assert termination_result[0].body.equals(expected1), f"{termination_result[0]}\n!=\n{expected1}"
    assert termination_result[1].body.equals(expected2), f"{termination_result[1]}\n!=\n{expected2}"


def test_map_class():
    class MyMap(MapClass):
        def __init__(self, mul=1, **kwargs):
            super().__init__(**kwargs)
            self._mul = mul

        def do(self, event):
            if event['bid'] > 700:
                return self.filter()
            event['xx'] = event['bid'] * self._mul
            return event

    controller = build_flow([
        Source(),
        MyMap(2),
        Reduce(0, lambda acc, x: acc + x['xx']),
    ]).run()

    controller.emit({'bid': 600})
    controller.emit({'bid': 700})
    controller.emit({'bid': 1000})
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 2600


def test_extend():
    controller = build_flow([
        Source(),
        Extend(lambda x: {'bid2': x['bid'] + 1}),
        Reduce([], append_and_return),
    ]).run()

    controller.emit({'bid': 1})
    controller.emit({'bid': 11})
    controller.emit({'bid': 111})
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [{'bid': 1, 'bid2': 2}, {'bid': 11, 'bid2': 12}, {'bid': 111, 'bid2': 112}]


def test_write_to_parquet(tmpdir):
    out_dir = f'{tmpdir}/test_write_to_parquet/{uuid.uuid4().hex}/'
    columns = ['my_int', 'my_string']
    controller = build_flow([
        Source(),
        WriteToParquet(out_dir, partition_cols='my_int', columns=columns, max_events=1)
    ]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f'this is {i}'])
        expected.append([i, f'this is {i}'])
    expected = pd.DataFrame(expected, columns=columns, dtype='int32')
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_sparse_data_to_parquet(tmpdir):
    out_dir = f'{tmpdir}/test_write_sparse_data_to_parquet/{uuid.uuid4().hex}'
    columns = ['my_int', 'my_string']
    controller = build_flow([
        Source(),
        WriteToParquet(out_dir, columns=columns)
    ]).run()

    expected = []
    for i in range(10):
        expected.append({'my_int': i})
        controller.emit({'my_int': i})
        expected.append({'my_string': f'this is {i}'})
        controller.emit({'my_string': f'this is {i}'})
    expected = pd.DataFrame(expected, columns=columns)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_single_file_on_termination(tmpdir):
    out_file = f'{tmpdir}/test_write_to_parquet_single_file_on_termination_{uuid.uuid4().hex}/out.parquet'
    columns = ['my_int', 'my_string']
    controller = build_flow([
        Source(),
        WriteToParquet(out_file, columns=columns)
    ]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f'this is {i}'])
        expected.append([i, f'this is {i}'])
    expected = pd.DataFrame(expected, columns=columns, dtype='int64')
    controller.terminate()
    controller.await_termination()

    assert os.path.isfile(out_file)
    read_back_df = pd.read_parquet(out_file, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_with_metadata(tmpdir):
    out_file = f'{tmpdir}/test_write_to_parquet_with_metadata{uuid.uuid4().hex}/'
    columns = ['event_key', 'my_int', 'my_string']
    controller = build_flow([
        Source(),
        WriteToParquet(out_file, columns=['event_key=$key', 'my_int', 'my_string'])
    ]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f'this is {i}'], key=f'key{i}')
        expected.append([f'key{i}', i, f'this is {i}'])
    expected = pd.DataFrame(expected, columns=columns, dtype='int64')
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_with_indices(tmpdir):
    out_file = f'{tmpdir}/test_write_to_parquet_with_indices{uuid.uuid4().hex}'
    controller = build_flow([
        Source(),
        WriteToParquet(out_file, index_cols='event_key=$key', columns=['my_int', 'my_string'])
    ]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f'this is {i}'], key=f'key{i}')
        expected.append([f'key{i}', i, f'this is {i}'])
    columns = ['event_key', 'my_int', 'my_string']
    expected = pd.DataFrame(expected, columns=columns, dtype='int64')
    expected.set_index(['event_key'], inplace=True)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_partition_by_date(tmpdir):
    out_file = f'{tmpdir}/test_write_to_parquet_partition_by_date{uuid.uuid4().hex}'
    controller = build_flow([
        Source(),
        WriteToParquet(out_file, partition_cols=['$date'], columns=['my_int', 'my_string'])
    ]).run()

    my_time = datetime(2020, 2, 15)

    expected = []
    for i in range(10):
        controller.emit([i, f'this is {i}'], event_time=my_time)
        expected.append(['2020-02-15', i, f'this is {i}'])
    columns = ['date', 'my_int', 'my_string']
    expected = pd.DataFrame(expected, columns=columns, dtype='int64')
    expected['date'] = expected['date'].astype("category")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_with_inference(tmpdir):
    out_dir = f'{tmpdir}/test_write_to_parquet_with_inference{uuid.uuid4().hex}/'
    controller = build_flow([
        Source(),
        WriteToParquet(out_dir, index_cols='$key', partition_cols=[])
    ]).run()

    expected = []
    for i in range(10):
        controller.emit({'my_int': i, 'my_string': f'this is {i}'}, key=f'key{i}')
        expected.append([f'key{i}', i, f'this is {i}'])
    expected = pd.DataFrame(expected, columns=['key', 'my_int', 'my_string'], dtype='int64')
    expected.set_index(['key'], inplace=True)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_with_inference_error_on_partition_index_collision(tmpdir):
    try:
        WriteToParquet('out/', index_cols='$key')
        assert False
    except ValueError:
        pass


def test_join_by_key():
    table = Table('test', NoopDriver())
    table._update_static_attrs(9, {'age': 1, 'color': 'blue9'})
    table._update_static_attrs(7, {'age': 3, 'color': 'blue7'})

    controller = build_flow([
        Source(),
        Filter(lambda x: x['col1'] > 8),
        JoinWithTable(table, lambda x: x['col1']),
        Reduce([], lambda acc, x: append_and_return(acc, x))
    ]).run()
    for i in range(10):
        controller.emit({'col1': i})

    expected = [{'col1': 9, 'age': 1, 'color': 'blue9'}]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected


def test_join_by_string_key():
    table = Table('test', NoopDriver())
    table._update_static_attrs(9, {'age': 1, 'color': 'blue9'})
    table._update_static_attrs(7, {'age': 3, 'color': 'blue7'})

    controller = build_flow([
        Source(),
        Filter(lambda x: x['col1'] > 8),
        JoinWithTable(table, 'col1'),
        Reduce([], lambda acc, x: append_and_return(acc, x))
    ]).run()
    for i in range(10):
        controller.emit({'col1': i})

    expected = [{'col1': 9, 'age': 1, 'color': 'blue9'}]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected


def test_termination_result_order():
    controller = build_flow([
        Source(),
        [Reduce(1, lambda acc, x: acc)],
        [Reduce(2, lambda acc, x: acc)]
    ]).run()

    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 1


def test_termination_result_on_none():
    controller = build_flow([
        Source(),
        [Reduce(None, lambda acc, x: acc)],
        [Reduce(2, lambda acc, x: acc)]
    ]).run()

    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 2


class MockFramesClient:
    def __init__(self):
        self.call_log = []

    def create(self, backend, table, **kwargs):
        kwargs['backend'] = backend
        kwargs['table'] = table
        self.call_log.append(('create', kwargs))

    def write(self, backend, table, dfs, **kwargs):
        kwargs['backend'] = backend
        kwargs['table'] = table
        kwargs['dfs'] = dfs
        self.call_log.append(('write', kwargs))


def test_write_to_tsdb():
    mock_frames_client = MockFramesClient()

    controller = build_flow([
        Source(),
        WriteToTSDB(path='container/some/path', time_col='time', index_cols='node', columns=['cpu', 'disk'], rate='1/h',
                    max_events=1,
                    frames_client=mock_frames_client)
    ]).run()

    expected_data = []
    date_time_str = '18/09/19 01:55:1'
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + ' UTC-0000', '%d/%m/%y %H:%M:%S UTC%z')
        controller.emit([now, i, i + 1, i + 2])
        expected_data.append([now, i, i + 1, i + 2])

    controller.terminate()
    controller.await_termination()

    expected_create = (
        'create', {'if_exists': 1, 'rate': '1/h', 'aggregates': '', 'aggregation_granularity': '', 'backend': 'tsdb',
                   'table': '/some/path'})
    assert mock_frames_client.call_log[0] == expected_create
    i = 0
    for write_call in mock_frames_client.call_log[1:]:
        assert write_call[0] == 'write'
        expected = pd.DataFrame([expected_data[i]], columns=['time', 'node', 'cpu', 'disk'])
        expected.set_index(keys=['time', 'node'], inplace=True)
        res = write_call[1]['dfs']
        assert expected.equals(res), f"result{res}\n!=\nexpected{expected}"
        del write_call[1]['dfs']
        assert write_call[1] == {'backend': 'tsdb', 'table': '/some/path'}
        i += 1


def test_write_to_tsdb_with_key_index():
    mock_frames_client = MockFramesClient()

    controller = build_flow([
        Source(),
        WriteToTSDB(path='container/some/path', time_col='time', index_cols='node=$key', columns=['cpu', 'disk'], rate='1/h',
                    max_events=1, frames_client=mock_frames_client)
    ]).run()

    expected_data = []
    date_time_str = '18/09/19 01:55:1'
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + ' UTC-0000', '%d/%m/%y %H:%M:%S UTC%z')
        controller.emit([now, i + 1, i + 2], key=i)
        expected_data.append([now, i, i + 1, i + 2])

    controller.terminate()
    controller.await_termination()

    expected_create = (
        'create', {'if_exists': 1, 'rate': '1/h', 'aggregates': '', 'aggregation_granularity': '', 'backend': 'tsdb',
                   'table': '/some/path'})
    assert mock_frames_client.call_log[0] == expected_create
    i = 0
    for write_call in mock_frames_client.call_log[1:]:
        assert write_call[0] == 'write'
        expected = pd.DataFrame([expected_data[i]], columns=['time', 'node', 'cpu', 'disk'])
        expected.set_index(keys=['time', 'node'], inplace=True)
        res = write_call[1]['dfs']
        assert expected.equals(res), f"result{res}\n!=\nexpected{expected}"
        del write_call[1]['dfs']
        assert write_call[1] == {'backend': 'tsdb', 'table': '/some/path'}
        i += 1


def test_write_to_tsdb_with_key_index_and_default_time():
    mock_frames_client = MockFramesClient()

    controller = build_flow([
        Source(),
        WriteToTSDB(path='container/some/path', index_cols='node=$key', columns=['cpu', 'disk'], rate='1/h',
                    max_events=1, frames_client=mock_frames_client)
    ]).run()

    expected_data = []
    date_time_str = '18/09/19 01:55:1'
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + ' UTC-0000', '%d/%m/%y %H:%M:%S UTC%z')
        controller.emit([i + 1, i + 2], key=i, event_time=now)
        expected_data.append([now, i, i + 1, i + 2])

    controller.terminate()
    controller.await_termination()

    expected_create = (
        'create', {'if_exists': 1, 'rate': '1/h', 'aggregates': '', 'aggregation_granularity': '', 'backend': 'tsdb',
                   'table': '/some/path'})
    assert mock_frames_client.call_log[0] == expected_create
    i = 0
    for write_call in mock_frames_client.call_log[1:]:
        assert write_call[0] == 'write'
        expected = pd.DataFrame([expected_data[i]], columns=['time', 'node', 'cpu', 'disk'])
        expected.set_index(keys=['time', 'node'], inplace=True)
        res = write_call[1]['dfs']
        assert expected.equals(res), f"result{res}\n!=\nexpected{expected}"
        del write_call[1]['dfs']
        assert write_call[1] == {'backend': 'tsdb', 'table': '/some/path'}
        i += 1


def test_csv_reader_parquet_write_microsecs(tmpdir):
    out_file = f'{tmpdir}/test_csv_reader_parquet_write_microsecs_{uuid.uuid4().hex}/'
    columns = ['k', 't']

    time_format = '%d/%m/%Y %H:%M:%S.%f'
    controller = build_flow([
        ReadCSV('tests/test-with-timestamp-microsecs.csv', header=True, key_field='k',
                time_field='t', timestamp_format=time_format),
        WriteToParquet(out_file, columns=columns, max_events=2)
    ]).run()

    expected = pd.DataFrame([['m1', datetime.strptime("15/02/2020 02:03:04.123456", time_format)],
                             ['m2', datetime.strptime("16/02/2020 02:03:04.123456", time_format)]],
                            columns=columns)
    controller.await_termination()
    read_back_df = pd.read_parquet(out_file, columns=columns)

    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_csv_reader_parquet_write_nanosecs(tmpdir):
    out_file = f'{tmpdir}/test_csv_reader_parquet_write_nanosecs_{uuid.uuid4().hex}/'
    columns = ['k', 't']

    time_format = '%d/%m/%Y %H:%M:%S.%f'
    controller = build_flow([
        ReadCSV('tests/test-with-timestamp-nanosecs.csv', header=True, key_field='k',
                time_field='t', timestamp_format=time_format),
        WriteToParquet(out_file, columns=columns, max_events=2)
    ]).run()

    expected = pd.DataFrame([['m1', datetime.strptime("15/02/2020 02:03:04.123456", time_format)],
                             ['m2', datetime.strptime("16/02/2020 02:03:04.123456", time_format)]],
                            columns=columns)
    controller.await_termination()
    read_back_df = pd.read_parquet(out_file, columns=columns)

    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_error_in_concurrent_by_key_task():
    table = Table('table', V3ioDriver(webapi='https://localhost:12345', access_key='abc'))

    controller = build_flow([
        Source(),
        WriteToTable(table, columns=['twice_total_activities']),
    ]).run()

    controller.emit({'col1': 0}, 'tal')

    controller.terminate()
    try:
        controller.await_termination()
    except TypeError:
        pass


def test_async_task_error_and_complete():
    table = Table('table', NoopDriver())

    controller = build_flow([
        Source(),
        WriteToTable(table),
        Map(RaiseEx(1).raise_ex),
        Complete()
    ]).run()

    awaitable_result = controller.emit({'col1': 0}, 'tal')
    try:
        awaitable_result.await_result()
        assert False
    except ATestException:
        pass

    controller.terminate()
    try:
        controller.await_termination()
        assert False
    except ATestException:
        pass


def test_async_task_error_and_complete_repeated_emits():
    table = Table('table', NoopDriver())

    controller = build_flow([
        Source(),
        WriteToTable(table),
        Map(RaiseEx(1).raise_ex),
        Complete()
    ]).run()
    for i in range(3):
        try:
            awaitable_result = controller.emit({'col1': 0}, 'tal')
        except ATestException:
            continue
        try:
            awaitable_result.await_result()
            assert False
        except ATestException:
            pass
    controller.terminate()
    try:
        controller.await_termination()
        assert False
    except ATestException:
        pass


def test_push_error():
    class PushErrorContext:
        def push_error(self, event, message, source):
            self.event = event
            self.message = message
            self.source = source

    context = PushErrorContext()
    controller = build_flow([
        Source(),
        Map(RaiseEx(1).raise_ex, context=context),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    try:
        controller.emit(0)
        controller.terminate()
        controller.await_termination()
        assert False
    except ATestException:
        assert context.event.body == 0
        assert 'raise ATestException' in context.message
        assert context.source == 'Map'


def test_metadata_fields():
    controller = build_flow([
        Source(key_field='mykey', time_field='mytime'),
        Reduce([], append_and_return, full_event=True)
    ]).run()

    t1 = datetime(2020, 2, 15, 2, 0)
    t2 = datetime(2020, 2, 15, 2, 1)
    body1 = {'mykey': 'k1', 'mytime': t1, 'otherfield': 'x'}
    body2 = {'mykey': 'k2', 'mytime': t2, 'otherfield': 'x'}

    controller.emit(body1)
    controller.emit(Event(body2, 'k2', t2))

    controller.terminate()
    result = controller.await_termination()

    assert len(result) == 2

    result1 = result[0]
    assert result1.key == 'k1'
    assert result1.time == t1
    assert result1.body == body1

    result2 = result[1]
    assert result2.key == 'k2'
    assert result2.time == t2
    assert result2.body == body2


async def async_test_async_metadata_fields():
    controller = await build_flow([
        AsyncSource(key_field='mykey', time_field='mytime'),
        Reduce([], append_and_return, full_event=True)
    ]).run()

    body = {'mykey': 'k1', 'mytime': datetime(2020, 2, 15, 2, 0), 'otherfield': 'x'}
    await controller.emit(body)
    await controller.terminate()
    result = await controller.await_termination()
    assert len(result) == 1
    result = result[0]
    assert result.key == 'k1'
    assert result.time == datetime(2020, 2, 15, 2, 0)
    assert result.body == body


def test_async_metadata_fields():
    asyncio.run(async_test_async_metadata_fields())


def test_uuid():
    controller = build_flow([
        Source(),
        Map(lambda event: event.copy(body=event.id), full_event=True),
        Reduce([], append_and_return)
    ]).run()

    for _ in range(1025):
        controller.emit(0)

    controller.terminate()
    result = controller.await_termination()

    assert len(result) == 1025
    base_id = result[0][:32]
    for i, cur_id in enumerate(result[:1024]):
        assert cur_id == f'{base_id}-{i:04}'
    assert result[1024][:32] != base_id
    assert result[1024][32:] == '-0000'


def test_input_path():
    controller = build_flow([
        Source(),
        Filter(lambda x: x < 5, input_path="col2.col3"),  # filter emits the full event
        Map(lambda x: x + 1, input_path="col2.col3"),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    for i in range(10):
        val = 5 if i % 2 == 0 else 1
        controller.emit({'col1': i, 'col2': {'col3': val}})
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 10


def test_result_path():
    controller = build_flow([
        Source(),
        Map(lambda x: {"new_field": 5}, result_path="step_result"),
        Reduce(0, lambda acc, x: x),
    ]).run()

    controller.emit({'col1': 1})
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == {'col1': 1, 'step_result': {"new_field": 5}}


def test_to_dict():
    source = Source(name='my_source', buffer_size=5)
    identity = Map(lambda x: x, full_event=False)
    assert source.to_dict() == {'class_name': 'storey.sources.Source', 'class_args': {'buffer_size': 5}, 'name': 'my_source',
                                'full_event': False}
    assert identity.to_dict() == {'class_name': 'storey.flow.Map', 'class_args': {}, 'full_event': False}


def test_flow_reuse():
    flow = build_flow([Source(), Map(lambda x: x + 1), Reduce(0, lambda acc, x: acc + x)])

    for _ in range(3):
        controller = flow.run()
        for i in range(10):
            controller.emit(i)
        controller.terminate()
        result = controller.await_termination()
        assert result == 55


def test_flow_to_dict_read_csv():
    step = ReadCSV('tests/test-with-timestamp-microsecs.csv', header=True, key_field='k', time_field='t',
                   timestamp_format='%d/%m/%Y %H:%M:%S.%f')
    assert step.to_dict() == {
        'class_name': 'storey.sources.ReadCSV',
        'class_args': {
            'build_dict': False,
            'header': True,
            'key_field': 'k',
            'paths': 'tests/test-with-timestamp-microsecs.csv',
            'time_field': 't',
            'timestamp_format': '%d/%m/%Y %H:%M:%S.%f',
            'type_inference': True
        },
        'full_event': False
    }


def test_flow_to_dict_write_to_parquet():
    step = WriteToParquet('outdir', columns=['col1', 'col2'], max_events=2)
    assert step.to_dict() == {
        'class_name': 'storey.writers.WriteToParquet',
        'class_args': {
            'path': 'outdir',
            'columns': ['col1', 'col2'],
            'max_events': 2,
            'flush_after_seconds': 60,
        },
        'full_event': False
    }


def test_flow_to_dict_write_to_tsdb():
    step = WriteToTSDB(path='some/path', time_col='time', index_cols='node', columns=['cpu', 'disk'], rate='1/h',
                       max_events=1, frames_client=MockFramesClient())

    assert step.to_dict() == {
        'class_name': 'storey.writers.WriteToTSDB',
        'class_args': {
            'columns': ['cpu', 'disk'],
            'index_cols': 'node',
            'max_events': 1,
            'path': 'some/path',
            'rate': '1/h',
            'time_col': 'time'
        },
        'full_event': False
    }


def test_flow_to_dict_dataframe_source():
    df = pd.DataFrame([['key1', datetime(2020, 2, 15), 'id1', 1.1]], columns=['my_key', 'my_time', 'my_id', 'my_value'])
    step = DataframeSource(df, key_field='my_key', time_field='my_time', id_field='my_id')

    assert step.to_dict() == {
        'class_name': 'storey.sources.DataframeSource',
        'class_args': {
            'id_field': 'my_id',
            'key_field': 'my_key',
            'time_field': 'my_time'
        },
        'full_event': False
    }


def test_to_code():
    flow = build_flow([
        Source(),
        Batch(5),
        ToDataFrame(index=[]),
        Reduce([], append_and_return, full_event=True)
    ])

    reconstructed_code = flow.to_code()
    expected = """source0 = Source()
batch0 = Batch(max_events=5)
to_data_frame0 = ToDataFrame()
reduce0 = Reduce(initial_value=[], full_event=True)

source0.to(batch0)
batch0.to(to_data_frame0)
to_data_frame0.to(reduce0)
"""
    assert reconstructed_code == expected


def test_split_flow_to_code():
    flow = build_flow([
        Source(),
        [
            Batch(5),
            Reduce([], lambda x: len(x))
        ],
        Batch(5),
        ToDataFrame(index=[]),
        Reduce([], append_and_return, full_event=True)
    ])

    reconstructed_code = flow.to_code()
    expected = """source0 = Source()
batch0 = Batch(max_events=5)
reduce0 = Reduce(initial_value=[])
batch1 = Batch(max_events=5)
to_data_frame0 = ToDataFrame()
reduce1 = Reduce(initial_value=[], full_event=True)

source0.to(batch0)
batch0.to(reduce0)
source0.to(batch1)
batch1.to(to_data_frame0)
to_data_frame0.to(reduce1)
"""
    assert reconstructed_code == expected


def test_reader_writer_to_code():
    flow = build_flow([
        ReadCSV('mycsv.csv'),
        WriteToParquet('mypq')
    ])

    reconstructed_code = flow.to_code()
    print(reconstructed_code)
    expected = """read_c_s_v0 = ReadCSV(paths='mycsv.csv', header=False, build_dict=False, type_inference=True)
write_to_parquet0 = WriteToParquet(flush_after_seconds=60, path='mypq', max_events=10000)

read_c_s_v0.to(write_to_parquet0)
"""
    assert reconstructed_code == expected


def test_illegal_step_no_source():
    try:
        Reduce([], append_and_return, full_event=True).run()
        assert False
    except ValueError as ex:
        assert str(ex) == 'Flow must start with a source'


def test_illegal_step_source_not_first_step():
    df = pd.DataFrame([['hello', 1, 1.5], ['world', 2, 2.5]], columns=['string', 'int', 'float'])
    try:
        build_flow([
            ReadParquet('tests'),
            DataframeSource(df),
            Reduce([], append_and_return),
        ]).run()
        assert False
    except ValueError as ex:
        assert str(ex) == 'DataframeSource can only appear as the first step of a flow'


def test_writer_downstream(tmpdir):
    file_path = f'{tmpdir}/test_writer_downstream/out.csv'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, columns=['n', 'n*10'], header=True),
        Reduce(0, lambda acc, x: acc + x[0])
    ]).run()

    for i in range(10):
        controller.emit([i, i * 10])

    controller.terminate()
    result = controller.await_termination()
    assert result == 45


def test_complete_in_error_flow():
    reduce = build_flow([
        Complete(),
        Reduce(0, lambda acc, x: acc + x)
    ])
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Map(RaiseEx(5).raise_ex, recovery_step=reduce),
        Map(lambda x: x * 100),
        reduce
    ]).run()

    for i in range(10):
        awaitable_result = controller.emit(i)
        if i == 4:
            assert awaitable_result.await_result() == i + 1
        else:
            assert awaitable_result.await_result() == (i + 1) * 100
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 5005


def test_non_existing_key_query_by_key():
    df = pd.DataFrame([['katya', 'green'], ['dina', 'blue']], columns=['name', 'color'])
    table = Table('table', NoopDriver())
    controller = build_flow([
        DataframeSource(df, key_field='name'),
        WriteToTable(table),
    ]).run()
    controller.await_termination()

    controller = build_flow([
        Source(),
        QueryByKey(["color"], table, key="name"),
    ]).run()

    controller.emit({'nameeeee': 'katya'}, 'katya')
    controller.terminate()
    controller.await_termination()


def test_csv_reader_with_none_values():
    controller = build_flow([
        ReadCSV('tests/test-with-none-values.csv', header=True, key_field='string'),
        Reduce([], append_and_return, full_event=True),
    ]).run()

    termination_result = controller.await_termination()

    print(termination_result)

    assert len(termination_result) == 2
    assert termination_result[0].key == 'a'
    assert termination_result[0].body == ['a', True, False, 1, 2.3]
    assert termination_result[1].key == 'b'
    assert termination_result[1].body == ['b', True, None, math.nan, math.nan]
