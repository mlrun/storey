import _csv
import asyncio
import time
import uuid
from datetime import datetime

import pandas as pd

from storey import build_flow, Source, Map, Filter, FlatMap, Reduce, FlowError, MapWithState, ReadCSV, Complete, AsyncSource, Choice, \
    Event, Batch, Table, NoopDriver, WriteToCSV, DataframeSource, MapClass, JoinWithTable, ReduceToDataFrame, ToDataFrame, WriteToParquet, \
    WriteToTSDB


class ATestException(Exception):
    pass


class RaiseEx:
    _counter = 0

    def __init__(self, raise_after):
        self._raise_after = raise_after

    def raise_ex(self, element):
        if self._counter == self._raise_after:
            raise ATestException("test")
        self._counter += 1
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


def test_csv_reader():
    controller = build_flow([
        ReadCSV('tests/test.csv', with_header=True),
        FlatMap(lambda x: x),
        Map(lambda x: int(x)),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    termination_result = controller.await_termination()
    assert termination_result == 21


def test_csv_reader_as_dict():
    controller = build_flow([
        ReadCSV('tests/test.csv', with_header=True, build_dict=True),
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
        ReadCSV('tests/test-with-timestamp.csv', with_header=True, build_dict=True, key_field='k',
                timestamp_field='t', timestamp_format='%d/%m/%Y %H:%M:%S'),
        Reduce([], append_and_return, full_event=True),
    ]).run()

    termination_result = controller.await_termination()

    assert len(termination_result) == 2
    assert termination_result[0].key == 'm1'
    assert termination_result[0].time == datetime(2020, 2, 15, 2, 0)
    assert termination_result[0].body == {'k': 'm1', 't': '15/02/2020 02:00:00', 'v': '8'}
    assert termination_result[1].key == 'm2'
    assert termination_result[1].time == datetime(2020, 2, 16, 2, 0)
    assert termination_result[1].body == {'k': 'm2', 't': '16/02/2020 02:00:00', 'v': '14'}


def test_csv_reader_with_key_and_timestamp():
    controller = build_flow([
        ReadCSV('tests/test-with-timestamp.csv', with_header=True, key_field='k',
                timestamp_field='t', timestamp_format='%d/%m/%Y %H:%M:%S'),
        Reduce([], append_and_return, full_event=True),
    ]).run()

    termination_result = controller.await_termination()

    assert len(termination_result) == 2
    assert termination_result[0].key == 'm1'
    assert termination_result[0].time == datetime(2020, 2, 15, 2, 0)
    assert termination_result[0].body == ['m1', '15/02/2020 02:00:00', '8']
    assert termination_result[1].key == 'm2'
    assert termination_result[1].time == datetime(2020, 2, 16, 2, 0)
    assert termination_result[1].body == ['m2', '16/02/2020 02:00:00', '14']


def test_csv_reader_as_dict_no_header():
    controller = build_flow([
        ReadCSV('tests/test-no-header.csv', with_header=False, build_dict=True),
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
        DataframeSource(df, key_column='my_key', time_column='my_time', id_column='my_id'),
        Reduce([], append_and_return, full_event=True),
    ]).run()

    termination_result = controller.await_termination()
    expected = [Event({'my_value': 1.1}, key='key1', time=t1, id='id1'), Event({'my_value': 2.2}, key='key2', time=t2, id='id2')]
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
    except FlowError as flow_ex:
        assert isinstance(flow_ex.__cause__, ATestException)


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
    table_object._cache['tal'] = {'color': 'blue'}
    table_object._cache['dina'] = {'color': 'red'}

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
    assert table_object._cache == expected_cache


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
    assert termination_result == expected
    assert table_object._cache == expected_cache


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
        awaitable_result = controller.emit(i, return_awaitable_result=True)
        assert awaitable_result.await_result() == i + 1
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 55


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
        result = await controller.emit(i, await_result=True)
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
        Map(RaiseEx(500).raise_ex),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    try:
        for i in range(1000):
            await controller.emit(i)
    except FlowError as flow_ex:
        assert isinstance(flow_ex.__cause__, ATestException)


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
    result = controller.emit(event, return_awaitable_result=True).await_result()
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


def test_batch_with_timeout():
    controller = build_flow([
        Source(),
        Batch(4, 2),
        Reduce([], lambda acc, x: append_and_return(acc, x)),
    ]).run()

    for i in range(10):
        if i == 3:
            time.sleep(3)
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [[0, 1, 2], [3, 4, 5, 6], [7, 8, 9]]


async def async_test_write_csv(tmpdir):
    file_path = f'{tmpdir}/test_write_csv.csv'
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
    except FlowError as ex:
        assert isinstance(ex.__cause__, _csv.Error)
    assert write_csv._open_file.closed


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
    except FlowError as flow_ex:
        assert isinstance(flow_ex.__cause__, ValueError)


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


def test_write_to_parquet_single_file_on_termination(tmpdir):
    out_file = f'{tmpdir}/test_write_to_parquet_single_file_on_termination_{uuid.uuid4().hex}.parquet'
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

    read_back_df = pd.read_parquet(out_file, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_with_metadata(tmpdir):
    out_file = f'{tmpdir}/test_write_to_parquet_with_metadata{uuid.uuid4().hex}.parquet'
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
    out_file = f'{tmpdir}/test_write_to_parquet_with_indices{uuid.uuid4().hex}.parquet'
    controller = build_flow([
        Source(),
        WriteToParquet(out_file, index_cols='event_key', columns=['event_key=$key', 'my_int', 'my_string'])
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


def test_join_by_key():
    table = Table('test', NoopDriver())
    table.update_key(9, {'age': 1, 'color': 'blue9'})
    table.update_key(7, {'age': 3, 'color': 'blue7'})

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
    columns = ['cpu', 'disk', 'time', 'node']
    mock_frames_client = MockFramesClient()

    controller = build_flow([
        Source(),
        WriteToTSDB(path='some/path', time_col='time', labels_cols='node', columns=columns, rate='1/h', max_events=1,
                    frames_client=mock_frames_client)
    ]).run()

    expected_data = []
    date_time_str = '18/09/19 01:55:1'
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + ' UTC-0000', '%d/%m/%y %H:%M:%S UTC%z')
        controller.emit([i + 1, i + 2, now, i])
        expected_data.append([i + 1, i + 2, now, i])

    controller.terminate()
    controller.await_termination()

    expected_create = ('create', {'if_exists': 1, 'rate': '1/h', 'aggregates': '', 'aggregation_granularity': '', 'backend': 'tsdb',
                                  'table': 'some/path'})
    assert mock_frames_client.call_log[0] == expected_create
    i = 0
    for write_call in mock_frames_client.call_log[1:]:
        assert write_call[0] == 'write'
        expected = pd.DataFrame([expected_data[i]], columns=columns)
        expected.set_index(keys=['time', 'node'], inplace=True)
        res = write_call[1]['dfs']
        assert expected.equals(res), f"result{res}\n!=\nexpected{expected}"
        del write_call[1]['dfs']
        assert write_call[1] == {'backend': 'tsdb', 'table': 'some/path'}
        i += 1
