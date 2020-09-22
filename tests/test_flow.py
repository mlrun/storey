import asyncio
import csv
import io
from datetime import datetime

from storey import build_flow, Source, Map, Filter, FlatMap, Reduce, FlowError, MapWithState, ReadCSV, Complete, AsyncSource, Choice, Event, \
    Batch, Cache, NoopDriver, WriteCSV
import time
import aiofiles

from storey import build_flow, Source, Map, Filter, FlatMap, Reduce, FlowError, MapWithState, ReadCSV, Complete, AsyncSource, Choice


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
    cache = Cache("table", NoopDriver())
    cache._cache['tal'] = {'color': 'blue'}
    cache._cache['dina'] = {'color': 'red'}

    def enrich(event, state):
        event['color'] = state['color']
        state['counter'] = state.get('counter', 0) + 1
        return event, state

    controller = build_flow([
        Source(),
        MapWithState(cache, lambda x, state: enrich(x, state), group_by_key=True),
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
    assert cache._cache == expected_cache


def test_map_with_empty_cache_state_flow():
    cache = Cache("table", NoopDriver())

    def enrich(event, state):
        if 'first_value' not in state:
            state['first_value'] = event['col1']
        event['diff_from_first'] = event['col1'] - state['first_value']
        state['counter'] = state.get('counter', 0) + 1
        return event, state

    controller = build_flow([
        Source(),
        MapWithState(cache, lambda x, state: enrich(x, state), group_by_key=True),
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
    assert cache._cache == expected_cache


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


async def async_test_write_csv():
    file_name = 'test_write_csv.csv'
    controller = await build_flow([
        AsyncSource(),
        WriteCSV(file_name, lambda x: [x, 10 * x], header=['n', 'n*10'])
    ]).run()

    for i in range(10):
        await controller.emit(i)

    await controller.terminate()
    await controller.await_termination()

    with open(file_name) as file:
        result = file.read()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result == expected


def test_write_csv():
    asyncio.run(async_test_write_csv())
