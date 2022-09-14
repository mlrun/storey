# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import asyncio
import base64
import json
import time
from datetime import datetime, timedelta
import pytest
import aiohttp
import pandas as pd
import v3io
import v3io.aio.dataplane
import v3io_frames as frames

from storey import Filter, JoinWithV3IOTable, SendToHttp, Map, Reduce, SyncEmitSource, HttpRequest, build_flow, \
    StreamTarget, V3ioDriver, TSDBTarget, Table, JoinWithTable, MapWithState, NoSqlTarget, DataframeSource, \
    CSVSource, AsyncEmitSource, Event
from .integration_test_utils import V3ioHeaders, append_return, test_base_time, setup_kv_teardown_test, \
    setup_teardown_test, TestContext, assign_stream_teardown_test, create_stream

_prevents_ide_from_optimizing_these_away = [
    setup_kv_teardown_test,
    setup_teardown_test,
    assign_stream_teardown_test,
]


class GetShardData(V3ioHeaders):
    async def get_shard_data(self, path):
        connector = aiohttp.TCPConnector()
        client_session = aiohttp.ClientSession(connector=connector)
        request_body = json.dumps({'Type': 'EARLIEST'})
        response = await client_session.request(
            'PUT', f'{self._webapi_url}/{path}', headers=self._seek_headers, data=request_body, ssl=False)
        if response.status == 404:
            return []
        if response.status == 400:  # Regression in 2.10
            body = await response.text()
            try:
                body_obj = json.loads(body)
                if body_obj['ErrorMessage'] == "ResourceNotFoundException":
                    return []
            except:
                raise AssertionError(f'Got response status code 400: {body}')
        assert response.status == 200, await response.text()
        location = json.loads(await response.text())['Location']
        data = []
        while True:
            request_body = json.dumps({'Location': location})
            response = await client_session.request(
                'PUT', f'{self._webapi_url}/{path}', headers=self._get_records_headers, data=request_body, ssl=False)
            assert response.status == 200, await response.text()
            response_dict = json.loads(await response.text())
            for record in response_dict['Records']:
                data.append(base64.b64decode(record['Data']))
            if response_dict['RecordsBehindLatest'] == 0:
                break
            location = response_dict['NextLocation']
        return data


def _get_redis_kv_all_attrs(setup_teardown_test: TestContext, key: str):
    from .integration_test_utils import get_redis_client
    from storey.redis_driver import RedisDriver
    table_name = setup_teardown_test.table_name
    redis_key = f'storey-test:{table_name}{key}:static'
    redis_fake_server = setup_teardown_test.redis_fake_server
    values = get_redis_client(redis_fake_server=redis_fake_server).hgetall(redis_key)
    return {RedisDriver.convert_to_str(key): RedisDriver.convert_redis_value_to_python_obj(val) for key, val in values.items()}


def get_key_all_attrs_test_helper(setup_teardown_test: TestContext, key: str):
    if setup_teardown_test.driver_name == "RedisDriver":
        result = _get_redis_kv_all_attrs(setup_teardown_test, key)
    else:
        response = asyncio.run(get_kv_item(setup_teardown_test.table_name, key))
        assert response.status_code == 200
        result = response.output.item
    return result


def test_join_with_v3io_table(setup_kv_teardown_test):
    if setup_kv_teardown_test.driver_name == "RedisDriver":
        pytest.skip(msg='test not relevant for Redis')

    table_path = setup_kv_teardown_test.table_name
    controller = build_flow([
        SyncEmitSource(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 8),
        JoinWithV3IOTable(V3ioDriver(), lambda x: x, lambda x, y: y['age'], table_path),
        Reduce(0, lambda x, y: x + y)
    ]).run()
    for i in range(10):
        controller.emit(i)

    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 42


def test_join_with_http():
    controller = build_flow([
        SyncEmitSource(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 8),
        SendToHttp(lambda _: HttpRequest('GET', 'https://google.com', ''), lambda _, response: response.status),
        Reduce(0, lambda x, y: x + y)
    ]).run()
    for i in range(10):
        controller.emit(i)

    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 200 * 7


async def async_test_write_to_v3io_stream(setup_stream_teardown_test):
    stream_path = setup_stream_teardown_test
    controller = build_flow([
        AsyncEmitSource(),
        Map(lambda x: str(x)),
        StreamTarget(V3ioDriver(), stream_path, sharding_func=lambda event: int(event.body), batch_size=8, shard_count=2, full_event=False)
    ]).run()
    for i in range(10):
        await controller.emit(i)

    await asyncio.sleep(5)

    try:
        shard0_data = await GetShardData().get_shard_data(f'{stream_path}/0')
        assert shard0_data == [b'0', b'2', b'4', b'6', b'8']
        shard1_data = await GetShardData().get_shard_data(f'{stream_path}/1')
        assert shard1_data == [b'1', b'3', b'5', b'7', b'9']
    finally:
        await controller.terminate()
        await controller.await_termination()


def test_write_to_v3io_stream(assign_stream_teardown_test):
    asyncio.run(async_test_write_to_v3io_stream(assign_stream_teardown_test))


async def async_test_write_to_v3io_stream_full_event_readback(setup_stream_teardown_test):
    stream_path = setup_stream_teardown_test
    event_time = datetime(2022, 8, 8)

    controller = build_flow([
        AsyncEmitSource(),
        StreamTarget(V3ioDriver(), stream_path, sharding_func=lambda event: int(event.body), batch_size=8, shard_count=2, full_event=True)
    ]).run()
    for i in range(10):
        await controller.emit(Event(i, time=event_time, id=str(i)))

    await asyncio.sleep(5)

    try:
        shard0_data = await GetShardData().get_shard_data(f'{stream_path}/0')
        assert shard0_data == [
            b'{"full_event_wrapper": true, "body": 0, "time": "2022-08-08 00:00:00", "id": "0"}',
            b'{"full_event_wrapper": true, "body": 2, "time": "2022-08-08 00:00:00", "id": "2"}',
            b'{"full_event_wrapper": true, "body": 4, "time": "2022-08-08 00:00:00", "id": "4"}',
            b'{"full_event_wrapper": true, "body": 6, "time": "2022-08-08 00:00:00", "id": "6"}',
            b'{"full_event_wrapper": true, "body": 8, "time": "2022-08-08 00:00:00", "id": "8"}'
        ]
        shard1_data = await GetShardData().get_shard_data(f'{stream_path}/1')
        assert shard1_data == [
            b'{"full_event_wrapper": true, "body": 1, "time": "2022-08-08 00:00:00", "id": "1"}',
            b'{"full_event_wrapper": true, "body": 3, "time": "2022-08-08 00:00:00", "id": "3"}',
            b'{"full_event_wrapper": true, "body": 5, "time": "2022-08-08 00:00:00", "id": "5"}',
            b'{"full_event_wrapper": true, "body": 7, "time": "2022-08-08 00:00:00", "id": "7"}',
            b'{"full_event_wrapper": true, "body": 9, "time": "2022-08-08 00:00:00", "id": "9"}'
        ]
    finally:
        await controller.terminate()
        await controller.await_termination()

    controller = build_flow([
        AsyncEmitSource(),
        Reduce([], lambda acc, x: append_return(acc, x), full_event=True),
    ]).run()
    for record in (shard0_data + shard1_data):
        await controller.emit(Event(json.loads(record.decode('utf8')), id='some-new-id'))

    await controller.terminate()
    result = await controller.await_termination()

    assert len(result) == 10
    expected_bodies = [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]
    for i, record in enumerate(result):
        assert record.body == expected_bodies[i]
        assert record.id == str(expected_bodies[i])
        assert record.time == event_time


def test_async_test_write_to_v3io_stream_full_event_readback(assign_stream_teardown_test):
    asyncio.run(async_test_write_to_v3io_stream_full_event_readback(assign_stream_teardown_test))


# ML-1219
def test_write_to_v3io_stream_timestamps(assign_stream_teardown_test):
    df = pd.DataFrame([['hello', pd.Timestamp('2018-05-07 13:52:37'), datetime(2012, 8, 8, 21, 46, 24, 862000)]],
                      columns=['string', 'ts', 'datetime'])
    stream_path = assign_stream_teardown_test
    controller = build_flow([
        DataframeSource(df),
        StreamTarget(V3ioDriver(), stream_path, sharding_func=lambda event: 0, infer_columns_from_data=True, full_event=False)
    ]).run()
    controller.await_termination()
    shard0_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/0'))
    assert len(shard0_data) == 1
    assert json.loads(shard0_data[0].decode("utf-8")) == {
        "datetime": "2012-08-08 21:46:24.862000", "string": "hello", "ts": "2018-05-07 13:52:37"
    }


def test_write_to_v3io_stream_with_column_inference(assign_stream_teardown_test):
    stream_path = assign_stream_teardown_test
    controller = build_flow([
        SyncEmitSource(),
        StreamTarget(V3ioDriver(), stream_path, sharding_func=lambda event: event.body['x'], infer_columns_from_data=True, shard_count=2,
                     full_event=False)
    ]).run()
    for i in range(10):
        controller.emit({'x': i, 'y': f'{i}+{i}={i * 2}'})

    controller.terminate()
    controller.await_termination()
    shard0_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/0'))
    assert shard0_data == [
        b'{"x": 0, "y": "0+0=0"}',
        b'{"x": 2, "y": "2+2=4"}',
        b'{"x": 4, "y": "4+4=8"}',
        b'{"x": 6, "y": "6+6=12"}',
        b'{"x": 8, "y": "8+8=16"}'
    ]
    shard1_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/1'))
    assert shard1_data == [
        b'{"x": 1, "y": "1+1=2"}',
        b'{"x": 3, "y": "3+3=6"}',
        b'{"x": 5, "y": "5+5=10"}',
        b'{"x": 7, "y": "7+7=14"}',
        b'{"x": 9, "y": "9+9=18"}'
    ]


def test_write_to_pre_existing_stream(assign_stream_teardown_test):
    stream_path = assign_stream_teardown_test
    asyncio.run(create_stream(stream_path))
    df = pd.DataFrame([['hello', "goodbye"]], columns=['first', 'second'])
    controller = build_flow([
        DataframeSource(df),
        StreamTarget(V3ioDriver(), stream_path, sharding_func=lambda event: 0, infer_columns_from_data=True, full_event=False)
    ]).run()
    controller.await_termination()
    shard0_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/0'))
    assert shard0_data == [b'{"first": "hello", "second": "goodbye"}']


def test_write_dict_to_v3io_stream(assign_stream_teardown_test):
    stream_path = assign_stream_teardown_test
    controller = build_flow([
        SyncEmitSource(),
        StreamTarget(V3ioDriver(), stream_path, sharding_func=lambda event: int(event.key), columns=['$key'],
                     infer_columns_from_data=True, shard_count=2, full_event=False)
    ]).run()
    expected_shard0 = []
    expected_shard1 = []
    for i in range(10):
        controller.emit({'mydata': 'abcdefg'}, key=f'{i}')
        expected = {'mydata': 'abcdefg', 'key': f'{i}'}
        if i % 2 == 0:
            expected_shard0.append(expected)
        else:
            expected_shard1.append(expected)

    controller.terminate()
    controller.await_termination()
    shard0_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/0'))
    shard1_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/1'))

    for i in range(len(shard0_data)):
        shard0_data[i] = json.loads(shard0_data[i])
    for i in range(len(shard1_data)):
        shard1_data[i] = json.loads(shard1_data[i])

    assert shard0_data == expected_shard0
    assert shard1_data == expected_shard1


def test_write_to_v3io_stream_unbalanced(assign_stream_teardown_test):
    stream_path = assign_stream_teardown_test
    controller = build_flow([
        SyncEmitSource(),
        Map(lambda x: str(x)),
        StreamTarget(V3ioDriver(), stream_path, sharding_func=lambda event: 0, shard_count=2, full_event=False)
    ]).run()
    for i in range(10):
        controller.emit(i)

    controller.terminate()
    controller.await_termination()
    shard0_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/0'))
    assert shard0_data == [b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9']
    shard1_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/1'))
    assert shard1_data == []


def test_push_error_on_write_to_v3io_stream(assign_stream_teardown_test):
    class Context:
        def __init__(self):
            self.num_push_error_called = 0

        def push_error(self, *args, **kwargs):
            self.num_push_error_called += 1

    context = Context()
    stream_path = assign_stream_teardown_test
    controller = build_flow([
        SyncEmitSource(),
        StreamTarget(V3ioDriver(), stream_path, sharding_func=lambda event: 0, shard_count=1, context=context, full_event=False)
    ]).run()
    # Write a 3MB record, which exceeds the 2MB v3io record size limit, then a small record.
    controller.emit('3' * (1024 * 1024 * 3))
    controller.emit('0')

    controller.terminate()
    controller.await_termination()
    assert context.num_push_error_called == 1
    shard0_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/0'))
    assert shard0_data == [b'0']


def test_write_to_tsdb():
    table_name = f'storey_ci/tsdb_path-{int(time.time_ns() / 1000)}'
    tsdb_path = f'v3io://bigdata/{table_name}'
    controller = build_flow([
        SyncEmitSource(),
        TSDBTarget(path=tsdb_path, time_col='time', index_cols='node', columns=['cpu', 'disk'], rate='1/h', max_events=2)
    ]).run()

    expected = []
    date_time_str = '18/09/19 01:55:1'
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + ' UTC-0000', '%d/%m/%y %H:%M:%S UTC%z')
        controller.emit([now, i, i + 1, i + 2])
        expected.append([now, f'{i}', float(i + 1), float(i + 2)])

    controller.terminate()
    controller.await_termination()

    client = frames.Client()
    res = client.read('tsdb', table_name, start='0', end='now', multi_index=True)
    res = res.sort_values(['time'])
    df = pd.DataFrame(expected, columns=['time', 'node', 'cpu', 'disk'])
    df.set_index(keys=['time', 'node'], inplace=True)
    assert res.equals(df), f"result{res}\n!=\nexpected{df}"


def test_write_to_tsdb_with_metadata_label():
    table_name = f'tsdb_path-{int(time.time_ns() / 1000)}'
    tsdb_path = f'projects/{table_name}'
    controller = build_flow([
        SyncEmitSource(),
        TSDBTarget(path=tsdb_path, index_cols='node', columns=['cpu', 'disk'], rate='1/h',
                   max_events=2)
    ]).run()

    expected = []
    date_time_str = '18/09/19 01:55:1'
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + ' UTC-0000', '%d/%m/%y %H:%M:%S UTC%z')
        controller.emit([i, i + 1, i + 2], event_time=now)
        expected.append([now, f'{i}', float(i + 1), float(i + 2)])

    controller.terminate()
    controller.await_termination()

    client = frames.Client(container='projects')
    res = client.read('tsdb', table_name, start='0', end='now', multi_index=True)
    res = res.sort_values(['time'])
    df = pd.DataFrame(expected, columns=['time', 'node', 'cpu', 'disk'])
    df.set_index(keys=['time', 'node'], inplace=True)
    assert res.equals(df), f"result{res}\n!=\nexpected{df}"


def test_join_by_key(setup_kv_teardown_test):
    table = Table(setup_kv_teardown_test.table_name, setup_kv_teardown_test.driver())

    controller = build_flow([
        SyncEmitSource(),
        JoinWithTable(table, 'col1', key='age'),
        Reduce([], lambda acc, x: append_return(acc, x))
    ]).run()

    controller.emit({'col1': 9})

    expected = [{'col1': 9, 'age': 1, 'color': 'blue9'}]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected


def test_join_by_key_specific_attributes(setup_kv_teardown_test):
    table = Table(setup_kv_teardown_test.table_name, setup_kv_teardown_test.driver())

    controller = build_flow([
        SyncEmitSource(),
        JoinWithTable(table, 'col1', attributes=['age']),
        Reduce([], lambda acc, x: append_return(acc, x))
    ]).run()
    controller.emit({'col1': 9})

    expected = [{'col1': 9, 'age': 1}]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected


def test_outer_join_by_key(setup_kv_teardown_test):
    table = Table(setup_kv_teardown_test.table_name, setup_kv_teardown_test.driver())

    controller = build_flow([
        SyncEmitSource(),
        JoinWithTable(table, 'col1', attributes=['age']),
        Reduce([], lambda acc, x: append_return(acc, x))
    ]).run()
    for i in range(9, 11):
        controller.emit({'col1': i})

    expected = [
        {'col1': 9, 'age': 1},
        {'col1': 10}
    ]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected


def test_inner_join_by_key(setup_kv_teardown_test):
    table = Table(setup_kv_teardown_test.table_name, setup_kv_teardown_test.driver())

    controller = build_flow([
        SyncEmitSource(),
        JoinWithTable(table, 'col1', attributes=['age'], inner_join=True),
        Reduce([], lambda acc, x: append_return(acc, x))
    ]).run()
    for i in range(9, 11):
        controller.emit({'col1': i})

    expected = [
        {'col1': 9, 'age': 1}
    ]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected


def test_write_table_specific_columns(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    table['tal'] = {'color': 'blue', 'age': 41, 'iss': True, 'sometime': test_base_time, 'min': 1, 'Avg': 3}

    def enrich(event, state):
        if 'first_activity' not in state:
            state['first_activity'] = event.time

        event.body['time_since_activity'] = (event.time - state['first_activity']).seconds
        state['last_event'] = event.time
        state['total_activities'] = state.get('total_activities', 0) + 1
        event.body['color'] = state['color']
        event.body['twice_total_activities'] = state['total_activities'] * 2
        return event, state

    controller = build_flow([
        SyncEmitSource(),
        MapWithState(table, enrich, group_by_key=True, full_event=True),
        NoSqlTarget(table, columns=['twice_total_activities']),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'time_since_activity': 0, 'twice_total_activities': 2, 'color': 'blue'},
        {'col1': 1, 'time_since_activity': 1500, 'twice_total_activities': 4, 'color': 'blue'},
        {'col1': 2, 'time_since_activity': 3000, 'twice_total_activities': 6, 'color': 'blue'},
        {'col1': 3, 'time_since_activity': 4500, 'twice_total_activities': 8, 'color': 'blue'},
        {'col1': 4, 'time_since_activity': 6000, 'twice_total_activities': 10, 'color': 'blue'},
        {'col1': 5, 'time_since_activity': 7500, 'twice_total_activities': 12, 'color': 'blue'},
        {'col1': 6, 'time_since_activity': 9000, 'twice_total_activities': 14, 'color': 'blue'},
        {'col1': 7, 'time_since_activity': 10500, 'twice_total_activities': 16, 'color': 'blue'},
        {'col1': 8, 'time_since_activity': 12000, 'twice_total_activities': 18, 'color': 'blue'},
        {'col1': 9, 'time_since_activity': 13500, 'twice_total_activities': 20, 'color': 'blue'}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    expected_cache = {'color': 'blue', 'age': 41, 'iss': True, 'sometime': test_base_time, 'first_activity': test_base_time,
                      'last_event': test_base_time + timedelta(minutes=25 * (items_in_ingest_batch - 1)), 'total_activities': 10,
                      'twice_total_activities': 20, 'min': 1, 'Avg': 3}

    actual_cache = get_key_all_attrs_test_helper(setup_teardown_test, 'tal')
    assert expected_cache == actual_cache


def test_write_table_metadata_columns(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    table['tal'] = {'color': 'blue', 'age': 41, 'iss': True, 'sometime': test_base_time}

    def enrich(event, state):
        if 'first_activity' not in state:
            state['first_activity'] = event.time

        event.body['time_since_activity'] = (event.time - state['first_activity']).seconds
        state['last_event'] = event.time
        state['total_activities'] = state.get('total_activities', 0) + 1
        event.body['color'] = state['color']
        event.body['twice_total_activities'] = state['total_activities'] * 2
        return event, state

    controller = build_flow([
        SyncEmitSource(),
        MapWithState(table, enrich, group_by_key=True, full_event=True),
        NoSqlTarget(table, columns=['twice_total_activities', 'my_key=$key']),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {'col1': 0, 'time_since_activity': 0, 'twice_total_activities': 2, 'color': 'blue'},
        {'col1': 1, 'time_since_activity': 1500, 'twice_total_activities': 4, 'color': 'blue'},
        {'col1': 2, 'time_since_activity': 3000, 'twice_total_activities': 6, 'color': 'blue'},
        {'col1': 3, 'time_since_activity': 4500, 'twice_total_activities': 8, 'color': 'blue'},
        {'col1': 4, 'time_since_activity': 6000, 'twice_total_activities': 10, 'color': 'blue'},
        {'col1': 5, 'time_since_activity': 7500, 'twice_total_activities': 12, 'color': 'blue'},
        {'col1': 6, 'time_since_activity': 9000, 'twice_total_activities': 14, 'color': 'blue'},
        {'col1': 7, 'time_since_activity': 10500, 'twice_total_activities': 16, 'color': 'blue'},
        {'col1': 8, 'time_since_activity': 12000, 'twice_total_activities': 18, 'color': 'blue'},
        {'col1': 9, 'time_since_activity': 13500, 'twice_total_activities': 20, 'color': 'blue'}]

    assert actual == expected_results, \
        f'actual did not match expected. \n actual: {actual} \n expected: {expected_results}'

    expected_cache = {'color': 'blue', 'age': 41, 'iss': True, 'sometime': test_base_time, 'first_activity': test_base_time,
                      'last_event': test_base_time + timedelta(minutes=25 * (items_in_ingest_batch - 1)), 'total_activities': 10,
                      'twice_total_activities': 20, 'my_key': 'tal'}
    actual_cache = get_key_all_attrs_test_helper(setup_teardown_test, 'tal')
    assert expected_cache == actual_cache


async def get_kv_item(full_path, key):
    try:
        headers = V3ioHeaders()
        container, path = full_path.split('/', 1)

        _v3io_client = v3io.aio.dataplane.Client(endpoint=headers._webapi_url, access_key=headers._access_key)
        response = await _v3io_client.kv.get(container, path, key,
                                             raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
        return response
    finally:
        await _v3io_client.close()


def test_writing_int_key(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    df = pd.DataFrame({"num": [0, 1, 2], "color": ["green", "blue", "red"]})

    controller = build_flow([
        DataframeSource(df, key_field='num'),
        NoSqlTarget(table),

    ]).run()
    controller.await_termination()


def test_writing_timedelta_key(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    df = pd.DataFrame({"key": ['a', 'b'], "timedelta": [pd.Timedelta("-1 days 2 min 3us"), pd.Timedelta("P0DT0H1M0S")]})

    controller = build_flow([
        DataframeSource(df, key_field='key'),
        NoSqlTarget(table),

    ]).run()
    controller.await_termination()


def test_write_two_keys_to_v3io_from_df(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    data = pd.DataFrame(
        {
            'first_name': ['moshe', 'yosi'],
            'last_name': ['cohen', 'levi'],
            'city': ['tel aviv', 'ramat gan'],
        }
    )

    keys = ['first_name', 'last_name']
    controller = build_flow([
        DataframeSource(data, key_field=keys),
        NoSqlTarget(table),
    ]).run()
    controller.await_termination()

    expected = {'city': 'tel aviv', 'first_name': 'moshe', 'last_name': 'cohen'}
    actual = get_key_all_attrs_test_helper(setup_teardown_test, 'moshe.cohen')
    assert expected == actual


# ML-775
def test_write_three_keys_to_v3io_from_df(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    data = pd.DataFrame(
        {
            'first_name': ['moshe', 'yosi'],
            'middle_name': ['tuna', 'fluffy'],
            'last_name': ['cohen', 'levi'],
            'city': ['tel aviv', 'ramat gan'],
        }
    )

    keys = ['first_name', 'middle_name', 'last_name']
    controller = build_flow([
        DataframeSource(data, key_field=keys),
        NoSqlTarget(table),
    ]).run()
    controller.await_termination()

    expected = {'city': 'tel aviv', 'first_name': 'moshe', 'middle_name': 'tuna', 'last_name': 'cohen'}
    actual = get_key_all_attrs_test_helper(setup_teardown_test, 'moshe.dc1e617eaae40eea3449baf3795bb5b50676c963')
    assert expected == actual


def test_write_string_as_time_via_time_field(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    t1 = '2020-03-16T05:00:00+00:00'
    t2 = '2020-03-15T18:00:00+00:00'
    df = pd.DataFrame(
        {
            'name': ['jack', 'tuna'],
            'time': [t1, t2],
        }
    )

    controller = build_flow([
        DataframeSource(df, key_field='name', time_field='time'),
        NoSqlTarget(table, columns=['name', '$time']),
    ]).run()
    controller.await_termination()

    expected = {'name': 'tuna', 'time': datetime.fromisoformat(t2)}
    actual = get_key_all_attrs_test_helper(setup_teardown_test, 'tuna')
    assert expected == actual


def test_write_string_as_time_via_schema(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    t1 = '2020-03-16T05:00:00+00:00'
    t2 = '2020-03-15T18:00:00+00:00'
    df = pd.DataFrame(
        {
            'name': ['jack', 'tuna'],
            'time': [t1, t2],
        }
    )

    controller = build_flow([
        DataframeSource(df, key_field='name'),
        NoSqlTarget(table, columns=[('name', 'str'), ('time', 'datetime')]),
    ]).run()
    controller.await_termination()

    expected = {'name': 'tuna', 'time': datetime.fromisoformat(t2)}
    actual = get_key_all_attrs_test_helper(setup_teardown_test, 'tuna')
    assert expected == actual


def test_write_multiple_keys_to_v3io_from_csv(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow([
        CSVSource('tests/test.csv', header=True, key_field=['n1', 'n2'], build_dict=True),
        NoSqlTarget(table),
    ]).run()
    controller.await_termination()

    expected = {'n1': 1, 'n2': 2, 'n3': 3}
    actual = get_key_all_attrs_test_helper(setup_teardown_test, '1.2')
    assert expected == actual

    expected = {'n1': 4, 'n2': 5, 'n3': 6}
    actual = get_key_all_attrs_test_helper(setup_teardown_test, '4.5')
    assert expected == actual


def test_write_multiple_keys_to_v3io(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow([
        SyncEmitSource(key_field=['n1', 'n2']),
        NoSqlTarget(table),
    ]).run()

    controller.emit({'n1': 1, 'n2': 2, 'n3': 3})
    controller.emit({'n1': 4, 'n2': 5, 'n3': 6})

    controller.terminate()
    controller.await_termination()

    expected = {'n1': 1, 'n2': 2, 'n3': 3}
    actual = get_key_all_attrs_test_helper(setup_teardown_test, '1.2')
    assert expected == actual

    expected = {'n1': 4, 'n2': 5, 'n3': 6}
    actual = get_key_all_attrs_test_helper(setup_teardown_test, '4.5')
    assert expected == actual


def test_write_none_time(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    data = pd.DataFrame(
        {
            'first_name': ['moshe', 'yosi'],
            'color': ['blue', 'yellow'],
            'time': [test_base_time, None]
        }
    )

    def set_moshe_time_to_none(data):
        if data['first_name'] == 'moshe':
            data['time'] = pd.NaT
        return data

    controller = build_flow([
        DataframeSource(data, key_field='first_name'),
        NoSqlTarget(table),
        Map(set_moshe_time_to_none),
        NoSqlTarget(table),
    ]).run()
    controller.await_termination()

    expected = {'first_name': 'yosi', 'color': 'yellow'}
    actual = get_key_all_attrs_test_helper(setup_teardown_test, 'yosi')
    assert expected == actual

    expected = {'first_name': 'moshe', 'color': 'blue'}
    actual = get_key_all_attrs_test_helper(setup_teardown_test, 'moshe')
    assert expected == actual


def test_cache_flushing(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver(), flush_interval_secs=3)
    controller = build_flow([
        SyncEmitSource(),
        NoSqlTarget(table),
    ]).run()

    controller.emit({'col1': 0}, 'dina', test_base_time + timedelta(minutes=25))

    response = None
    if setup_teardown_test.driver_name == "RedisDriver":
        response = _get_redis_kv_all_attrs(setup_teardown_test, 'dina')
    else:
        response = asyncio.run(get_kv_item(setup_teardown_test.table_name, 'dina')).output.item
    assert response == {}

    time.sleep(4)

    response = None
    if setup_teardown_test.driver_name == "RedisDriver":
        response = _get_redis_kv_all_attrs(setup_teardown_test, 'dina')
    else:
        response = asyncio.run(get_kv_item(setup_teardown_test.table_name, 'dina')).output.item
    assert response == {'col1': 0}

    controller.terminate()
    controller.await_termination()


def test_write_empty_df(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    df = pd.DataFrame({})

    controller = build_flow([
        DataframeSource(df),
        NoSqlTarget(table),
    ]).run()
    controller.await_termination()
