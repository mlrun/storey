import asyncio
import base64
import json
import time
import pandas as pd
import v3io_frames as frames

import aiohttp

from _datetime import datetime

from storey import Filter, JoinWithV3IOTable, SendToHttp, Map, Reduce, Source, HttpRequest, build_flow, \
    WriteToV3IOStream, V3ioDriver, WriteToTSDB, Batch, Table, JoinWithTable
from .integration_test_utils import V3ioHeaders, append_return, setup_kv_teardown_test


class SetupKvTable(V3ioHeaders):
    async def setup(self, table_path):
        connector = aiohttp.TCPConnector()
        client_session = aiohttp.ClientSession(connector=connector)
        for i in range(1, 10):
            request_body = json.dumps({'Item': {'secret': {'N': f'{10 - i}'}}})
            response = await client_session.request(
                'PUT', f'{self._webapi_url}/{table_path}/{i}', headers=self._put_item_headers, data=request_body, ssl=False)
            assert response.status == 200, f'Bad response {await response.text()} to request {request_body}'


class SetupStream(V3ioHeaders):
    async def setup(self, stream_path):
        connector = aiohttp.TCPConnector()
        client_session = aiohttp.ClientSession(connector=connector)
        request_body = json.dumps({"ShardCount": 2, "RetentionPeriodHours": 1})
        response = await client_session.request(
            'POST', f'{self._webapi_url}/{stream_path}/', headers=self._create_stream_headers, data=request_body, ssl=False)
        assert response.status == 204, f'Bad response {await response.text()} to request {request_body}'


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


def test_join_with_v3io_table():
    table_path = f'bigdata/test_join_with_v3io_table/{int(time.time_ns() / 1000)}'
    asyncio.run(SetupKvTable().setup(table_path))
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 8),
        JoinWithV3IOTable(V3ioDriver(), lambda x: x, lambda x, y: y['secret'], table_path),
        Reduce(0, lambda x, y: x + y)
    ]).run()
    for i in range(10):
        controller.emit(i)

    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 42


def test_join_with_http():
    controller = build_flow([
        Source(),
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


def test_write_to_v3io_stream():
    stream_path = f'bigdata/test_write_to_v3io_stream/{int(time.time_ns() / 1000)}/'
    asyncio.run(SetupStream().setup(stream_path))
    controller = build_flow([
        Source(),
        Map(lambda x: str(x)),
        WriteToV3IOStream(V3ioDriver(), stream_path, sharding_func=lambda event: int(event.body))
    ]).run()
    for i in range(10):
        controller.emit(i)

    controller.terminate()
    controller.await_termination()
    shard0_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/0'))
    assert shard0_data == [b'0', b'2', b'4', b'6', b'8']
    shard1_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/1'))
    assert shard1_data == [b'1', b'3', b'5', b'7', b'9']


def test_write_to_v3io_stream_unbalanced():
    stream_path = f'bigdata/test_write_to_v3io_stream/{int(time.time_ns() / 1000)}/'
    asyncio.run(SetupStream().setup(stream_path))
    controller = build_flow([
        Source(),
        Map(lambda x: str(x)),
        WriteToV3IOStream(V3ioDriver(), stream_path, sharding_func=lambda event: 0)
    ]).run()
    for i in range(10):
        controller.emit(i)

    controller.terminate()
    controller.await_termination()
    shard0_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/0'))
    assert shard0_data == [b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9']
    shard1_data = asyncio.run(GetShardData().get_shard_data(f'{stream_path}/1'))
    assert shard1_data == []


def test_write_to_tsdb():
    tsdb_path = f'tsdb_path-{int(time.time_ns() / 1000)}'
    columns = ['cpu', 'disk', 'time', 'node']

    controller = build_flow([
        Source(),
        WriteToTSDB(path=tsdb_path, time_col='time', labels_cols='node', columns=columns, rate='1/h', max_events=2)
    ]).run()

    expected = []
    date_time_str = '18/09/19 01:55:1'
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + ' UTC-0000', '%d/%m/%y %H:%M:%S UTC%z')
        controller.emit([i + 1, i + 2, now, i])
        expected.append([float(i + 1), float(i + 2), now, i])

    controller.terminate()
    controller.await_termination()

    client = frames.Client()
    res = client.read("tsdb", tsdb_path, start='0', end='now')
    res = res.sort_values(['time'])
    res['node'] = pd.to_numeric(res["node"])
    df = pd.DataFrame(expected, columns=columns)
    df.set_index(keys=['time'], inplace=True)
    assert res.equals(df), f"result{res}\n!=\nexpected{df}"


def test_write_to_tsdb_with_metadata_label():
    tsdb_path = f'tsdb_path-{int(time.time_ns() / 1000)}'
    columns = ['cpu', 'disk', 'time', 'node']

    controller = build_flow([
        Source(),
        WriteToTSDB(path=tsdb_path, time_col='time', labels_cols='node', columns=columns, metadata_columns=['time'], rate='1/h',
                    max_events=2)
    ]).run()

    expected = []
    date_time_str = '18/09/19 01:55:1'
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + ' UTC-0000', '%d/%m/%y %H:%M:%S UTC%z')
        controller.emit([i + 1, i + 2, i], event_time=now)
        expected.append([float(i + 1), float(i + 2), now, i])

    controller.terminate()
    controller.await_termination()

    client = frames.Client()
    res = client.read("tsdb", tsdb_path, start='0', end='now')
    res = res.sort_values(['time'])
    res['node'] = pd.to_numeric(res["node"])
    df = pd.DataFrame(expected, columns=columns)
    df.set_index(keys=['time'], inplace=True)
    assert res.equals(df), f"result{res}\n!=\nexpected{df}"


def test_join_by_key(setup_kv_teardown_test):
    table = Table(setup_kv_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        Filter(lambda x: x['col1'] > 8),
        JoinWithTable(table, lambda x: x['col1']),
        Reduce([], lambda acc, x: append_return(acc, x))
    ]).run()
    for i in range(10):
        controller.emit({'col1': i})

    expected = [{'col1': 9, 'age': 1, 'color': 'blue9'}]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected


def test_join_by_key_specific_attributes(setup_kv_teardown_test):
    table = Table(setup_kv_teardown_test, V3ioDriver())

    controller = build_flow([
        Source(),
        Filter(lambda x: x['col1'] > 8),
        JoinWithTable(table, lambda x: x['col1'], attributes=['age']),
        Reduce([], lambda acc, x: append_return(acc, x))
    ]).run()
    for i in range(10):
        controller.emit({'col1': i})

    expected = [{'col1': 9, 'age': 1}]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected
