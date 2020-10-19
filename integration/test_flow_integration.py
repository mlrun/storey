import asyncio
import base64
import json
import time

import aiohttp

from storey import Filter, JoinWithV3IOTable, JoinWithHttp, Map, Reduce, Source, HttpRequest, build_flow, \
    WriteToV3IOStream, V3ioDriver
from .integration_test_utils import V3ioHeaders


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
        JoinWithV3IOTable(V3ioDriver(), lambda x: x.body, lambda x, y: y['secret'], table_path),
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
        JoinWithHttp(lambda _: HttpRequest('GET', 'https://google.com', ''), lambda _, response: response.status),
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
