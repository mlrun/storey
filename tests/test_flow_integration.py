from .conftest import has_v3io_creds
from storey import Filter, JoinWithV3IOTable, JoinWithHttp, Map, Reduce, Source, NeedsV3ioAccess, HttpRequest, build_flow

import aiohttp
import asyncio
import json
import pytest
import time


class Setup(NeedsV3ioAccess):
    async def setup(self, table_path):
        connector = aiohttp.TCPConnector()
        client_session = aiohttp.ClientSession(connector=connector)
        for i in range(1, 10):
            request_body = json.dumps({'Item': {'secret': {'N': f'{10 - i}'}}})
            response = await client_session.request(
                'PUT', f'{self._webapi_url}/{table_path}/{i}', headers=self._put_item_headers, data=request_body, ssl=False)
            assert response.status == 200, f'Bad response {response} to request {request_body}'


@pytest.mark.skipif(not has_v3io_creds, reason='missing v3io credentials')
def test_join_with_v3io_table():
    table_path = f'bigdata/{int(time.time_ns() / 1000)}'
    asyncio.run(Setup().setup(table_path))
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 8),
        JoinWithV3IOTable(lambda x: x, lambda x, y: y['secret'], table_path),
        Reduce(0, lambda x, y: x + y)
    ]).run()
    for i in range(10):
        controller.emit(i)

    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 42


@pytest.mark.skipif(not has_v3io_creds, reason='missing v3io credentials')
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
