import asyncio
import base64
import json
import random
import re
import string
from datetime import datetime

import aiohttp
import pytest

from storey.drivers import NeedsV3ioAccess
from storey.flow import V3ioError

_non_int_char_pattern = re.compile(r"[^-0-9]")
test_base_time = datetime.fromisoformat("2020-07-21T21:40:00+00:00")


class V3ioHeaders(NeedsV3ioAccess):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._get_item_headers = {
            'X-v3io-function': 'GetItem',
            'X-v3io-session-key': self._access_key
        }

        self._get_items_headers = {
            'X-v3io-function': 'GetItems',
            'X-v3io-session-key': self._access_key
        }

        self._put_item_headers = {
            'X-v3io-function': 'PutItem',
            'X-v3io-session-key': self._access_key
        }

        self._update_item_headers = {
            'X-v3io-function': 'UpdateItem',
            'X-v3io-session-key': self._access_key
        }

        self._put_records_headers = {
            'X-v3io-function': 'PutRecords',
            'X-v3io-session-key': self._access_key
        }

        self._create_stream_headers = {
            'X-v3io-function': 'CreateStream',
            'X-v3io-session-key': self._access_key
        }

        self._describe_stream_headers = {
            'X-v3io-function': 'DescribeStream',
            'X-v3io-session-key': self._access_key
        }

        self._seek_headers = {
            'X-v3io-function': 'Seek',
            'X-v3io-session-key': self._access_key
        }

        self._get_records_headers = {
            'X-v3io-function': 'GetRecords',
            'X-v3io-session-key': self._access_key
        }

        self._get_put_file_headers = {
            'X-v3io-session-key': self._access_key
        }


def append_return(lst, x):
    lst.append(x)
    return lst


def _generate_table_name(prefix='bigdata/Aggr_test'):
    random_table = ''.join([random.choice(string.ascii_letters) for i in range(10)])
    return f'{prefix}/{random_table}'


@pytest.fixture()
def setup_teardown_test():
    # Setup
    table_name = _generate_table_name()

    # Test runs
    yield table_name

    # Teardown
    asyncio.run(recursive_delete(table_name, V3ioHeaders()))


@pytest.fixture()
def setup_kv_teardown_test():
    # Setup
    table_path = _generate_table_name()
    asyncio.run(create_temp_kv(table_path))

    # Test runs
    yield table_path

    # Teardown
    asyncio.run(recursive_delete(table_path, V3ioHeaders()))


@pytest.fixture()
def setup_stream_teardown_test():
    # Setup
    stream_path = _generate_table_name('bigdata/stream_test')
    asyncio.run(setup_stream(stream_path))

    # Test runs
    yield stream_path

    # Teardown
    asyncio.run(recursive_delete(stream_path, V3ioHeaders()))


async def setup_stream(stream_path):
    v3io_access = V3ioHeaders()
    connector = aiohttp.TCPConnector()
    client_session = aiohttp.ClientSession(connector=connector)
    request_body = json.dumps({"ShardCount": 2, "RetentionPeriodHours": 1})
    response = await client_session.request(
        'POST', f'{v3io_access._webapi_url}/{stream_path}/', headers=v3io_access._create_stream_headers, data=request_body, ssl=False)
    assert response.status == 204, f'Bad response {await response.text()} to request {request_body}'


async def create_temp_kv(table_path):
    connector = aiohttp.TCPConnector()
    v3io_access = V3ioHeaders()
    client_session = aiohttp.ClientSession(connector=connector)
    for i in range(1, 10):
        request_body = json.dumps({'Item': {'age': {'N': f'{10 - i}'}, 'color': {'S': f'blue{i}'}}})
        response = await client_session.request(
            'PUT', f'{v3io_access._webapi_url}/{table_path}/{i}', headers=v3io_access._put_item_headers, data=request_body, ssl=False)
        assert response.status == 200, f'Bad response {await response.text()} to request {request_body}'


def _v3io_parse_get_items_response(response_body):
    response_object = json.loads(response_body)
    i = 0
    for item in response_object['Items']:
        parsed_item = {}
        for name, type_to_value in item.items():
            for typ, value in type_to_value.items():
                val = _convert_nginx_to_python_type(typ, value)
                parsed_item[name] = val
        response_object['Items'][i] = parsed_item
        i = i + 1
    return response_object


# Deletes the entire table
async def recursive_delete(path, v3io_access):
    connector = aiohttp.TCPConnector()
    client_session = aiohttp.ClientSession(connector=connector)

    try:
        has_more = True
        next_marker = ''
        while has_more:
            get_items_body = {'AttributesToGet': '__name', 'Marker': next_marker}
            response = await client_session.put(f'{v3io_access._webapi_url}/{path}/',
                                                headers=v3io_access._get_items_headers, data=json.dumps(get_items_body), ssl=False)
            body = await response.text()
            if response.status == 200:
                res = _v3io_parse_get_items_response(body)
                for item in res['Items']:
                    await _delete_item(f'{v3io_access._webapi_url}/{path}/{item["__name"]}', v3io_access, client_session)

                has_more = 'NextMarker' in res
                if has_more:
                    next_marker = res['NextMarker']
            elif response.status == 404:
                break
            else:
                raise V3ioError(f'Failed to delete table {path}. Response status code was {response.status}: {body}')

        await _delete_item(f'{v3io_access._webapi_url}/{path}/', v3io_access, client_session)
    finally:
        await client_session.close()


async def _delete_item(path, v3io_access, client_session):
    response = await client_session.delete(path, headers=v3io_access._get_put_file_headers, ssl=False)
    if response.status >= 300 and response.status != 404 and response.status != 409:
        body = await response.text()
        raise V3ioError(f'Failed to delete item at {path}. Response status code was {response.status}: {body}')


def _v3io_parse_get_item_response(response_body):
    response_object = json.loads(response_body)["Item"]
    for name, type_to_value in response_object.items():
        val = None
        for typ, value in type_to_value.items():
            val = _convert_nginx_to_python_type(typ, value)
        response_object[name] = val
    return response_object


def _convert_nginx_to_python_type(typ, value):
    if typ == 'S' or typ == 'BOOL':
        return value
    elif typ == 'N':
        if _non_int_char_pattern.search(value):
            return float(value)
        else:
            return int(value)
    elif typ == 'B':
        return base64.b64decode(value)
    elif typ == 'TS':
        splits = value.split(':', 1)
        secs = int(splits[0])
        nanosecs = int(splits[1])
        return datetime.utcfromtimestamp(secs + nanosecs / 1000000000)
    else:
        raise V3ioError(f'Type {typ} in get item response is not supported')
