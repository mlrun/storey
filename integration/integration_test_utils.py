import aiohttp
import json
import random
import asyncio
import string
import pytest

from storey import NeedsV3ioAccess
from storey.flow import _convert_nginx_to_python_type, V3ioError


def _generate_table_name(prefix='bigdata/aggr_test'):
    random_table = ''.join([random.choice(string.ascii_letters) for i in range(10)])
    return f'{prefix}/{random_table}'


@pytest.fixture()
def setup_teardown_test():
    # Setup
    table_name = _generate_table_name()

    # Test runs
    yield table_name

    # Teardown
    asyncio.run(recursive_delete(table_name, NeedsV3ioAccess()))


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
            elif response.status != 404:
                raise V3ioError(f'Failed to delete table {path}. Response status code was {response.status}: {body}')

        await _delete_item(f'{v3io_access._webapi_url}/{path}/', v3io_access, client_session)
    finally:
        await client_session.close()


async def _delete_item(path, v3io_access, client_session):
    response = await client_session.delete(path, headers=v3io_access._get_put_file_headers, ssl=False)
    if response.status >= 300 and response.status != 404 and response.status != 409:
        body = await response.text()
        raise V3ioError(f'Failed to delete item at {path}. Response status code was {response.status}: {body}')
