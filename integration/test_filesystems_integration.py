import asyncio
from .integration_test_utils import setup_teardown_test, _generate_table_name, V3ioHeaders, V3ioError
from storey import build_flow, ReadCSV, WriteToCSV, Source, Reduce, Map, FlatMap, AsyncSource, WriteToParquet
import pandas as pd
import aiohttp
import pytest
import v3io
import uuid


@pytest.fixture()
def v3io_create_csv():
    # Setup
    file_path = _generate_table_name('bigdata/csv_test')

    asyncio.run(_write_test_csv(file_path))

    # Test runs
    yield file_path

    # Teardown
    asyncio.run(_delete_file(file_path))


@pytest.fixture()
def v3io_teardown_file():
    # Setup
    file_path = _generate_table_name('bigdata/csv_test')

    # Test runs
    yield file_path

    # Teardown
    asyncio.run(_delete_file(file_path))


async def _write_test_csv(file_path):
    connector = aiohttp.TCPConnector()
    v3io_access = V3ioHeaders()
    client_session = aiohttp.ClientSession(connector=connector)
    try:
        data = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
        await client_session.put(f'{v3io_access._webapi_url}/{file_path}', data=data,
                                 headers=v3io_access._get_put_file_headers, ssl=False)
    finally:
        await client_session.close()


async def _delete_file(path):
    connector = aiohttp.TCPConnector()
    v3io_access = V3ioHeaders()
    client_session = aiohttp.ClientSession(connector=connector)
    try:
        response = await client_session.delete(f'{v3io_access._webapi_url}/{path}',
                                               headers=v3io_access._get_put_file_headers, ssl=False)
        if response.status >= 300 and response.status != 404 and response.status != 409:
            body = await response.text()
            raise V3ioError(f'Failed to delete item at {path}. Response status code was {response.status}: {body}')
    finally:
        await client_session.close()


def test_csv_reader_from_v3io(v3io_create_csv):
    controller = build_flow([
        ReadCSV(f'v3io:///{v3io_create_csv}', header=True),
        FlatMap(lambda x: x),
        Map(lambda x: int(x)),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    termination_result = controller.await_termination()
    assert termination_result == 495


def test_csv_reader_from_v3io_error_on_file_not_found():
    controller = build_flow([
        ReadCSV('v3io:///bigdatra/tests/idontexist.csv', header=True),
    ]).run()

    try:
        controller.await_termination()
        assert False
    except FileNotFoundError:
        pass


async def async_test_write_csv_to_v3io(v3io_teardown_csv):
    controller = await build_flow([
        AsyncSource(),
        WriteToCSV(f'v3io:///{v3io_teardown_csv}', columns=['n', 'n*10'], header=True)
    ]).run()

    for i in range(10):
        await controller.emit([i, 10 * i])

    await controller.terminate()
    await controller.await_termination()

    v3io_access = V3ioHeaders()
    v3io_client = v3io.aio.dataplane.Client(endpoint=v3io_access._webapi_url, access_key=v3io_access._access_key)
    try:
        container, path = v3io_teardown_csv.split('/', 1)
        result = await v3io_client.object.get(container, path)
    finally:
        await v3io_client.close()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result.body.decode("utf-8") == expected


def test_write_csv_to_v3io(v3io_teardown_file):
    asyncio.run(async_test_write_csv_to_v3io(v3io_teardown_file))


def test_write_csv_with_dict_to_v3io(v3io_teardown_file):
    file_path = f'v3io:///{v3io_teardown_file}'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, columns=['n', 'n*10'], header=True)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i})

    controller.terminate()
    controller.await_termination()

    v3io_access = V3ioHeaders()
    v3io_client = v3io.dataplane.Client(endpoint=v3io_access._webapi_url, access_key=v3io_access._access_key)
    try:
        container, path = v3io_teardown_file.split('/', 1)
        result = v3io_client.object.get(container, path)
    finally:
        v3io_client.close()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result.body.decode("utf-8") == expected


def test_write_csv_infer_columns_without_header_to_v3io(v3io_teardown_file):
    file_path = f'v3io:///{v3io_teardown_file}'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i})

    controller.terminate()
    controller.await_termination()

    v3io_access = V3ioHeaders()
    v3io_client = v3io.dataplane.Client(endpoint=v3io_access._webapi_url, access_key=v3io_access._access_key)
    try:
        container, path = v3io_teardown_file.split('/', 1)
        result = v3io_client.object.get(container, path)
    finally:
        v3io_client.close()

    expected = "0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result.body.decode("utf-8") == expected


def test_write_csv_from_lists_with_metadata_and_column_pruning_to_v3io(v3io_teardown_file):
    file_path = f'v3io:///{v3io_teardown_file}'
    controller = build_flow([
        Source(),
        WriteToCSV(file_path, columns=['event_key=$key', 'n*10'], header=True)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i}, key=f'key{i}')

    controller.terminate()
    controller.await_termination()

    v3io_access = V3ioHeaders()
    v3io_client = v3io.dataplane.Client(endpoint=v3io_access._webapi_url, access_key=v3io_access._access_key)
    try:
        container, path = v3io_teardown_file.split('/', 1)
        result = v3io_client.object.get(container, path)
    finally:
        v3io_client.close()

    expected = "event_key,n*10\nkey0,0\nkey1,10\nkey2,20\nkey3,30\nkey4,40\nkey5,50\nkey6,60\nkey7,70\nkey8,80\nkey9,90\n"
    assert result.body.decode("utf-8") == expected


def test_write_to_parquet_to_v3io(setup_teardown_test):
    out_dir = f'v3io:///{setup_teardown_test}'
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


def test_write_to_parquet_to_v3io_single_file_on_termination(setup_teardown_test):
    out_file = f'v3io:///{setup_teardown_test}/out.parquet'
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


def test_write_to_parquet_to_v3io_with_indices(setup_teardown_test):
    out_file = f'v3io:///{setup_teardown_test}/test_write_to_parquet_with_indices{uuid.uuid4().hex}.parquet'
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
