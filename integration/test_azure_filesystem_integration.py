import asyncio
import os
import uuid

import pandas as pd
import pytest

from storey import build_flow, CSVSource, CSVTarget, EmitSource, Reduce, Map, FlatMap, ParquetTarget
from .integration_test_utils import _generate_table_name

has_azure_credentials = os.getenv("AZURE_ACCOUNT_NAME") and os.getenv("AZURE_ACCOUNT_KEY") and os.getenv("AZURE_BLOB_STORE")
if has_azure_credentials:
    storage_options = {"account_name": os.getenv("AZURE_ACCOUNT_NAME"), "account_key": os.getenv("AZURE_ACCOUNT_KEY")}
    from adlfs import AzureBlobFileSystem


@pytest.fixture()
def azure_create_csv():
    # Setup
    azure_blob = os.getenv("AZURE_BLOB_STORE")
    file_path = _generate_table_name(f'{azure_blob}/az_storey')

    _write_test_csv(file_path)

    # Test runs
    yield file_path

    # Teardown
    _delete_file(file_path)


@pytest.fixture()
def azure_teardown_file():
    # Setup
    azure_blob = os.getenv("AZURE_BLOB_STORE")
    file_path = _generate_table_name(f'{azure_blob}/az_storey')

    # Test runs
    yield file_path

    # Teardown
    _delete_file(file_path)


@pytest.fixture()
def azure_setup_teardown_test():
    # Setup
    table_name = _generate_table_name(f'{os.getenv("AZURE_BLOB_STORE")}/test')

    # Test runs
    yield table_name

    # Teardown
    azure_recursive_delete(table_name)


def _write_test_csv(file_path):
    az_fs = AzureBlobFileSystem(**storage_options)
    data = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    with az_fs.open(file_path, 'w') as f:
        f.write(data)


def _delete_file(path):
    az_fs = AzureBlobFileSystem(**storage_options)
    az_fs.delete(path)


def azure_recursive_delete(path):
    az_fs = AzureBlobFileSystem(**storage_options)
    az_fs.rm(path, True)


@pytest.mark.skipif(not has_azure_credentials, reason='No azure credentials found')
def test_csv_reader_from_azure(azure_create_csv):
    controller = build_flow([
        CSVSource(f'az:///{azure_create_csv}', header=True, storage_options=storage_options),
        FlatMap(lambda x: x),
        Map(lambda x: int(x)),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    termination_result = controller.await_termination()
    assert termination_result == 495


@pytest.mark.skipif(not has_azure_credentials, reason='No azure credentials found')
def test_csv_reader_from_azure_error_on_file_not_found():
    controller = build_flow([
        CSVSource(f'az:///{os.getenv("AZURE_BLOB_STORE")}/idontexist.csv', header=True, storage_options=storage_options),
    ]).run()

    try:
        controller.await_termination()
        assert False
    except FileNotFoundError:
        pass


async def async_test_write_csv_to_azure(azure_teardown_csv):
    controller = build_flow([
        EmitSource(),
        CSVTarget(f'az:///{azure_teardown_csv}', columns=['n', 'n*10'], header=True, storage_options=storage_options)
    ]).run()

    for i in range(10):
        await controller.emit([i, 10 * i])

    await controller.terminate()
    await controller.await_termination()

    actual = AzureBlobFileSystem(**storage_options).open(azure_teardown_csv).read()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert actual.decode("utf-8") == expected


@pytest.mark.skipif(not has_azure_credentials, reason='No azure credentials found')
def test_write_csv_to_azure(azure_teardown_file):
    asyncio.run(async_test_write_csv_to_azure(azure_teardown_file))


@pytest.mark.skipif(not has_azure_credentials, reason='No azure credentials found')
def test_write_csv_with_dict_to_azure(azure_teardown_file):
    file_path = f'az:///{azure_teardown_file}'
    controller = build_flow([
        EmitSource(),
        CSVTarget(file_path, columns=['n', 'n*10'], header=True, storage_options=storage_options)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i})

    controller.terminate()
    controller.await_termination()

    actual = AzureBlobFileSystem(**storage_options).open(azure_teardown_file).read()
    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert actual.decode("utf-8") == expected


@pytest.mark.skipif(not has_azure_credentials, reason='No azure credentials found')
def test_write_csv_infer_columns_without_header_to_azure(azure_teardown_file):
    file_path = f'az:///{azure_teardown_file}'
    controller = build_flow([
        EmitSource(),
        CSVTarget(file_path, storage_options=storage_options)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i})

    controller.terminate()
    controller.await_termination()

    actual = AzureBlobFileSystem(**storage_options).open(azure_teardown_file).read()
    expected = "0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert actual.decode("utf-8") == expected


@pytest.mark.skipif(not has_azure_credentials, reason='No azure credentials found')
def test_write_csv_from_lists_with_metadata_and_column_pruning_to_azure(azure_teardown_file):
    file_path = f'az:///{azure_teardown_file}'
    controller = build_flow([
        EmitSource(),
        CSVTarget(file_path, columns=['event_key=$key', 'n*10'], header=True, storage_options=storage_options)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i}, key=f'key{i}')

    controller.terminate()
    controller.await_termination()

    actual = AzureBlobFileSystem(**storage_options).open(azure_teardown_file).read()
    expected = "event_key,n*10\nkey0,0\nkey1,10\nkey2,20\nkey3,30\nkey4,40\nkey5,50\nkey6,60\nkey7,70\nkey8,80\nkey9,90\n"
    assert actual.decode("utf-8") == expected


@pytest.mark.skipif(not has_azure_credentials, reason='No azure credentials found')
def test_write_to_parquet_to_azure(azure_setup_teardown_test):
    out_dir = f'az:///{azure_setup_teardown_test}'
    columns = ['my_int', 'my_string']
    controller = build_flow([
        EmitSource(),
        ParquetTarget(out_dir, partition_cols='my_int', columns=columns, max_events=1, storage_options=storage_options)
    ]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f'this is {i}'])
        expected.append([i, f'this is {i}'])
    expected = pd.DataFrame(expected, columns=columns, dtype='int32')
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns, storage_options=storage_options)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


@pytest.mark.skipif(not has_azure_credentials, reason='No azure credentials found')
def test_write_to_parquet_to_azure_single_file_on_termination(azure_setup_teardown_test):
    out_file = f'az:///{azure_setup_teardown_test}/out.parquet'
    columns = ['my_int', 'my_string']
    controller = build_flow([
        EmitSource(),
        ParquetTarget(out_file, columns=columns, storage_options=storage_options)
    ]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f'this is {i}'])
        expected.append([i, f'this is {i}'])
    expected = pd.DataFrame(expected, columns=columns, dtype='int64')
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns, storage_options=storage_options)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


@pytest.mark.skipif(not has_azure_credentials, reason='No azure credentials found')
def test_write_to_parquet_to_azure_with_indices(azure_setup_teardown_test):
    out_file = f'az:///{azure_setup_teardown_test}/test_write_to_parquet_with_indices{uuid.uuid4().hex}.parquet'
    controller = build_flow([
        EmitSource(),
        ParquetTarget(out_file, index_cols='event_key=$key', columns=['my_int', 'my_string'], storage_options=storage_options)
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

    read_back_df = pd.read_parquet(out_file, columns=columns, storage_options=storage_options)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"
