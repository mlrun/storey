import asyncio
import sys
import random as rand
import os

from .integration_test_utils import setup_teardown_test, _generate_table_name, V3ioHeaders, V3ioError
from storey import build_flow, CSVSource, CSVTarget, SyncEmitSource, Reduce, Map, FlatMap, AsyncEmitSource, ParquetTarget, ParquetSource, \
    DataframeSource, ReduceToDataFrame
import pandas as pd
import aiohttp
import pytest
import v3io
import uuid
import datetime


@pytest.fixture()
def v3io_create_csv():
    # Setup
    file_path = _generate_table_name('bigdata/storey_ci/csv_test') + 'file.csv'

    asyncio.run(_write_test_csv(file_path))

    # Test runs
    yield file_path

    # Teardown
    asyncio.run(_delete_file(file_path))


@pytest.fixture()
def v3io_teardown_file():
    # Setup
    file_path = _generate_table_name('bigdata/storey_ci/csv_test')

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
        CSVSource(f'v3io:///{v3io_create_csv}', header=True),
        FlatMap(lambda x: x),
        Map(lambda x: int(x)),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    termination_result = controller.await_termination()
    assert termination_result == 495


def test_csv_reader_from_v3io_error_on_file_not_found():
    controller = build_flow([
        CSVSource('v3io:///bigdatra/tests/idontexist.csv', header=True),
    ]).run()

    try:
        controller.await_termination()
        assert False
    except FileNotFoundError:
        pass


async def async_test_write_csv_to_v3io(v3io_teardown_csv):
    controller = build_flow([
        AsyncEmitSource(),
        CSVTarget(f'v3io:///{v3io_teardown_csv}', columns=['n', 'n*10'], header=True)
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
        SyncEmitSource(),
        CSVTarget(file_path, columns=['n', 'n*10'], header=True)
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
        SyncEmitSource(),
        CSVTarget(file_path)
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
        SyncEmitSource(),
        CSVTarget(file_path, columns=['event_key=$key', 'n*10'], header=True)
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
    out_dir = f'v3io:///{setup_teardown_test["table_name"]}'
    columns = ['my_int', 'my_string']
    controller = build_flow([
        SyncEmitSource(),
        ParquetTarget(out_dir, partition_cols='my_int', columns=columns, max_events=1)
    ]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f'this is {i}'])
        expected.append([i, f'this is {i}'])
    expected_in_pyarrow1 = pd.DataFrame(expected, columns=columns)
    expected_in_pyarrow3 = expected_in_pyarrow1.copy()
    expected_in_pyarrow1['my_int'] = expected_in_pyarrow1['my_int'].astype('int32')
    expected_in_pyarrow3['my_int'] = expected_in_pyarrow3['my_int'].astype('category')

    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    assert read_back_df.equals(expected_in_pyarrow1) or read_back_df.equals(expected_in_pyarrow3)


def test_write_to_parquet_to_v3io_single_file_on_termination(setup_teardown_test):
    out_file = f'v3io:///{setup_teardown_test["table_name"]}/out.parquet'
    columns = ['my_int', 'my_string']
    controller = build_flow([
        SyncEmitSource(),
        ParquetTarget(out_file, columns=columns)
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


# ML-775
def test_write_to_parquet_key_hash_partitioning(setup_teardown_test):
    out_dir = f'v3io:///{setup_teardown_test["table_name"]}/test_write_to_parquet_default_partitioning{uuid.uuid4().hex}/'
    controller = build_flow([
        SyncEmitSource(key_field=1),
        ParquetTarget(out_dir, columns=['my_int', 'my_string'], partition_cols=[('$key', 4)])
    ]).run()

    expected = []
    expected_buckets = [3, 0, 1, 3, 0, 3, 1, 1, 1, 2]
    for i in range(10):
        controller.emit([i, f'this is {i}'])
        expected.append([i, f'this is {i}', expected_buckets[i]])
    expected = pd.DataFrame(expected, columns=['my_int', 'my_string', 'hash4_key'])
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir)
    read_back_df['hash4_key'] = read_back_df['hash4_key'].astype('int64')
    read_back_df.sort_values('my_int', inplace=True)
    read_back_df.reset_index(inplace=True, drop=True)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


# ML-701
def test_write_to_parquet_to_v3io_force_string_to_timestamp(setup_teardown_test):
    out_file = f'v3io:///{setup_teardown_test["table_name"]}/out.parquet'
    columns = ['time']
    controller = build_flow([
        SyncEmitSource(),
        ParquetTarget(out_file, columns=[('time', 'datetime')])
    ]).run()

    expected = []
    for i in range(10):
        t = '2021-03-02T19:45:00'
        controller.emit([t])
        expected.append([datetime.datetime.fromisoformat(t)])
    expected = pd.DataFrame(expected, columns=columns)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    assert read_back_df.equals(expected)


def test_write_to_parquet_to_v3io_with_indices(setup_teardown_test):
    out_file = f'v3io:///{setup_teardown_test["table_name"]}/test_write_to_parquet_with_indices{uuid.uuid4().hex}.parquet'
    controller = build_flow([
        SyncEmitSource(),
        ParquetTarget(out_file, index_cols='event_key=$key', columns=['my_int', 'my_string'])
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


# ML-602
def test_write_to_parquet_to_v3io_with_nulls(setup_teardown_test):
    out_dir = f'v3io:///{setup_teardown_test["table_name"]}/test_write_to_parquet_to_v3io_with_nulls{uuid.uuid4().hex}/'
    flow = build_flow([
        SyncEmitSource(),
        ParquetTarget(out_dir, columns=[('key=$key', 'str'), ('my_int', 'int'), ('my_string', 'str'), ('my_datetime', 'datetime')],
                      partition_cols=[], max_events=1)
    ])

    expected = []
    my_time = datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=5)))

    controller = flow.run()
    controller.emit({'my_int': 0, 'my_string': 'hello', 'my_datetime': my_time}, key=f'key1')
    # TODO: Expect correct time zone. Can be done in _Writer, but requires fix for ARROW-10511, which is pyarrow>=3.
    expected.append(['key1', 0, 'hello', my_time.astimezone(datetime.timezone(datetime.timedelta())).replace(tzinfo=None)])
    controller.terminate()
    controller.await_termination()

    controller = flow.run()
    controller.emit({}, key=f'key2')
    expected.append(['key2', None, None, None])
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir)
    read_back_df.sort_values('key', inplace=True)
    read_back_df.reset_index(inplace=True, drop=True)
    expected = pd.DataFrame(expected, columns=['key', 'my_int', 'my_string', 'my_datetime'])
    assert read_back_df.compare(expected).empty


def append_and_return(lst, x):
    lst.append(x)
    return lst


def test_filter_before_after_non_partitioned(setup_teardown_test):
    columns = ['my_string', 'my_time']

    df = pd.DataFrame([['good', pd.Timestamp('2018-05-07 13:52:37')],
                       ['hello', pd.Timestamp('2019-01-26 14:52:37')],
                       ['world', pd.Timestamp('2020-05-11 13:52:37')]],
                      columns=columns)
    df.set_index('my_string')

    out_file = f'v3io:///{setup_teardown_test["table_name"]}/before_after_non_partioned/'
    controller = build_flow([
        DataframeSource(df),
        ParquetTarget(out_file, columns=columns, partition_cols=[]),
    ]).run()
    controller.await_termination()

    before = pd.Timestamp('2019-12-01 00:00:00')
    after = pd.Timestamp('2019-01-01 23:59:59.999999')

    controller = build_flow([
        ParquetSource(out_file, end_filter=before, start_filter=after, filter_column='my_time'),
        Reduce([], append_and_return)
    ]).run()
    read_back_result = controller.await_termination()
    expected = [{'my_string': 'hello', 'my_time': pd.Timestamp('2019-01-26 14:52:37')}]

    assert read_back_result == expected, f"{read_back_result}\n!=\n{expected}"


def test_filter_before_after_partitioned_random(setup_teardown_test):
    low_limit = pd.Timestamp('2018-01-01')
    high_limit = pd.Timestamp('2020-12-31 23:59:59.999999')

    delta = high_limit - low_limit

    seed_value = rand.randrange(sys.maxsize)
    print('Seed value:', seed_value)
    rand.seed(seed_value)

    random_second = rand.randrange(int(delta.total_seconds()))
    middle_limit = low_limit + datetime.timedelta(seconds=random_second)

    print("middle_limit is " + str(middle_limit))

    number_below_middle_limit = rand.randrange(0, 10)

    def create_rand_data(num_elements, low_limit, high_limit, char):
        import datetime
        delta = high_limit - low_limit

        data = []
        for i in range(0, num_elements):
            element = {}
            element['string'] = char + str(i)
            random_second = rand.randrange(int(delta.total_seconds()))
            element['datetime'] = low_limit + datetime.timedelta(seconds=random_second)
            data.append(element)
        return data

    list1 = create_rand_data(number_below_middle_limit, low_limit, middle_limit, 'lower')
    list2 = create_rand_data(10 - number_below_middle_limit, middle_limit, high_limit, 'higher')
    combined_list = list1 + list2

    df = pd.DataFrame(combined_list)
    print("data frame is " + str(df))

    all_partition_columns = ['$year', '$month', '$day', '$hour', '$minute', '$second']
    num_part_columns = rand.randrange(1, 6)
    partition_columns = all_partition_columns[:num_part_columns]
    print("partitioned by " + str(partition_columns))

    out_file = f'v3io:///{setup_teardown_test["table_name"]}/random/'
    controller = build_flow([
        DataframeSource(df, time_field='datetime'),
        ParquetTarget(out_file, columns=['string', 'datetime'], partition_cols=partition_columns),
    ]).run()

    controller.await_termination()

    controller = build_flow([
        ParquetSource(out_file, end_filter=high_limit, start_filter=middle_limit, filter_column='datetime'),
        Reduce([], append_and_return)
    ]).run()
    read_back_result = controller.await_termination()
    print("expecting " + str(10 - number_below_middle_limit) + " to be above middle limit")
    assert (len(read_back_result)) == 10 - number_below_middle_limit

    controller = build_flow([
        ParquetSource(out_file, end_filter=middle_limit, start_filter=low_limit, filter_column='datetime'),
        Reduce([], append_and_return)
    ]).run()
    read_back_result = controller.await_termination()
    print("expecting " + str(number_below_middle_limit) + " to be below middle limit")
    assert (len(read_back_result)) == number_below_middle_limit


def test_filter_before_after_partitioned_inner_other_partition(setup_teardown_test):
    columns = ['my_string', 'my_time', 'my_city']

    df = pd.DataFrame([['hello', pd.Timestamp('2020-12-31 14:05:00'), 'tel aviv'],
                       ['world', pd.Timestamp('2018-12-30 09:00:00'), 'haifa'],
                       ['sun', pd.Timestamp('2019-12-29 09:00:00'), 'tel aviv'],
                       ['is', pd.Timestamp('2019-06-30 15:00:45'), 'hod hasharon'],
                       ['shining', pd.Timestamp('2020-02-28 13:00:56'), 'hod hasharon']],
                      columns=columns)
    df.set_index('my_string')

    out_file = f'v3io:///{setup_teardown_test["table_name"]}/inner_other_partition/'
    controller = build_flow([
        DataframeSource(df, time_field='my_time'),
        ParquetTarget(out_file, columns=columns, partition_cols=['$year', '$month', '$day', '$hour', 'my_city']),
    ]).run()
    controller.await_termination()

    before = pd.Timestamp('2020-12-31 14:00:00')
    after = pd.Timestamp('2019-07-01 00:00:00')

    controller = build_flow([
        ParquetSource(out_file, end_filter=before, start_filter=after, filter_column='my_time', columns=columns),
        Reduce([], append_and_return)
    ]).run()
    read_back_result = controller.await_termination()

    expected = [{'my_string': 'sun', 'my_time': pd.Timestamp('2019-12-29 09:00:00'), 'my_city': 'tel aviv'},
                {'my_string': 'shining', 'my_time': pd.Timestamp('2020-02-28 13:00:56'), 'my_city': 'hod hasharon'}]

    assert read_back_result == expected, f"{read_back_result}\n!=\n{expected}"


def test_filter_before_after_partitioned_outer_other_partition(setup_teardown_test):
    columns = ['my_string', 'my_time', 'my_city']

    df = pd.DataFrame([['shining', pd.Timestamp('2020-12-31 14:00:00'), 'ramat gan'],
                       ['hello', pd.Timestamp('2020-12-30 08:53:00'), 'tel aviv'],
                       ['beautiful', pd.Timestamp('2020-12-30 09:00:00'), 'haifa'],
                       ['sun', pd.Timestamp('2020-12-29 09:00:00'), 'tel aviv'],
                       ['world', pd.Timestamp('2020-12-30 15:00:45'), 'hod hasharon'],
                       ['is', pd.Timestamp('2020-12-31 13:00:56'), 'hod hasharon']],
                      columns=columns)
    df.set_index('my_string')

    out_file = f'v3io:///{setup_teardown_test["table_name"]}/outer_other_partition/'
    controller = build_flow([
        DataframeSource(df, time_field='my_time'),
        ParquetTarget(out_file, columns=columns, partition_cols=['my_city', '$year', '$month', '$day', '$hour']),
    ]).run()
    controller.await_termination()

    start = pd.Timestamp('2020-12-30 08:53:00')
    end = pd.Timestamp('2020-12-31 14:00:00')

    controller = build_flow([
        ParquetSource(out_file, start_filter=start, end_filter=end, filter_column='my_time', columns=columns),
        Reduce([], append_and_return)
    ]).run()
    read_back_result = controller.await_termination()
    expected = [{'my_string': 'beautiful', 'my_time': pd.Timestamp('2020-12-30 09:00:00'), 'my_city': 'haifa'},
                {'my_string': 'world', 'my_time': pd.Timestamp('2020-12-30 15:00:45'), 'my_city': 'hod hasharon'},
                {'my_string': 'is', 'my_time': pd.Timestamp('2020-12-31 13:00:56'), 'my_city': 'hod hasharon'},
                {'my_string': 'shining', 'my_time': pd.Timestamp('2020-12-31 14:00:00'), 'my_city': 'ramat gan'}]

    assert read_back_result == expected, f"{read_back_result}\n!=\n{expected}"


def test_filter_by_time_non_partioned(setup_teardown_test):
    columns = ['my_string', 'my_time', 'my_city']

    df = pd.DataFrame([['dina', pd.Timestamp('2019-07-01 00:00:00'), 'tel aviv'],
                       ['uri', pd.Timestamp('2018-12-30 09:00:00'), 'tel aviv'],
                       ['katya', pd.Timestamp('2020-12-31 14:00:00'), 'hod hasharon']],
                      columns=columns)
    df.set_index('my_string')
    path = '/tmp/test_filter_by_time_non_partioned.parquet'
    df.to_parquet(path)
    start = pd.Timestamp('2019-07-01 00:00:00')
    end = pd.Timestamp('2020-12-31 14:00:00')

    controller = build_flow([
        ParquetSource(path, end_filter=end, start_filter=start, filter_column='my_time'),
        Reduce([], append_and_return)
    ]).run()

    read_back_result = controller.await_termination()

    expected = [{'my_string': 'katya', 'my_time': pd.Timestamp('2020-12-31 14:00:00'), 'my_city': 'hod hasharon'}]

    try:
        assert read_back_result == expected, f"{read_back_result}\n!=\n{expected}"
    finally:
        os.remove(path)


def test_empty_filter_result(setup_teardown_test):
    columns = ['my_string', 'my_time', 'my_city']

    df = pd.DataFrame([['dina', pd.Timestamp('2019-07-01 00:00:00'), 'tel aviv'],
                       ['uri', pd.Timestamp('2018-12-30 09:00:00'), 'tel aviv'],
                       ['katya', pd.Timestamp('2020-12-31 14:00:00'), 'hod hasharon']],
                      columns=columns)
    df.set_index('my_string')
    path = '/tmp/test_filter_by_time_non_partioned.parquet'
    df.to_parquet(path)
    start = pd.Timestamp('2022-07-01 00:00:00')
    end = pd.Timestamp('2022-12-31 14:00:00')

    controller = build_flow([
        ParquetSource(path, end_filter=end, start_filter=start, filter_column='my_time'),
        ReduceToDataFrame(index=["my_string"], insert_key_column_as=["my_string"])
    ]).run()

    read_back_result = controller.await_termination()

    try:
        pd.testing.assert_frame_equal(read_back_result, pd.DataFrame({}))
    finally:
        os.remove(path)
