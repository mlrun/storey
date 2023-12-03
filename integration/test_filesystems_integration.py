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
import datetime
import random as rand
import sys
import uuid

import aiohttp
import pandas as pd
import pytest
import v3io

from integration.integration_test_utils import V3ioHeaders, _generate_table_name
from storey import (
    AsyncEmitSource,
    CSVSource,
    CSVTarget,
    DataframeSource,
    FlatMap,
    Map,
    ParquetSource,
    ParquetTarget,
    Reduce,
    SyncEmitSource,
    build_flow,
)
from storey.dtypes import V3ioError


@pytest.fixture()
def v3io_create_csv():
    # Setup
    file_path = _generate_table_name("bigdata/storey_ci/csv_test") + "file.csv"

    asyncio.run(_write_test_csv(file_path))

    # Test runs
    yield file_path

    # Teardown
    asyncio.run(_delete_file(file_path))


@pytest.fixture()
def v3io_teardown_file():
    # Setup
    file_path = _generate_table_name("bigdata/storey_ci/csv_test")

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
        await client_session.put(
            f"{v3io_access._webapi_url}/{file_path}",
            data=data,
            headers=v3io_access._get_put_file_headers,
            ssl=False,
        )
    finally:
        await client_session.close()


async def _delete_file(path):
    connector = aiohttp.TCPConnector()
    v3io_access = V3ioHeaders()
    client_session = aiohttp.ClientSession(connector=connector)
    try:
        response = await client_session.delete(
            f"{v3io_access._webapi_url}/{path}",
            headers=v3io_access._get_put_file_headers,
            ssl=False,
        )
        if response.status >= 300 and response.status != 404 and response.status != 409:
            body = await response.text()
            raise V3ioError(f"Failed to delete item at {path}. Response status code was {response.status}: {body}")
    finally:
        await client_session.close()


def test_csv_reader_from_v3io(v3io_create_csv):
    controller = build_flow(
        [
            CSVSource(f"v3io:///{v3io_create_csv}"),
            FlatMap(lambda x: x),
            Map(lambda x: int(x)),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    termination_result = controller.await_termination()
    assert termination_result == 495


def test_csv_reader_from_v3io_error_on_file_not_found():
    controller = build_flow(
        [
            CSVSource("v3io:///bigdata/tests/idontexist.csv"),
        ]
    )

    with pytest.raises(FileNotFoundError):
        controller.run()


async def async_test_write_csv_to_v3io(v3io_teardown_csv):
    controller = build_flow(
        [
            AsyncEmitSource(),
            CSVTarget(f"v3io:///{v3io_teardown_csv}", columns=["n", "n*10"], header=True),
        ]
    ).run()

    for i in range(10):
        await controller.emit([i, 10 * i])

    await controller.terminate()
    await controller.await_termination()

    v3io_access = V3ioHeaders()
    v3io_client = v3io.aio.dataplane.Client(endpoint=v3io_access._webapi_url, access_key=v3io_access._access_key)
    try:
        container, path = v3io_teardown_csv.split("/", 1)
        result = await v3io_client.object.get(container, path)
    finally:
        await v3io_client.close()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result.body.decode("utf-8") == expected


def test_write_csv_to_v3io(v3io_teardown_file):
    asyncio.run(async_test_write_csv_to_v3io(v3io_teardown_file))


def test_write_csv_with_dict_to_v3io(v3io_teardown_file):
    file_path = f"v3io:///{v3io_teardown_file}"
    controller = build_flow([SyncEmitSource(), CSVTarget(file_path, columns=["n", "n*10"], header=True)]).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i})

    controller.terminate()
    controller.await_termination()

    v3io_access = V3ioHeaders()
    v3io_client = v3io.dataplane.Client(endpoint=v3io_access._webapi_url, access_key=v3io_access._access_key)
    try:
        container, path = v3io_teardown_file.split("/", 1)
        result = v3io_client.object.get(container, path)
    finally:
        v3io_client.close()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result.body.decode("utf-8") == expected


def test_write_csv_infer_columns_without_header_to_v3io(v3io_teardown_file):
    file_path = f"v3io:///{v3io_teardown_file}"
    controller = build_flow([SyncEmitSource(), CSVTarget(file_path)]).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i})

    controller.terminate()
    controller.await_termination()

    v3io_access = V3ioHeaders()
    v3io_client = v3io.dataplane.Client(endpoint=v3io_access._webapi_url, access_key=v3io_access._access_key)
    try:
        container, path = v3io_teardown_file.split("/", 1)
        result = v3io_client.object.get(container, path)
    finally:
        v3io_client.close()

    expected = "0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result.body.decode("utf-8") == expected


def test_write_csv_from_lists_with_metadata_and_column_pruning_to_v3io(
    v3io_teardown_file,
):
    file_path = f"v3io:///{v3io_teardown_file}"
    controller = build_flow(
        [
            SyncEmitSource(),
            CSVTarget(file_path, columns=["event_key=$key", "n*10"], header=True),
        ]
    ).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i}, key=f"key{i}")

    controller.terminate()
    controller.await_termination()

    v3io_access = V3ioHeaders()
    v3io_client = v3io.dataplane.Client(endpoint=v3io_access._webapi_url, access_key=v3io_access._access_key)
    try:
        container, path = v3io_teardown_file.split("/", 1)
        result = v3io_client.object.get(container, path)
    finally:
        v3io_client.close()

    expected = (
        "event_key,n*10\n"
        "key0,0\n"
        "key1,10\n"
        "key2,20\n"
        "key3,30\n"
        "key4,40\n"
        "key5,50\n"
        "key6,60\n"
        "key7,70\n"
        "key8,80\n"
        "key9,90\n"
    )

    assert result.body.decode("utf-8") == expected


def test_write_to_parquet_to_v3io(setup_teardown_test):
    out_dir = f"v3io:///{setup_teardown_test.table_name}"
    columns = ["my_int", "my_string"]
    controller = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(out_dir, partition_cols="my_int", columns=columns, max_events=1),
        ]
    ).run()

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}"])
        expected.append([i, f"this is {i}"])
    expected_df = pd.DataFrame(expected, columns=columns)
    expected_df["my_int"] = expected_df["my_int"].astype("category")

    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    pd.testing.assert_frame_equal(read_back_df, expected_df, check_categorical=False)


def test_write_to_parquet_to_v3io_single_file_on_termination(setup_teardown_test):
    out_file = f"v3io:///{setup_teardown_test.table_name}/out.parquet"
    columns = ["my_int", "my_string"]
    controller = build_flow([SyncEmitSource(), ParquetTarget(out_file, columns=columns)]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}"])
        expected.append([i, f"this is {i}"])
    expected = pd.DataFrame(expected, columns=columns)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    pd.testing.assert_frame_equal(read_back_df, expected)


# ML-775
def test_write_to_parquet_key_hash_partitioning(setup_teardown_test):
    out_dir = f"v3io:///{setup_teardown_test.table_name}/test_write_to_parquet_default_partitioning{uuid.uuid4().hex}/"
    controller = build_flow(
        [
            SyncEmitSource(key_field=1),
            ParquetTarget(out_dir, columns=["my_int", "my_string"], partition_cols=[("$key", 4)]),
        ]
    ).run()

    expected = []
    expected_buckets = [3, 0, 1, 3, 0, 3, 1, 1, 1, 2]
    for i in range(10):
        controller.emit([i, f"this is {i}"])
        expected.append([i, f"this is {i}", expected_buckets[i]])
    expected = pd.DataFrame(expected, columns=["my_int", "my_string", "hash4_key"])
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir)
    read_back_df["hash4_key"] = read_back_df["hash4_key"].astype("int64")
    read_back_df.sort_values("my_int", inplace=True)
    read_back_df.reset_index(inplace=True, drop=True)
    pd.testing.assert_frame_equal(read_back_df, expected)


# ML-701
def test_write_to_parquet_to_v3io_force_string_to_timestamp(setup_teardown_test):
    out_file = f"v3io:///{setup_teardown_test.table_name}/out.parquet"
    columns = ["time"]
    controller = build_flow([SyncEmitSource(), ParquetTarget(out_file, columns=[("time", "datetime")])]).run()

    expected = []
    for _ in range(10):
        t = "2021-03-02T19:45:00"
        controller.emit([t])
        expected.append([datetime.datetime.fromisoformat(t)])
    expected = pd.DataFrame(expected, columns=columns).astype("datetime64[us]")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    pd.testing.assert_frame_equal(read_back_df, expected)


def test_write_to_parquet_to_v3io_with_indices(setup_teardown_test):
    out_file = f"v3io:///{setup_teardown_test.table_name}/test_write_to_parquet_with_indices{uuid.uuid4().hex}.parquet"
    controller = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(out_file, index_cols="event_key=$key", columns=["my_int", "my_string"]),
        ]
    ).run()

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}"], key=f"key{i}")
        expected.append([f"key{i}", i, f"this is {i}"])
    columns = ["event_key", "my_int", "my_string"]
    expected = pd.DataFrame(expected, columns=columns)
    expected.set_index(["event_key"], inplace=True)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    pd.testing.assert_frame_equal(read_back_df, expected)


# ML-602
def test_write_to_parquet_to_v3io_with_nulls(setup_teardown_test):
    out_dir = f"v3io:///{setup_teardown_test.table_name}/test_write_to_parquet_to_v3io_with_nulls{uuid.uuid4().hex}/"
    flow = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(
                out_dir,
                columns=[
                    ("key=$key", "str"),
                    ("my_int", "int"),
                    ("my_string", "str"),
                    ("my_datetime", "datetime"),
                ],
                partition_cols=[],
                max_events=1,
            ),
        ]
    )

    expected = []
    my_time = datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=5)))

    controller = flow.run()
    controller.emit({"my_int": 0, "my_string": "hello", "my_datetime": my_time}, key="key1")
    # TODO: Expect correct time zone. Can be done in _Writer, but requires fix for ARROW-10511, which is pyarrow>=3.
    expected.append(
        [
            "key1",
            0,
            "hello",
            my_time.astimezone(datetime.timezone(datetime.timedelta())).replace(tzinfo=None),
        ]
    )
    controller.terminate()
    controller.await_termination()

    controller = flow.run()
    controller.emit({}, key="key2")
    expected.append(["key2", None, None, None])
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir)
    read_back_df.sort_values("key", inplace=True)
    read_back_df.reset_index(inplace=True, drop=True)
    expected = pd.DataFrame(expected, columns=["key", "my_int", "my_string", "my_datetime"])
    assert read_back_df.compare(expected).empty


def append_and_return(lst, x):
    lst.append(x)
    return lst


def test_filter_before_after_non_partitioned(setup_teardown_test):
    columns = ["my_string", "my_time"]

    df = pd.DataFrame(
        [
            ["good", pd.Timestamp("2018-05-07 13:52:37")],
            ["hello", pd.Timestamp("2019-01-26 14:52:37")],
            ["world", pd.Timestamp("2020-05-11 13:52:37")],
        ],
        columns=columns,
    )
    df.set_index("my_string")

    out_file = f"v3io:///{setup_teardown_test.table_name}/before_after_non_partitioned/"
    controller = build_flow(
        [
            DataframeSource(df),
            ParquetTarget(out_file, columns=columns, partition_cols=[]),
        ]
    ).run()
    controller.await_termination()

    before = pd.Timestamp("2019-12-01 00:00:00")
    after = pd.Timestamp("2019-01-01 23:59:59.999999")

    controller = build_flow(
        [
            ParquetSource(out_file, start_filter=after, end_filter=before, filter_column="my_time"),
            Reduce([], append_and_return),
        ]
    ).run()
    read_back_result = controller.await_termination()
    expected = [{"my_string": "hello", "my_time": pd.Timestamp("2019-01-26 14:52:37")}]

    assert read_back_result == expected, f"{read_back_result}\n!=\n{expected}"


def test_filter_before_after_partitioned_random(setup_teardown_test):
    low_limit = pd.Timestamp("2018-01-01")
    high_limit = pd.Timestamp("2020-12-31 23:59:59.999999")

    delta = high_limit - low_limit

    seed_value = rand.randrange(sys.maxsize)
    print("Seed value:", seed_value)
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
            element["string"] = char + str(i)
            random_second = rand.randrange(int(delta.total_seconds()))
            element["datetime"] = low_limit + datetime.timedelta(seconds=random_second)
            data.append(element)
        return data

    list1 = create_rand_data(number_below_middle_limit, low_limit, middle_limit, "lower")
    list2 = create_rand_data(10 - number_below_middle_limit, middle_limit, high_limit, "higher")
    combined_list = list1 + list2

    df = pd.DataFrame(combined_list)
    print("data frame is " + str(df))

    all_partition_columns = ["$year", "$month", "$day", "$hour", "$minute", "$second"]
    num_part_columns = rand.randrange(1, 6)
    partition_columns = all_partition_columns[:num_part_columns]
    print("partitioned by " + str(partition_columns))

    out_file = f"v3io:///{setup_teardown_test.table_name}/random/"
    controller = build_flow(
        [
            DataframeSource(df),
            ParquetTarget(
                out_file,
                columns=["string", "datetime"],
                partition_cols=partition_columns,
                time_field="datetime",
            ),
        ]
    ).run()

    controller.await_termination()

    controller = build_flow(
        [
            ParquetSource(
                out_file,
                start_filter=middle_limit,
                end_filter=high_limit,
                filter_column="datetime",
            ),
            Reduce([], append_and_return),
        ]
    ).run()
    read_back_result = controller.await_termination()
    print("expecting " + str(10 - number_below_middle_limit) + " to be above middle limit")
    assert (len(read_back_result)) == 10 - number_below_middle_limit

    controller = build_flow(
        [
            ParquetSource(
                out_file,
                start_filter=low_limit,
                end_filter=middle_limit,
                filter_column="datetime",
            ),
            Reduce([], append_and_return),
        ]
    ).run()
    read_back_result = controller.await_termination()
    print("expecting " + str(number_below_middle_limit) + " to be below middle limit")
    assert (len(read_back_result)) == number_below_middle_limit


def test_filter_before_after_partitioned_inner_other_partition(setup_teardown_test):
    columns = ["my_string", "my_time", "my_city"]

    df = pd.DataFrame(
        [
            ["hello", pd.Timestamp("2020-12-31 14:05:00"), "tel aviv"],
            ["world", pd.Timestamp("2018-12-30 09:00:00"), "haifa"],
            ["sun", pd.Timestamp("2019-12-29 09:00:00"), "tel aviv"],
            ["is", pd.Timestamp("2019-06-30 15:00:45"), "hod hasharon"],
            ["shining", pd.Timestamp("2020-02-28 13:00:56"), "hod hasharon"],
        ],
        columns=columns,
    )
    df.set_index("my_string")

    out_file = f"v3io:///{setup_teardown_test.table_name}/inner_other_partition/"
    controller = build_flow(
        [
            DataframeSource(df),
            ParquetTarget(
                out_file,
                columns=columns,
                partition_cols=["$year", "$month", "$day", "$hour", "my_city"],
                time_field="my_time",
            ),
        ]
    ).run()
    controller.await_termination()

    before = pd.Timestamp("2020-12-31 14:00:00")
    after = pd.Timestamp("2019-07-01 00:00:00")

    controller = build_flow(
        [
            ParquetSource(
                out_file,
                start_filter=after,
                end_filter=before,
                filter_column="my_time",
                columns=columns,
            ),
            Reduce([], append_and_return),
        ]
    ).run()
    read_back_result = controller.await_termination()

    expected = [
        {
            "my_string": "sun",
            "my_time": pd.Timestamp("2019-12-29 09:00:00"),
            "my_city": "tel aviv",
        },
        {
            "my_string": "shining",
            "my_time": pd.Timestamp("2020-02-28 13:00:56"),
            "my_city": "hod hasharon",
        },
    ]

    assert read_back_result == expected, f"{read_back_result}\n!=\n{expected}"


def test_filter_before_after_partitioned_outer_other_partition(setup_teardown_test):
    columns = ["my_string", "my_time", "my_city"]

    df = pd.DataFrame(
        [
            ["shining", pd.Timestamp("2020-12-31 14:00:00"), "ramat gan"],
            ["hello", pd.Timestamp("2020-12-30 08:53:00"), "tel aviv"],
            ["beautiful", pd.Timestamp("2020-12-30 09:00:00"), "haifa"],
            ["sun", pd.Timestamp("2020-12-29 09:00:00"), "tel aviv"],
            ["world", pd.Timestamp("2020-12-30 15:00:45"), "hod hasharon"],
            ["is", pd.Timestamp("2020-12-31 13:00:56"), "hod hasharon"],
        ],
        columns=columns,
    )
    df.set_index("my_string")

    out_file = f"v3io:///{setup_teardown_test.table_name}/outer_other_partition/"
    controller = build_flow(
        [
            DataframeSource(df),
            ParquetTarget(
                out_file,
                columns=columns,
                partition_cols=["my_city", "$year", "$month", "$day", "$hour"],
                time_field="my_time",
            ),
        ]
    ).run()
    controller.await_termination()

    start = pd.Timestamp("2020-12-30 08:53:00")
    end = pd.Timestamp("2020-12-31 14:00:00")

    controller = build_flow(
        [
            ParquetSource(
                out_file,
                start_filter=start,
                end_filter=end,
                filter_column="my_time",
                columns=columns,
            ),
            Reduce([], append_and_return),
        ]
    ).run()
    read_back_result = controller.await_termination()
    expected = [
        {
            "my_string": "beautiful",
            "my_time": pd.Timestamp("2020-12-30 09:00:00"),
            "my_city": "haifa",
        },
        {
            "my_string": "world",
            "my_time": pd.Timestamp("2020-12-30 15:00:45"),
            "my_city": "hod hasharon",
        },
        {
            "my_string": "is",
            "my_time": pd.Timestamp("2020-12-31 13:00:56"),
            "my_city": "hod hasharon",
        },
        {
            "my_string": "shining",
            "my_time": pd.Timestamp("2020-12-31 14:00:00"),
            "my_city": "ramat gan",
        },
    ]

    assert read_back_result == expected, f"{read_back_result}\n!=\n{expected}"
