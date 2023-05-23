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
import os
import uuid

import pandas as pd
import pytest

from storey import (
    AsyncEmitSource,
    CSVSource,
    CSVTarget,
    FlatMap,
    Map,
    ParquetTarget,
    Reduce,
    SyncEmitSource,
    build_flow,
)

from .integration_test_utils import _generate_table_name

has_s3_credentials = os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY") and os.getenv("AWS_BUCKET")
if has_s3_credentials:
    from s3fs import S3FileSystem


@pytest.fixture()
def s3_create_csv():
    # Setup
    aws_bucket = os.getenv("AWS_BUCKET")
    file_path = _generate_table_name(f"{aws_bucket}/s3_storey")

    _write_test_csv(file_path)

    # Test runs
    yield file_path

    # Teardown
    _delete_file(file_path)


@pytest.fixture()
def s3_teardown_file():
    # Setup
    aws_bucket = os.getenv("AWS_BUCKET")
    file_path = _generate_table_name(f"{aws_bucket}/s3_storey")

    # Test runs
    yield file_path

    # Teardown
    _delete_file(file_path)


@pytest.fixture()
def s3_setup_teardown_test():
    # Setup
    table_name = _generate_table_name(f'{os.getenv("AWS_BUCKET")}/csv_test')

    # Test runs
    yield table_name

    # Teardown
    s3_recursive_delete(table_name)


@pytest.fixture()
def s3_teardown_file_in_bucket():
    full_type_path = ""

    def _create_file_name(file_type):
        aws_bucket = os.getenv("AWS_BUCKET")
        nonlocal full_type_path
        full_type_path = f"{aws_bucket}/file_in_bucket.{file_type}"
        return full_type_path

    yield _create_file_name

    # Teardown
    _delete_file(full_type_path)


def _write_test_csv(file_path):
    s3_fs = S3FileSystem()
    data = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    with s3_fs.open(file_path, "w") as f:
        f.write(data)


def _delete_file(path):
    s3_fs = S3FileSystem()
    s3_fs.delete(path)


def s3_recursive_delete(path):
    s3_fs = S3FileSystem()
    s3_fs.rm(path, True)


@pytest.mark.skipif(not has_s3_credentials, reason="No s3 credentials found")
def test_csv_reader_from_s3(s3_create_csv):
    controller = build_flow(
        [
            CSVSource(f"s3://{s3_create_csv}"),
            FlatMap(lambda x: x),
            Map(lambda x: int(x)),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    termination_result = controller.await_termination()
    assert termination_result == 495


@pytest.mark.skipif(not has_s3_credentials, reason="No s3 credentials found")
def test_csv_reader_from_s3_error_on_file_not_found():
    controller = build_flow(
        [
            CSVSource(f's3://{os.getenv("AWS_BUCKET")}/idontexist.csv'),
        ]
    ).run()

    with pytest.raises(FileNotFoundError):
        controller.await_termination()


async def async_test_write_csv_to_s3(s3_teardown_csv):
    controller = build_flow(
        [
            AsyncEmitSource(),
            CSVTarget(f"s3://{s3_teardown_csv}", columns=["n", "n*10"], header=True),
        ]
    ).run()

    for i in range(10):
        await controller.emit([i, 10 * i])

    await controller.terminate()
    await controller.await_termination()

    actual = S3FileSystem().open(s3_teardown_csv).read()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert actual.decode("utf-8") == expected


@pytest.mark.skipif(not has_s3_credentials, reason="No s3 credentials found")
def test_write_csv_to_s3(s3_teardown_file):
    asyncio.run(async_test_write_csv_to_s3(s3_teardown_file))


@pytest.mark.skipif(not has_s3_credentials, reason="No s3 credentials found")
def test_write_csv_with_dict_to_s3(s3_teardown_file):
    file_path = f"s3://{s3_teardown_file}"
    controller = build_flow([SyncEmitSource(), CSVTarget(file_path, columns=["n", "n*10"], header=True)]).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i})

    controller.terminate()
    controller.await_termination()

    actual = S3FileSystem().open(s3_teardown_file).read()
    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert actual.decode("utf-8") == expected


@pytest.mark.skipif(not has_s3_credentials, reason="No s3 credentials found")
def test_write_csv_infer_columns_without_header_to_s3(s3_teardown_file):
    file_path = f"s3://{s3_teardown_file}"
    controller = build_flow([SyncEmitSource(), CSVTarget(file_path)]).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i})

    controller.terminate()
    controller.await_termination()

    actual = S3FileSystem().open(s3_teardown_file).read()
    expected = "0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert actual.decode("utf-8") == expected


@pytest.mark.skipif(not has_s3_credentials, reason="No s3 credentials found")
def test_write_csv_from_lists_with_metadata_and_column_pruning_to_s3(s3_teardown_file):
    file_path = f"s3://{s3_teardown_file}"
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

    actual = S3FileSystem().open(s3_teardown_file).read()
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

    assert actual.decode("utf-8") == expected


@pytest.mark.skipif(not has_s3_credentials, reason="No s3 credentials found")
def test_write_to_parquet_to_s3(s3_setup_teardown_test):
    out_dir = f"s3://{s3_setup_teardown_test}/"
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
    expected = pd.DataFrame(expected, columns=columns)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    read_back_df["my_int"] = read_back_df["my_int"].astype("int64")
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


@pytest.mark.skipif(not has_s3_credentials, reason="No s3 credentials found")
def test_write_to_parquet_to_s3_single_file_on_termination(s3_setup_teardown_test):
    out_file = f"s3://{s3_setup_teardown_test}/myfile.pq"
    columns = ["my_int", "my_string"]
    controller = build_flow([SyncEmitSource(), ParquetTarget(out_file, columns=columns)]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}"])
        expected.append([i, f"this is {i}"])
    expected = pd.DataFrame(expected, columns=columns, dtype="int64")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


@pytest.mark.skipif(not has_s3_credentials, reason="No s3 credentials found")
def test_write_to_parquet_to_s3_with_indices(s3_setup_teardown_test):
    out_file = f"s3://{s3_setup_teardown_test}/test_write_to_parquet_with_indices{uuid.uuid4().hex}/"
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
    expected = pd.DataFrame(expected, columns=columns, dtype="int64")
    expected.set_index(["event_key"], inplace=True)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    # when reading from buckets lines order can be scrambled
    read_back_df.sort_values("event_key", inplace=True)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


@pytest.mark.skipif(not has_s3_credentials, reason="No s3 credentials found")
def test_write_csv_to_s3_bucket_directly(s3_teardown_file_in_bucket):
    file_path = f's3://{s3_teardown_file_in_bucket("csv")}'

    controller = build_flow([SyncEmitSource(), CSVTarget(file_path, columns=["n", "n*10"], header=True)]).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i})

    controller.terminate()
    controller.await_termination()

    actual = S3FileSystem().open(s3_teardown_file_in_bucket("csv")).read()
    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert actual.decode("utf-8") == expected


@pytest.mark.skipif(not has_s3_credentials, reason="No s3 credentials found")
def test_write_parquet_to_s3_bucket_directly(s3_teardown_file_in_bucket):
    columns = ["my_int", "my_string"]

    file_path = f's3://{s3_teardown_file_in_bucket("parquet")}'
    #    file_path = f'/tmp/{s3_teardown_file_in_bucket("parquet")}'
    controller = build_flow([SyncEmitSource(), ParquetTarget(file_path, columns=columns)]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}"])
        expected.append([i, f"this is {i}"])

    expected = pd.DataFrame(expected, columns=columns)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(file_path, columns=columns)

    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"
