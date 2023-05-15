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
import copy
import math
import os
import queue
import time
import traceback
import uuid
from datetime import datetime
from random import choice
from unittest.mock import MagicMock

import fakeredis
import pandas as pd
import pyarrow.parquet as pq
import pytest
from aiohttp import ClientConnectorError, InvalidURL
from pandas.testing import assert_frame_equal

import integration.conftest
import storey
from storey import (
    AsyncEmitSource,
    Batch,
    Choice,
    Complete,
    CSVSource,
    CSVTarget,
    DataframeSource,
    Driver,
    Event,
    Extend,
    Filter,
    FlatMap,
    HttpRequest,
    JoinWithTable,
    Map,
    MapClass,
    MapWithState,
    NoopDriver,
    NoSqlTarget,
    ParquetSource,
    ParquetTarget,
    QueryByKey,
    Recover,
    Reduce,
    ReduceToDataFrame,
    SendToHttp,
    SQLSource,
    SyncEmitSource,
    Table,
    ToDataFrame,
    TSDBTarget,
    V3ioDriver,
    build_flow,
)
from storey.flow import Context, ReifyMetadata, Rename, _ConcurrentJobExecution


class ATestException(Exception):
    pass


class RaiseEx:
    _counter = 0

    def __init__(self, raise_on_nth):
        self._raise_after = raise_on_nth

    def raise_ex(self, element):
        self._counter += 1
        if self._counter == self._raise_after:
            raise ATestException("test")
        return element


def test_functional_flow():
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            FlatMap(lambda x: [x, x * 10]),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    for _ in range(100):
        for i in range(10):
            controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 3300


def test_multiple_upstreams():
    source = SyncEmitSource()
    map1 = Map(lambda x: x + 1)
    map2 = Map(lambda x: x * 10)
    reduce = Reduce(0, lambda x, y: x + y)
    source.to(map1)
    source.to(map2)
    map1.to(reduce)
    map2.to(reduce)
    controller = source.run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 55 + 450


def test_multiple_upstreams_completion():
    source = SyncEmitSource()
    map1 = Map(lambda x: x + 1)
    map2 = Map(lambda x: x * 10)
    complete = Complete()
    source.to(map1)
    source.to(map2)
    map1.to(complete)
    map2.to(complete)
    controller = source.run()

    results = []
    try:
        for i in range(3):
            result = controller.emit(i, expected_number_of_results=2).await_result()
            results.append(result)
    finally:
        controller.terminate()
    controller.await_termination()
    assert results == [[1, 0], [2, 10], [3, 20]]


# ML-1167
def test_multiple_upstreams_termination():
    class FailOnSubsequentTermination(storey.Flow):
        def _init(self):
            super()._init()
            self.terminated = False

        async def _do(self, event):
            if event is storey.dtypes._termination_obj:
                if self.terminated:
                    raise AssertionError("Termination must only be received once")
                self.terminated = True
            return event

    source = SyncEmitSource()
    map1 = Map(lambda x: x + 1)
    map2 = Map(lambda x: x * 10)
    termination_checker = FailOnSubsequentTermination()
    source.to(map1)
    source.to(map2)
    map1.to(termination_checker)
    map2.to(termination_checker)

    controller = source.run()
    try:
        for i in range(3):
            controller.emit(i)
    finally:
        controller.terminate()
    controller.await_termination()

    assert termination_checker.terminated


def test_recover():
    def increment_maybe_boom(x):
        inc = x + 1
        if inc == 7:
            raise ValueError("boom")
        return inc

    reduce = Reduce(0, lambda x, y: x + y)
    controller = build_flow(
        [
            SyncEmitSource(),
            Recover({ValueError: reduce}),
            Map(increment_maybe_boom),
            reduce,
        ]
    ).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 54


# ML-777
def test_emit_timeless_event():
    class TimelessEvent:
        pass

    controller = build_flow([SyncEmitSource(), ReduceToDataFrame(insert_processing_time_column_as="mytime")]).run()

    event = TimelessEvent()
    event.id = "myevent"
    event.body = {"salutation": "hello"}
    t = datetime(2020, 2, 15, 2, 0)
    event.timestamp = t

    controller.emit(event)
    controller.terminate()
    termination_result = controller.await_termination()
    expected = pd.DataFrame([["hello", t]], columns=["salutation", "mytime"])
    assert termination_result.equals(expected)


def test_csv_reader():
    controller = build_flow(
        [
            CSVSource("tests/test.csv"),
            FlatMap(lambda x: x),
            Map(lambda x: int(x)),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    termination_result = controller.await_termination()
    assert termination_result == 21


def test_csv_reader_error_on_file_not_found():
    flow = build_flow(
        [
            CSVSource("tests/idontexist.csv"),
        ]
    )
    with pytest.raises(
        FileNotFoundError,
    ):
        flow.run()


def test_csv_reader_as_dict():
    controller = build_flow(
        [
            CSVSource("tests/test.csv", build_dict=True),
            FlatMap(lambda x: [x["n1"], x["n2"], x["n3"]]),
            Map(lambda x: int(x)),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    termination_result = controller.await_termination()
    assert termination_result == 21


def append_and_return(lst, x):
    lst.append(x)
    return lst


def test_csv_reader_as_dict_with_key_and_timestamp():
    controller = build_flow(
        [
            CSVSource(
                "tests/test-with-timestamp.csv",
                header=True,
                build_dict=True,
                key_field="k",
                parse_dates="t",
                timestamp_format="%d/%m/%Y %H:%M:%S",
            ),
            Reduce([], append_and_return, full_event=True),
        ]
    ).run()

    termination_result = controller.await_termination()

    assert len(termination_result) == 2
    assert termination_result[0].key == "m1"
    assert termination_result[0].body == {
        "k": "m1",
        "t": datetime(2020, 2, 15, 2, 0),
        "v": 8,
        "b": True,
    }
    assert termination_result[1].key == "m2"
    assert termination_result[1].body == {
        "k": "m2",
        "t": datetime(2020, 2, 16, 2, 0),
        "v": 14,
        "b": False,
    }


def test_csv_reader_as_dict_with_compact_timestamp():
    controller = build_flow(
        [
            CSVSource(
                "tests/test-with-compact-timestamp.csv",
                header=True,
                build_dict=True,
                parse_dates="t",
                timestamp_format="%Y%m%d%H",
            ),
            Reduce([], append_and_return, full_event=True),
        ]
    ).run()

    termination_result = controller.await_termination()

    assert len(termination_result) == 2
    assert termination_result[0].key is None
    assert termination_result[0].body == {
        "k": "m1",
        "t": datetime(2020, 2, 15, 2, 0),
        "v": 8,
        "b": True,
    }
    assert termination_result[1].key is None
    assert termination_result[1].body == {
        "k": "m2",
        "t": datetime(2020, 2, 16, 2, 0),
        "v": 14,
        "b": False,
    }


def test_csv_reader_with_key_and_timestamp():
    controller = build_flow(
        [
            CSVSource(
                "tests/test-with-timestamp.csv",
                header=True,
                key_field="k",
                parse_dates="t",
                timestamp_format="%d/%m/%Y %H:%M:%S",
            ),
            Reduce([], append_and_return, full_event=True),
        ]
    ).run()

    termination_result = controller.await_termination()

    assert len(termination_result) == 2
    assert termination_result[0].key == "m1"
    assert termination_result[0].body == ["m1", datetime(2020, 2, 15, 2, 0), 8, True]
    assert termination_result[1].key == "m2"
    assert termination_result[1].body == ["m2", datetime(2020, 2, 16, 2, 0), 14, False]


@pytest.mark.parametrize(
    "csv_source_kwargs", [{"parse_dates": ["t", "non_existent_column"]}, {"time_field": "non_existent_column"}]
)
def test_parse_dates_key_error(csv_source_kwargs):
    with pytest.raises(ValueError) as value_error:
        controller = build_flow(
            [
                CSVSource(
                    "tests/test-with-timestamp.csv",
                    header=True,
                    key_field="t",
                    timestamp_format="%d/%m/%Y %H:%M:%S",
                    **csv_source_kwargs,
                )
            ]
        ).run()
        controller.await_termination()
    assert str(value_error.value) == "Missing column provided to 'parse_dates': 'non_existent_column'"


@pytest.mark.parametrize("csv_source_kwargs", [{"parse_dates": ["t", 10]}, {"time_field": 10}])
def test_parse_dates_index_error(csv_source_kwargs):
    with pytest.raises(IndexError):
        controller = build_flow(
            [
                CSVSource(
                    "tests/test-with-timestamp.csv",
                    header=True,
                    key_field="k",
                    timestamp_format="%d/%m/%Y %H:%M:%S",
                    **csv_source_kwargs,
                )
            ]
        ).run()
        controller.await_termination()


def test_csv_reader_none_in_keyfield_should_send_error_log():
    logger = MockLogger()
    context = MockContext(logger, True)

    controller = build_flow(
        [
            CSVSource("tests/test-none-in-keyfield.csv", key_field="k", context=context),
        ]
    ).run()

    controller.await_termination()

    assert "error" == logger.logs[0][0]
    assert "Encountered null values in key fields. null Key values were: k" in logger.logs[0][1][0]


def test_csv_source_key_error():
    with pytest.raises(ValueError) as value_error:
        controller = build_flow(
            [
                CSVSource("tests/test.csv", key_field="not_exist"),
            ]
        ).run()

        controller.await_termination()
    assert str(value_error.value) == "key column 'not_exist' is missing from dataframe. File path: tests/test.csv."


def test_csv_reader_id_key_error():
    with pytest.raises(ValueError) as value_error:
        controller = build_flow(
            [
                CSVSource("tests/test.csv", id_field="not_exist"),
            ]
        ).run()

        controller.await_termination()
    assert str(value_error.value) == "id column 'not_exist' is missing from dataframe. File path: tests/test.csv."


def test_dataframe_source():
    df = pd.DataFrame([["hello", 1, 1.5], ["world", 2, 2.5]], columns=["string", "int", "float"])
    controller = build_flow(
        [
            DataframeSource(df),
            Reduce([], append_and_return),
        ]
    ).run()

    termination_result = controller.await_termination()
    expected = [
        {"string": "hello", "int": 1, "float": 1.5},
        {"string": "world", "int": 2, "float": 2.5},
    ]
    assert termination_result == expected


def test_indexed_dataframe_source():
    df = pd.DataFrame([["hello", 1, 1.5], ["world", 2, 2.5]], columns=["string", "int", "float"])
    df.set_index(["string", "int"], inplace=True)
    controller = build_flow(
        [
            DataframeSource(df),
            Reduce([], append_and_return),
        ]
    ).run()

    termination_result = controller.await_termination()
    expected = [
        {"string": "hello", "int": 1, "float": 1.5},
        {"string": "world", "int": 2, "float": 2.5},
    ]
    assert termination_result == expected


def test_dataframe_source_with_metadata():
    t1 = datetime(2020, 2, 15)
    t2 = datetime(2020, 2, 16)
    df = pd.DataFrame(
        [["key1", t1, "id1", 1.1], ["key2", t2, "id2", 2.2]],
        columns=["my_key", "my_time", "my_id", "my_value"],
    )
    controller = build_flow(
        [
            DataframeSource(df, key_field="my_key", time_field="my_time", id_field="my_id"),
            Reduce([], append_and_return, full_event=True),
        ]
    ).run()

    termination_result = controller.await_termination()
    expected = [
        Event(
            {"my_key": "key1", "my_time": t1, "my_id": "id1", "my_value": 1.1},
            key="key1",
            id="id1",
        ),
        Event(
            {"my_key": "key2", "my_time": t2, "my_id": "id2", "my_value": 2.2},
            key="key2",
            id="id2",
        ),
    ]
    assert termination_result == expected


async def async_dataframe_source():
    df = pd.DataFrame([["hello", 1, 1.5], ["world", 2, 2.5]], columns=["string", "int", "float"])
    controller = await build_flow(
        [
            DataframeSource(df),
            Reduce([], append_and_return),
        ]
    ).run_async()

    termination_result = await controller.await_termination()
    expected = [
        {"string": "hello", "int": 1, "float": 1.5},
        {"string": "world", "int": 2, "float": 2.5},
    ]
    assert termination_result == expected


def test_async_dataframe_source():
    asyncio.run(async_test_async_source())


def test_write_parquet_timestamp_nanosecs(tmpdir):
    out_dir = f"{tmpdir}/test_write_parquet_timestamp_nanosecs/{uuid.uuid4().hex}/"
    columns = ["string", "timestamp1", "timestamp2"]
    df = pd.DataFrame(
        [
            [
                "hello",
                pd.Timestamp("2020-01-26 14:52:37.12325679"),
                pd.Timestamp("2020-01-26 12:41:37.123456789"),
            ],
            [
                "world",
                pd.Timestamp("2018-05-11 13:52:37.333421789"),
                pd.Timestamp("2020-01-14 14:52:37.987654321"),
            ],
        ],
        columns=columns,
    )
    df.set_index(keys=["timestamp1"], inplace=True)
    parquet_target = ParquetTarget(
        out_dir, columns=["string", "timestamp2"], partition_cols=[], index_cols="timestamp1", time_field="timestamp1"
    )
    controller = build_flow(
        [
            DataframeSource(df),
            parquet_target,
        ]
    ).run()
    controller.await_termination()

    assert parquet_target._last_written_event == pd.Timestamp("2020-01-26 14:52:37.12325679")

    controller = build_flow(
        [
            ParquetSource(out_dir),
            Reduce([], append_and_return),
        ]
    ).run()

    termination_result = controller.await_termination()
    expected = [
        {
            "string": "hello",
            "timestamp1": pd.Timestamp("2020-01-26 14:52:37.123256"),
            "timestamp2": pd.Timestamp("2020-01-26 12:41:37.123456"),
        },
        {
            "string": "world",
            "timestamp1": pd.Timestamp("2018-05-11 13:52:37.333421"),
            "timestamp2": pd.Timestamp("2020-01-14 14:52:37.987654"),
        },
    ]
    assert termination_result == expected


def test_write_parquet_string_timestamp(tmpdir):
    out_dir = f"{tmpdir}/test_write_parquet_timestamp_nanosecs/{uuid.uuid4().hex}/"
    columns = ["string", "timestamp1", "timestamp2"]

    df = pd.DataFrame(
        [
            [
                "hello",
                "2020-01-26 14:52:37.123256",
                "2020-01-26 12:41:37.123456",
            ],
            [
                "world",
                "2018-05-11 13:52:37.333421",
                "2020-01-14 14:52:37.987654",
            ],
        ],
        columns=columns,
    )
    df.set_index(keys=["timestamp1"], inplace=True)
    parquet_target = ParquetTarget(
        out_dir,
        columns=["string", "timestamp2"],
        partition_cols=[],
        index_cols="timestamp1",
        time_field="timestamp1",
        time_format="%Y-%m-%d %H:%M:%S.%f",
    )
    controller = build_flow(
        [
            DataframeSource(df),
            parquet_target,
        ]
    ).run()
    controller.await_termination()

    assert parquet_target._last_written_event == pd.Timestamp("2020-01-26 14:52:37.123256")

    controller = build_flow(
        [
            ParquetSource(out_dir),
            Reduce([], append_and_return),
        ]
    ).run()

    termination_result = controller.await_termination()
    expected = [
        {
            "string": "hello",
            "timestamp1": pd.Timestamp("2020-01-26 14:52:37.123256"),
            "timestamp2": "2020-01-26 12:41:37.123456",
        },
        {
            "string": "world",
            "timestamp1": pd.Timestamp("2018-05-11 13:52:37.333421"),
            "timestamp2": "2020-01-14 14:52:37.987654",
        },
    ]
    assert termination_result == expected


# ML-1553
def test_write_parquet_time_zone_mix(tmpdir):
    out_file = f"{tmpdir}/test_write_parquet_time_zone_mix/{uuid.uuid4().hex}.pq"

    controller = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(out_file, columns=[("time", "datetime")]),
        ]
    ).run()

    controller.emit({"time": datetime.fromisoformat("2022-01-01T09:40:00+02:00")})
    controller.emit({"time": datetime.fromisoformat("2022-01-01T09:40:00+03:00")})

    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file)
    assert read_back_df.to_dict() == {
        "time": {0: datetime.fromisoformat("2022-01-01 07:40:00"), 1: datetime.fromisoformat("2022-01-01 06:40:00")}
    }


def test_read_parquet():
    controller = build_flow(
        [
            ParquetSource("tests/test.parquet"),
            Reduce([], append_and_return),
        ]
    ).run()

    termination_result = controller.await_termination()
    expected = [
        {"string": "hello", "int": 1, "float": 1.5},
        {"string": "world", "int": 2, "float": 2.5},
    ]
    assert termination_result == expected


def test_read_parquet_files():
    controller = build_flow(
        [
            ParquetSource(["tests/test.parquet", "tests/test.parquet"]),
            Reduce([], append_and_return),
        ]
    ).run()

    termination_result = controller.await_termination()
    expected = [
        {"string": "hello", "int": 1, "float": 1.5},
        {"string": "world", "int": 2, "float": 2.5},
        {"string": "hello", "int": 1, "float": 1.5},
        {"string": "world", "int": 2, "float": 2.5},
    ]
    assert termination_result == expected


def test_write_parquet_read_parquet(tmpdir):
    out_dir = f"{tmpdir}/test_write_parquet_read_parquet/{uuid.uuid4().hex}/"
    columns = ["my_int", "my_string"]
    controller = build_flow([SyncEmitSource(), ParquetTarget(out_dir, columns=columns, partition_cols=[])]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}"])
        expected.append({"my_int": i, "my_string": f"this is {i}"})
    controller.terminate()
    controller.await_termination()

    controller = build_flow(
        [
            ParquetSource(out_dir),
            Reduce([], append_and_return),
        ]
    ).run()
    read_back_result = controller.await_termination()

    assert read_back_result == expected


def test_write_parquet_read_parquet_partitioned(tmpdir):
    out_dir = f"{tmpdir}/test_write_parquet_read_parquet_partitioned/{uuid.uuid4().hex}/"
    columns = ["my_int", "my_string"]
    controller = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(out_dir, partition_cols="my_int", columns=columns),
        ]
    ).run()

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}"])
        expected.append({"my_int": i, "my_string": f"this is {i}"})
    controller.terminate()
    controller.await_termination()

    controller = build_flow(
        [
            ParquetSource(out_dir),
            Reduce([], append_and_return),
        ]
    ).run()
    read_back_result = controller.await_termination()

    assert read_back_result == expected


async def async_test_write_parquet_flush(tmpdir):
    out_dir = f"{tmpdir}/test_write_parquet_read_parquet_partitioned/{uuid.uuid4().hex}/"
    columns = ["my_int", "my_string"]
    target = ParquetTarget(out_dir, partition_cols="my_int", columns=columns, flush_after_seconds=2)

    async def f():
        pass

    mock = MagicMock(return_value=asyncio.get_running_loop().create_task(f()))
    target._emit = mock

    controller = build_flow(
        [
            AsyncEmitSource(),
            target,
        ]
    ).run()

    for i in range(10):
        await controller.emit([i, f"this is {i}"])

    try:
        assert mock.call_count == 0
        await asyncio.sleep(3)
        assert mock.call_count == 10
    finally:
        await controller.terminate()
        await controller.await_termination()


def test_write_parquet_flush(tmpdir):
    asyncio.run(async_test_write_parquet_flush(tmpdir))


def test_error_flow():
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: x + 1),
            Map(RaiseEx(500).raise_ex),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    with pytest.raises(ATestException):
        for i in range(1000):
            controller.emit(i)
        controller.terminate()
        controller.await_termination()


def test_error_recovery():
    reduce = Reduce(0, lambda acc, x: acc + x)
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: x + 1),
            Map(RaiseEx(5).raise_ex, recovery_step=reduce),
            reduce,
        ]
    ).run()

    for i in range(10):
        controller.emit(i)

    controller.terminate()
    result = controller.await_termination()
    assert result == 55


def test_set_recovery_step():
    reduce = Reduce(0, lambda acc, x: acc + x)
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: x + 1),
            Map(RaiseEx(5).raise_ex).set_recovery_step(reduce),
            reduce,
        ]
    ).run()

    for i in range(10):
        controller.emit(i)

    controller.terminate()
    result = controller.await_termination()
    assert result == 55


def test_read_space_in_header_csv():
    controller = build_flow(
        [
            CSVSource("tests/test_space_in_header.csv", build_dict=True),
            Reduce([], append_and_return),
        ]
    ).run()

    termination_result = controller.await_termination()
    expected = [{"header with space": 1, "n2": 2, "n3": 3}, {"header with space": 4, "n2": 5, "n3": 6}]
    assert termination_result == expected


def test_read_space_in_header_parquet():
    controller = build_flow(
        [
            ParquetSource("tests/test_space_in_header.parquet"),
            Reduce([], append_and_return),
        ]
    ).run()

    termination_result = controller.await_termination()
    expected = [{"header with space": 1, "n2": 2, "n3": 3}, {"header with space": 4, "n2": 5, "n3": 6}]
    assert termination_result == expected


def test_error_specific_recovery():
    reduce = Reduce(0, lambda acc, x: acc + x)
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: x + 1),
            Map(RaiseEx(5).raise_ex, recovery_step={ATestException: reduce}),
            reduce,
        ]
    ).run()

    for i in range(10):
        controller.emit(i)

    controller.terminate()
    result = controller.await_termination()
    assert result == 55


def test_error_specific_recovery_check_exception():
    reduce = Reduce(
        [],
        lambda acc, event: append_and_return(acc, type(event.error)),
        full_event=True,
    )
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(RaiseEx(2).raise_ex, recovery_step={ATestException: reduce}),
            reduce,
        ]
    ).run()

    for i in range(3):
        controller.emit(i)

    controller.terminate()
    result = controller.await_termination()
    assert result == [type(None), ATestException, type(None)]


def test_error_nonrecovery():
    reduce = Reduce(0, lambda acc, x: acc + x)
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: x + 1),
            Map(RaiseEx(5).raise_ex, recovery_step={ValueError: reduce}),
            reduce,
        ]
    ).run()

    with pytest.raises(ATestException):
        for i in range(10):
            controller.emit(i)
        controller.terminate()
        controller.await_termination()


def test_error_recovery_containment():
    reduce = Reduce(0, lambda acc, x: acc + x)
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: x + 1, recovery_step=reduce),
            Map(RaiseEx(5).raise_ex),
            reduce,
        ]
    ).run()

    with pytest.raises(ATestException):
        for i in range(10):
            controller.emit(i)
        controller.terminate()
        controller.await_termination()


def test_broadcast():
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3, termination_result_fn=lambda x, y: x + y),
            [Reduce(0, lambda acc, x: acc + x)],
            [Reduce(0, lambda acc, x: acc + x)],
        ]
    ).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 6


def test_broadcast_complex():
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3, termination_result_fn=lambda x, y: x + y),
            [
                Reduce(0, lambda acc, x: acc + x),
            ],
            [Map(lambda x: x * 100), Reduce(0, lambda acc, x: acc + x)],
            [Map(lambda x: x * 1000), Reduce(0, lambda acc, x: acc + x)],
        ]
    ).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 3303


# Same as test_broadcast_complex but without using build_flow
def test_broadcast_complex_no_sugar():
    source = SyncEmitSource()
    filter = Filter(lambda x: x < 3, termination_result_fn=lambda x, y: x + y)
    source.to(Map(lambda x: x + 1)).to(filter)
    filter.to(
        Reduce(0, lambda acc, x: acc + x),
    )
    filter.to(Map(lambda x: x * 100)).to(Reduce(0, lambda acc, x: acc + x))
    filter.to(Map(lambda x: x * 1000)).to(Reduce(0, lambda acc, x: acc + x))
    controller = source.run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 3303


def test_nested_branching():
    controller = build_flow(
        [
            SyncEmitSource(),
            [[Reduce(0, lambda acc, x: acc + x)]],
            [
                [Map(lambda x: x * 100), Reduce(0, lambda acc, x: acc + x)],
                [Map(lambda x: x * 1000), Reduce(0, lambda acc, x: acc + x)],
            ],
        ]
    ).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 45


def test_map_with_state_flow():
    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(1000, lambda x, state: (state, x)),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 1036


def test_map_with_state_flow_keyless_event():
    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(1000, lambda x, state: (state, x)),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    for i in range(10):
        event = Event(i)
        del event.key
        controller.emit(event)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 1036


def test_map_with_state_closes_state_on_termination():
    class MyCloseable:
        def __init__(self):
            self.times_closed = 0

        def close(self):
            self.times_closed += 1

    closeable_state = MyCloseable()
    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(closeable_state, lambda x, state: (x, state)),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    for i in range(3):
        event = Event(i)
        del event.key
        controller.emit(event)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 3
    assert closeable_state.times_closed == 1


@pytest.mark.parametrize("driver_type", ["v3io", "redis"])
def test_map_with_table_state_flow(driver_type):
    if driver_type == "v3io":
        driver = NoopDriver()
    elif driver_type == "redis":
        driver = fakeredis.FakeRedis(decode_responses=True, server=fakeredis.FakeServer())
    else:
        raise AssertionError(f"Unknown driver type {driver_type}")
    table_object = Table("table", driver)
    table_object["tal"] = {"color": "blue"}
    table_object["dina"] = {"color": "red"}

    def enrich(event, state):
        event["color"] = state["color"]
        state["counter"] = state.get("counter", 0) + 1
        return event, state

    table_path = f"{driver_type}:///mycontainer/mytable/"
    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(
                table_path,
                lambda x, state: enrich(x, state),
                group_by_key=True,
                context=Context(initial_tables={table_path: table_object}),
            ),
            Reduce([], append_and_return),
        ]
    ).run()

    for i in range(10):
        key = "tal"
        if i % 3 == 0:
            key = "dina"
        controller.emit(Event(body={"col1": i}, key=key))
    controller.terminate()

    termination_result = controller.await_termination()
    expected = [
        {"col1": 0, "color": "red"},
        {"col1": 1, "color": "blue"},
        {"col1": 2, "color": "blue"},
        {"col1": 3, "color": "red"},
        {"col1": 4, "color": "blue"},
        {"col1": 5, "color": "blue"},
        {"col1": 6, "color": "red"},
        {"col1": 7, "color": "blue"},
        {"col1": 8, "color": "blue"},
        {"col1": 9, "color": "red"},
    ]
    expected_cache = {
        "tal": {"color": "blue", "counter": 6},
        "dina": {"color": "red", "counter": 4},
    }

    assert termination_result == expected
    assert len(table_object._attrs_cache) == len(expected_cache)
    assert table_object["tal"] == expected_cache["tal"]
    assert table_object["dina"] == expected_cache["dina"]


def test_map_with_empty_table_state_flow():
    table_object = Table("table", NoopDriver())

    def enrich(event, state):
        if "first_value" not in state:
            state["first_value"] = event["col1"]
        event["diff_from_first"] = event["col1"] - state["first_value"]
        state["counter"] = state.get("counter", 0) + 1
        return event, state

    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(table_object, lambda x, state: enrich(x, state), group_by_key=True),
            Reduce([], append_and_return),
        ]
    ).run()

    for i in range(10):
        key = "tal"
        if i % 3 == 0:
            key = "dina"
        controller.emit(Event(body={"col1": i}, key=key))
    controller.terminate()

    termination_result = controller.await_termination()
    expected = [
        {"col1": 0, "diff_from_first": 0},
        {"col1": 1, "diff_from_first": 0},
        {"col1": 2, "diff_from_first": 1},
        {"col1": 3, "diff_from_first": 3},
        {"col1": 4, "diff_from_first": 3},
        {"col1": 5, "diff_from_first": 4},
        {"col1": 6, "diff_from_first": 6},
        {"col1": 7, "diff_from_first": 6},
        {"col1": 8, "diff_from_first": 7},
        {"col1": 9, "diff_from_first": 9},
    ]
    assert termination_result == expected
    expected_cache = {
        "dina": {"first_value": 0, "counter": 4},
        "tal": {"first_value": 1, "counter": 6},
    }
    assert len(table_object._attrs_cache) == len(expected_cache)
    assert table_object["tal"] == expected_cache["tal"]
    assert table_object["dina"] == expected_cache["dina"]


def test_awaitable_result():
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: x + 1, termination_result_fn=lambda _, x: x),
            [Complete()],
            [Reduce(0, lambda acc, x: acc + x)],
        ]
    ).run()

    for i in range(10):
        awaitable_result = controller.emit(i)
        assert awaitable_result.await_result() == i + 1
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 55


def test_double_completion():
    controller = build_flow([SyncEmitSource(), Complete(), Complete(), Reduce(0, lambda acc, x: acc + x)]).run()

    for i in range(10):
        awaitable_result = controller.emit(i)
        assert awaitable_result.await_result() == i
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 45


async def async_test_async_double_completion():
    controller = build_flow([AsyncEmitSource(), Complete(), Complete(), Reduce(0, lambda acc, x: acc + x)]).run()

    for i in range(10):
        result = await controller.emit(i)
        assert result == i
    await controller.terminate()
    termination_result = await controller.await_termination()
    assert termination_result == 45


def test_async_double_completion():
    asyncio.run(async_test_async_double_completion())


def test_awaitable_result_error():
    def boom(_):
        raise ValueError("boom")

    controller = build_flow([SyncEmitSource(), Map(boom), Complete()]).run()

    awaitable_result = controller.emit(0)
    try:
        with pytest.raises(ValueError):
            awaitable_result.await_result()
    finally:
        controller.terminate()


async def async_test_async_awaitable_result_error():
    def boom(_):
        raise ValueError("boom")

    controller = build_flow([AsyncEmitSource(), Map(boom), Complete()]).run()

    awaitable_result = controller.emit(0)
    try:
        with pytest.raises(ValueError):
            await awaitable_result
    finally:
        await controller.terminate()


def test_async_awaitable_result_error():
    asyncio.run(async_test_async_awaitable_result_error())


def test_complete_without_awaitable_result():
    def delete_awaitable(event):
        event._awaitable_result = None
        return event

    controller = build_flow([SyncEmitSource(), Map(delete_awaitable, full_event=True), Complete()]).run()
    for i in range(3):
        controller.emit(i)
    controller.terminate()
    controller.await_termination()


async def async_test_async_source():
    controller = build_flow(
        [
            AsyncEmitSource(),
            Map(lambda x: x + 1, termination_result_fn=lambda _, x: x),
            [Complete()],
            [Reduce(0, lambda acc, x: acc + x)],
        ]
    ).run()

    for i in range(10):
        result = await controller.emit(i)
        assert result == i + 1
    await controller.terminate()
    termination_result = await controller.await_termination()
    assert termination_result == 55


def test_async_source():
    loop = asyncio.new_event_loop()
    loop.run_until_complete(async_test_async_source())


async def async_test_error_async_flow():
    controller = build_flow(
        [
            AsyncEmitSource(),
            Map(lambda x: x + 1),
            Map(RaiseEx(5).raise_ex),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    try:
        for i in range(10):
            await controller.emit(i)
    except ATestException:
        pass


def test_awaitable_result_error_in_async_downstream():
    controller = build_flow(
        [
            SyncEmitSource(),
            SendToHttp(
                lambda _: HttpRequest("GET", "bad_url", ""),
                lambda _, response: response.status,
            ),
            Complete(),
        ]
    ).run()
    try:
        with pytest.raises(InvalidURL):
            controller.emit(1).await_result()
    finally:
        controller.terminate()


async def async_test_async_awaitable_result_error_in_async_downstream():
    controller = build_flow(
        [
            AsyncEmitSource(),
            SendToHttp(
                lambda _: HttpRequest("GET", "bad_url", ""),
                lambda _, response: response.status,
            ),
            Complete(),
        ]
    ).run()
    with pytest.raises(InvalidURL):
        await controller.emit(1)


def test_async_awaitable_result_error_in_async_downstream():
    asyncio.run(async_test_async_awaitable_result_error_in_async_downstream())


def test_awaitable_result_error_in_by_key_async_downstream():
    class DriverBoom(Driver):
        async def _save_key(
            self,
            container,
            table_path,
            key,
            aggr_item,
            partitioned_by_key,
            additional_data,
        ):
            raise ValueError("boom")

    controller = build_flow([SyncEmitSource(), NoSqlTarget(Table("test", DriverBoom())), Complete()]).run()
    try:
        with pytest.raises(ValueError):
            controller.emit({"col1": 0}, "key").await_result()
            controller.terminate()
            controller.await_termination()
    finally:
        controller.terminate()


def test_error_async_flow():
    loop = asyncio.new_event_loop()
    loop.run_until_complete(async_test_error_async_flow())


# ML-1147
def test_error_trace():
    def boom(_):
        raise ValueError("boom")

    controller = build_flow([SyncEmitSource(), Map(boom), Complete()]).run()

    awaitable_results = []
    for _ in range(2):
        try:
            awaitable_results.append(controller.emit(0))
        except ValueError:
            pass

    last_trace_size = None
    for awaitable_result in awaitable_results:
        try:
            awaitable_result.await_result()
            raise AssertionError()
        except ValueError:
            trace_size = len(traceback.format_exc())
            if last_trace_size is not None:
                assert trace_size == last_trace_size
            last_trace_size = trace_size
        finally:
            controller.terminate()


def test_choice():
    small_reduce = Reduce(0, lambda acc, x: acc + x)

    big_reduce = build_flow([Map(lambda x: x * 100), Reduce(0, lambda acc, x: acc + x)])

    controller = build_flow(
        [
            SyncEmitSource(),
            Choice(
                [(big_reduce, lambda x: x % 2 == 0)],
                default=small_reduce,
                termination_result_fn=lambda x, y: x + y,
            ),
        ]
    ).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 2025


def test_metadata():
    def mapf(x):
        x.key = x.key + 1
        return x

    def redf(acc, x):
        if x.key not in acc:
            acc[x.key] = []
        acc[x.key].append(x.body)
        return acc

    controller = build_flow(
        [
            SyncEmitSource(),
            Map(mapf, full_event=True),
            Reduce({}, redf, full_event=True),
        ]
    ).run()

    for i in range(10):
        controller.emit(Event(i, key=i % 3))
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == {1: [0, 3, 6, 9], 2: [1, 4, 7], 3: [2, 5, 8]}


def test_metadata_immutability():
    def mapf(x):
        x.key = "new key"
        return x

    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: "new body"),
            Map(mapf, full_event=True),
            Complete(full_event=True),
        ]
    ).run()

    event = Event("original body", key="original key")
    result = controller.emit(event).await_result()
    controller.terminate()
    controller.await_termination()

    assert event.key == "original key"
    assert event.body == "original body"
    assert result.key == "new key"
    assert result.body == "new body"


def test_batch():
    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(4, 100),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]


def test_batch_full_event():
    def append_body_and_return(lst, x):
        ll = []
        for item in x:
            ll.append(item.body)
        lst.append(ll)
        return lst

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(4, 100, full_event=True),
            Reduce([], lambda acc, x: append_body_and_return(acc, x)),
        ]
    ).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]


def test_batch_by_user_key():
    def append_and_return(lst, x):
        lst.append(x)
        return lst

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(2, 100, "value"),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    values_1 = [i for i in range(4)]
    values_2 = [i for i in range(4)]
    values_3 = [i for i in range(4)]
    values_4 = [i for i in range(4)]

    for _ in range(4):
        rand_val_1 = choice(values_1)
        rand_val_2 = choice(values_2)
        rand_val_3 = choice(values_3)
        rand_val_4 = choice(values_4)

        values_1.remove(rand_val_1)
        values_2.remove(rand_val_2)
        values_3.remove(rand_val_3)
        values_4.remove(rand_val_4)

        controller.emit({"value": rand_val_1})
        controller.emit({"value": rand_val_2})
        controller.emit({"value": rand_val_3})
        controller.emit({"value": rand_val_4})

    controller.terminate()
    termination_result = controller.await_termination()

    assert len(termination_result) == 8

    for element in termination_result:
        assert len(element) == 2
        numbers = [e["value"] for e in element]
        assert numbers[0] == numbers[1]


def test_batch_by_event_key():
    def append_and_return(lst, x):
        lst.append(x)
        return lst

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(5, 100, "$key"),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    controller.emit(1, key="key1")
    controller.emit(2, key="key1")
    controller.emit(3, key="key1")
    controller.emit(4, key="key1")

    controller.emit(8, key="key2")
    controller.emit(9, key="key2")
    controller.emit(10, key="key2")

    controller.emit(5, key="key1")
    controller.emit(6, key="key1")
    controller.emit(7, key="key1")

    controller.terminate()
    termination_result = controller.await_termination()

    assert termination_result[0] == [1, 2, 3, 4, 5]  # Emitted first due to max_events
    assert termination_result[1] == [8, 9, 10]
    assert termination_result[2] == [6, 7]


def test_batch_by_field_value_key_extractor():
    def append_and_return(lst, x):
        lst.append(x)
        return lst

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(3, 100, "field"),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    controller.emit({"field": "name_1", "field_data": 10})
    controller.emit({"field": "name_2", "field_data": 9})
    controller.emit({"field": "name_1", "field_data": 8})
    controller.emit({"field": "name_2", "field_data": 7})
    controller.emit({"field": "name_1", "field_data": 6})
    controller.emit({"field": "name_2", "field_data": 5})
    controller.emit({"field": "name_1", "field_data": 4})
    controller.emit({"field": "name_2", "field_data": 3})
    controller.emit({"field": "name_1", "field_data": 2})
    controller.emit({"field": "name_2", "field_data": 1})
    controller.emit({"field": "name_1", "field_data": 0})

    controller.terminate()
    termination_result = controller.await_termination()

    # Grouped with same field value, emitted after 3 events due to configuration
    assert termination_result[0] == [
        {"field": "name_1", "field_data": 10},
        {"field": "name_1", "field_data": 8},
        {"field": "name_1", "field_data": 6},
    ]
    assert termination_result[1] == [
        {"field": "name_2", "field_data": 9},
        {"field": "name_2", "field_data": 7},
        {"field": "name_2", "field_data": 5},
    ]
    assert termination_result[2] == [
        {"field": "name_1", "field_data": 4},
        {"field": "name_1", "field_data": 2},
        {"field": "name_1", "field_data": 0},
    ]
    assert termination_result[3] == [
        {"field": "name_2", "field_data": 3},
        {"field": "name_2", "field_data": 1},
    ]


def test_batch_by_function_key_extractor():
    def append_and_return(lst, x):
        lst.append(x)
        return lst

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(10, 100, lambda event: event.body % 3 == 0),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    controller.emit(1)
    controller.emit(2)
    controller.emit(3)
    controller.emit(4)
    controller.emit(5)
    controller.emit(6)
    controller.emit(7)
    controller.emit(8)
    controller.emit(9)

    controller.terminate()
    termination_result = controller.await_termination()

    assert termination_result[0] == [1, 2, 4, 5, 7, 8]
    assert termination_result[1] == [
        3,
        6,
        9,
    ]  # Group all numbers that return true on Event.body % 3 == 0


def test_batch_grouping_with_timeout():
    q = queue.Queue(1)

    def reduce_fn(acc, event):
        if event == [1]:
            q.put(None)
        acc.append(event)
        return acc

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(3, 1, "$key"),
            Reduce([], lambda acc, x: reduce_fn(acc, x)),
        ]
    ).run()

    controller.emit(1, key=1)
    q.get()
    controller.emit(2, key=2)
    controller.emit(2, key=2)
    controller.emit(2, key=2)
    controller.emit(3, key=2)
    controller.emit(3, key=2)
    controller.emit(3, key=2)

    controller.terminate()
    termination_result = controller.await_termination()

    assert termination_result[0] == [1]  # Emitted first due to timeout
    assert termination_result[1] == [
        2,
        2,
        2,
    ]  # Emitted second due to max_events configuration
    assert termination_result[2] == [3, 3, 3]


def test_batch_with_timeout():
    q = queue.Queue(1)

    def reduce_fn(acc, x):
        if x[0] == 0:
            q.put(None)
        acc.append(x)
        return acc

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(4, 1),
            Reduce([], reduce_fn),
        ]
    ).run()

    for i in range(10):
        if i == 3:
            q.get()
        controller.emit(Event(i, processing_time=datetime(2020, 2, 15, 2, 0)))
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [[0, 1, 2], [3, 4, 5, 6], [7, 8, 9]]


async def async_test_write_csv(tmpdir):
    file_path = f"{tmpdir}/test_write_csv/out.csv"
    controller = build_flow([AsyncEmitSource(), CSVTarget(file_path, columns=["n", "n*10"], header=True)]).run()

    for i in range(10):
        await controller.emit([i, 10 * i])

    await controller.terminate()
    await controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result == expected


def test_write_csv(tmpdir):
    asyncio.run(async_test_write_csv(tmpdir))


async def async_test_write_csv_error(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_error.csv"

    write_csv = CSVTarget(file_path)
    controller = build_flow([AsyncEmitSource(), write_csv]).run()

    with pytest.raises(TypeError):
        for i in range(10):
            await controller.emit(i)
        await controller.terminate()
        await controller.await_termination()


def test_write_csv_error(tmpdir):
    asyncio.run(async_test_write_csv_error(tmpdir))


def test_write_csv_with_dict(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_with_dict.csv"
    controller = build_flow([SyncEmitSource(), CSVTarget(file_path, columns=["n", "n*10"], header=True)]).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i})

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result == expected


def test_write_csv_infer_columns(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_infer_columns.csv"
    controller = build_flow([SyncEmitSource(), CSVTarget(file_path, header=True)]).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i})

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "n,n*10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result == expected


def test_write_csv_infer_columns_without_header(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_infer_columns_without_header.csv"
    controller = build_flow([SyncEmitSource(), CSVTarget(file_path)]).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i})

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result == expected


def test_write_csv_with_metadata(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_with_metadata.csv"
    controller = build_flow(
        [
            SyncEmitSource(),
            CSVTarget(file_path, columns=["event_key=$key", "n", "n*10"], header=True),
        ]
    ).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i}, key=f"key{i}")

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = (
        "event_key,n,n*10\n"
        "key0,0,0\n"
        "key1,1,10\n"
        "key2,2,20\n"
        "key3,3,30\n"
        "key4,4,40\n"
        "key5,5,50\n"
        "key6,6,60\n"
        "key7,7,70\n"
        "key8,8,80\n"
        "key9,9,90\n"
    )

    assert result == expected


def test_write_csv_with_metadata_no_rename(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_with_metadata_no_rename.csv"
    controller = build_flow(
        [
            SyncEmitSource(),
            CSVTarget(file_path, columns=["$key", "n", "n*10"], header=True),
        ]
    ).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i}, key=f"key{i}")

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = (
        "key,n,n*10\n"
        "key0,0,0\n"
        "key1,1,10\n"
        "key2,2,20\n"
        "key3,3,30\n"
        "key4,4,40\n"
        "key5,5,50\n"
        "key6,6,60\n"
        "key7,7,70\n"
        "key8,8,80\n"
        "key9,9,90\n"
    )

    assert result == expected


def test_write_csv_with_rename(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_with_rename.csv"
    controller = build_flow(
        [
            SyncEmitSource(),
            CSVTarget(file_path, columns=["n", "n x 10=n*10"], header=True),
        ]
    ).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i})

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "n,n x 10\n0,0\n1,10\n2,20\n3,30\n4,40\n5,50\n6,60\n7,70\n8,80\n9,90\n"
    assert result == expected


def test_write_csv_from_lists_with_metadata(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_with_metadata.csv"
    controller = build_flow(
        [
            SyncEmitSource(),
            CSVTarget(file_path, columns=["event_key=$key", "n", "n*10"], header=True),
        ]
    ).run()

    for i in range(10):
        controller.emit([i, 10 * i], key=f"key{i}")

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = (
        "event_key,n,n*10\n"
        "key0,0,0\n"
        "key1,1,10\n"
        "key2,2,20\n"
        "key3,3,30\n"
        "key4,4,40\n"
        "key5,5,50\n"
        "key6,6,60\n"
        "key7,7,70\n"
        "key8,8,80\n"
        "key9,9,90\n"
    )

    assert result == expected


def test_write_csv_from_lists_with_metadata_and_column_pruning(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_from_lists_with_metadata_and_column_pruning.csv"
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

    with open(file_path) as file:
        result = file.read()

    expected = (
        "event_key,n*10\nkey0,0\nkey1,10\nkey2,20\nkey3,30\nkey4,40\nkey5,50\nkey6,60\nkey7,70\nkey8,80\nkey9,90\n"
    )
    assert result == expected


def test_write_csv_infer_with_metadata_columns(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_infer_with_metadata_columns.csv"
    controller = build_flow(
        [
            SyncEmitSource(),
            CSVTarget(
                file_path,
                columns=["event_key=$key"],
                header=True,
                infer_columns_from_data=True,
            ),
        ]
    ).run()

    for i in range(10):
        controller.emit({"n": i, "n*10": 10 * i}, key=f"key{i}")

    controller.terminate()
    controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = (
        "event_key,n,n*10\n"
        "key0,0,0\n"
        "key1,1,10\n"
        "key2,2,20\n"
        "key3,3,30\n"
        "key4,4,40\n"
        "key5,5,50\n"
        "key6,6,60\n"
        "key7,7,70\n"
        "key8,8,80\n"
        "key9,9,90\n"
    )

    assert result == expected


def test_write_csv_fail_to_infer_columns(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_fail_to_infer_columns.csv"
    controller = build_flow([SyncEmitSource(), CSVTarget(file_path, header=True)]).run()

    with pytest.raises(TypeError):
        controller.emit([0])
        controller.terminate()
        controller.await_termination()


def test_reduce_to_dataframe():
    controller = build_flow([SyncEmitSource(), ReduceToDataFrame()]).run()

    expected = []
    for i in range(10):
        controller.emit({"my_int": i, "my_string": f"this is {i}"})
        expected.append({"my_int": i, "my_string": f"this is {i}"})
    expected = pd.DataFrame(expected)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result.equals(expected), f"{termination_result}\n!=\n{expected}"


def test_reduce_to_dataframe_with_index():
    index = "my_int"
    controller = build_flow([SyncEmitSource(), ReduceToDataFrame(index=index)]).run()

    expected = []
    for i in range(10):
        controller.emit({"my_int": i, "my_string": f"this is {i}"})
        expected.append({"my_int": i, "my_string": f"this is {i}"})
    expected = pd.DataFrame(expected)
    expected.set_index(index, inplace=True)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result.equals(expected), f"{termination_result}\n!=\n{expected}"


def test_reduce_to_dataframe_with_index_from_lists():
    index = "my_int"
    controller = build_flow(
        [
            SyncEmitSource(),
            ReduceToDataFrame(index=index, columns=["my_int", "my_string"]),
        ]
    ).run()

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}"])
        expected.append({"my_int": i, "my_string": f"this is {i}"})
    expected = pd.DataFrame(expected)
    expected.set_index(index, inplace=True)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result.equals(expected), f"{termination_result}\n!=\n{expected}"


def test_reduce_to_dataframe_indexed_by_key():
    index = "my_key"
    controller = build_flow([SyncEmitSource(), ReduceToDataFrame(index=index, insert_key_column_as=index)]).run()

    expected = []
    for i in range(10):
        controller.emit({"my_int": i, "my_string": f"this is {i}"}, key=f"key{i}")
        expected.append({"my_int": i, "my_string": f"this is {i}", "my_key": f"key{i}"})
    expected = pd.DataFrame(expected)
    expected.set_index(index, inplace=True)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result.equals(expected), f"{termination_result}\n!=\n{expected}"


def test_to_dataframe_with_index():
    index = "my_int"
    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(5),
            ToDataFrame(index=index),
            Reduce([], append_and_return, full_event=True),
        ]
    ).run()

    expected1 = []
    for i in range(5):
        data = {"my_int": i, "my_string": f"this is {i}"}
        controller.emit(data)
        expected1.append(data)

    expected2 = []
    for i in range(5, 10):
        data = {"my_int": i, "my_string": f"this is {i}"}
        controller.emit(data)
        expected2.append(data)

    expected1 = pd.DataFrame(expected1)
    expected2 = pd.DataFrame(expected2)
    expected1.set_index(index, inplace=True)
    expected2.set_index(index, inplace=True)

    controller.terminate()
    termination_result = controller.await_termination()

    assert len(termination_result) == 2
    assert termination_result[0].body.equals(expected1), f"{termination_result[0]}\n!=\n{expected1}"
    assert termination_result[1].body.equals(expected2), f"{termination_result[1]}\n!=\n{expected2}"


def test_map_class():
    class MyMap(MapClass):
        def __init__(self, mul=1, **kwargs):
            super().__init__(**kwargs)
            self._mul = mul

        def do(self, event):
            if event["bid"] > 700:
                return self.filter()
            event["xx"] = event["bid"] * self._mul
            return event

    controller = build_flow(
        [
            SyncEmitSource(),
            MyMap(2),
            Reduce(0, lambda acc, x: acc + x["xx"]),
        ]
    ).run()

    controller.emit({"bid": 600})
    controller.emit({"bid": 700})
    controller.emit({"bid": 1000})
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 2600


def test_extend():
    controller = build_flow(
        [
            SyncEmitSource(),
            Extend(lambda x: {"bid2": x["bid"] + 1}),
            Reduce([], append_and_return),
        ]
    ).run()

    controller.emit({"bid": 1})
    controller.emit({"bid": 11})
    controller.emit({"bid": 111})
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [
        {"bid": 1, "bid2": 2},
        {"bid": 11, "bid2": 12},
        {"bid": 111, "bid2": 112},
    ]


def test_write_to_parquet(tmpdir):
    out_dir = f"{tmpdir}/test_write_to_parquet/{uuid.uuid4().hex}/"
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
    expected_in_pyarrow1 = pd.DataFrame(expected, columns=columns)
    expected_in_pyarrow3 = expected_in_pyarrow1.copy()
    expected_in_pyarrow1["my_int"] = expected_in_pyarrow1["my_int"].astype("int32")
    expected_in_pyarrow3["my_int"] = expected_in_pyarrow3["my_int"].astype("category")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    assert read_back_df.equals(expected_in_pyarrow1) or read_back_df.equals(expected_in_pyarrow3)


# Regression test for ML-2510.
# Partitioning by datetime is not something you would normally want to do.
def test_write_to_parquet_partition_by_datetime(tmpdir):
    out_dir = f"{tmpdir}/test_write_to_parquet_partition_by_datetime/{uuid.uuid4().hex}/"
    columns = ["my_int", "my_string", "my_datetime"]
    controller = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(out_dir, partition_cols="my_datetime", columns=columns, max_events=1),
        ]
    ).run()

    my_time = datetime(2020, 2, 15)

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}", my_time])
        expected.append([i, f"this is {i}", my_time.isoformat(sep=" ")])
    expected_df = pd.DataFrame(expected, columns=columns)
    expected_df["my_datetime"] = expected_df["my_datetime"].astype("category")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    read_back_df.sort_values("my_int", inplace=True)
    read_back_df.reset_index(drop=True, inplace=True)
    assert read_back_df.equals(expected_df)


def test_write_to_parquet_string_as_datetime(tmpdir):
    out_dir = f"{tmpdir}/test_write_to_parquet_string_to_datetime/{uuid.uuid4().hex}/"
    columns = ["my_int", "my_string", "my_datetime"]
    columns_with_type = [
        ("my_int", "int"),
        ("my_string", "str"),
        ("my_datetime", "datetime"),
    ]
    controller = build_flow(
        [
            SyncEmitSource(),
            # set time_field="" to test for ML-3544 regression
            ParquetTarget(out_dir, partition_cols=[], columns=columns_with_type, time_field="", max_events=1),
        ]
    ).run()

    my_time = datetime(2020, 2, 15)

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}", my_time.isoformat()])
        expected.append([i, f"this is {i}", my_time.isoformat(sep=" ")])
    expected_df = pd.DataFrame(expected, columns=columns)
    expected_df["my_datetime"] = expected_df["my_datetime"].astype("datetime64[us]")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    read_back_df.sort_values("my_int", inplace=True)
    read_back_df.reset_index(drop=True, inplace=True)
    assert read_back_df.equals(expected_df)


def test_write_sparse_data_to_parquet(tmpdir):
    out_dir = f"{tmpdir}/test_write_sparse_data_to_parquet/{uuid.uuid4().hex}"
    columns = ["my_int", "my_string"]
    controller = build_flow([SyncEmitSource(), ParquetTarget(out_dir, columns=columns)]).run()

    expected = []
    for i in range(10):
        expected.append({"my_int": i})
        controller.emit({"my_int": i})
        expected.append({"my_string": f"this is {i}"})
        controller.emit({"my_string": f"this is {i}"})
    expected = pd.DataFrame(expected, columns=columns)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_single_file_on_termination(tmpdir):
    out_file = f"{tmpdir}/test_write_to_parquet_single_file_on_termination_{uuid.uuid4().hex}/out.parquet"
    columns = ["my_int", "my_string"]
    controller = build_flow([SyncEmitSource(), ParquetTarget(out_file, columns=columns)]).run()

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}"])
        expected.append([i, f"this is {i}"])
    expected = pd.DataFrame(expected, columns=columns)
    controller.terminate()
    controller.await_termination()

    assert os.path.isfile(out_file)
    read_back_df = pd.read_parquet(out_file, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


# ML-1500
def test_write_to_parquet_single_file_pandas_metadata(tmpdir):
    out_file = f"{tmpdir}/test_write_to_parquet_single_file_pandas_metadata{uuid.uuid4().hex}/out.parquet"
    controller = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(out_file, index_cols=[("my_int", "int")], columns=[("my_string", "str")]),
        ]
    ).run()

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}"])
        expected.append([i, f"this is {i}"])
    controller.terminate()
    controller.await_termination()

    assert os.path.isfile(out_file)
    pf = pq.ParquetFile(out_file)
    assert pf.schema_arrow.pandas_metadata["columns"] == [
        {
            "field_name": "my_string",
            "metadata": None,
            "name": "my_string",
            "numpy_type": "object",
            "pandas_type": "unicode",
        },
        {
            "field_name": "my_int",
            "metadata": None,
            "name": "my_int",
            "numpy_type": "int64",
            "pandas_type": "int64",
        },
    ]


def test_write_to_parquet_with_metadata(tmpdir):
    out_file = f"{tmpdir}/test_write_to_parquet_with_metadata{uuid.uuid4().hex}/"
    columns = ["event_key", "my_int", "my_string"]
    controller = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(
                out_file,
                columns=["event_key=$key", "my_int", "my_string"],
                partition_cols=["$year", "$month", "$day", "$hour"],
            ),
        ]
    ).run()

    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}"], key=f"key{i}")
        expected.append([f"key{i}", i, f"this is {i}"])
    expected = pd.DataFrame(expected, columns=columns)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_with_indices(tmpdir):
    out_file = f"{tmpdir}/test_write_to_parquet_with_indices{uuid.uuid4().hex}"
    controller = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(
                out_file,
                index_cols="event_key=$key",
                columns=["my_int", "my_string"],
                partition_cols=["$year", "$month", "$day", "$hour"],
            ),
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
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_partition_by_date(tmpdir):
    out_file = f"{tmpdir}/test_write_to_parquet_partition_by_date{uuid.uuid4().hex}"
    controller = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(out_file, partition_cols=["$date"], columns=["time", "my_int", "my_string"], time_field=0),
        ]
    ).run()

    my_time = datetime(2020, 2, 15)

    expected = []
    for i in range(10):
        controller.emit([my_time, i, f"this is {i}"])
        expected.append(["2020-02-15", i, f"this is {i}"])
    columns = ["date", "my_int", "my_string"]
    expected = pd.DataFrame(expected, columns=columns)
    expected["date"] = expected["date"].astype("category")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_partition_by_hash(tmpdir):
    out_file = f"{tmpdir}/test_write_to_parquet_partition_by_hash{uuid.uuid4().hex}"
    controller = build_flow(
        [SyncEmitSource(), ParquetTarget(out_file, columns=["time", "my_int", "my_string"], time_field=0)]
    ).run()

    my_time = datetime(2020, 2, 15)

    expected = []
    for i in range(10):
        controller.emit([my_time, i, f"this is {i}"], key=[i])
        expected.append([my_time, i, f"this is {i}"])
    columns = ["time", "my_int", "my_string"]
    expected = pd.DataFrame(expected, columns=columns)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    read_back_df.sort_values("my_int", inplace=True)
    read_back_df.reset_index(drop=True, inplace=True)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_partition_by_column(tmpdir):
    out_file = f"{tmpdir}/test_write_to_parquet_partition_by_column{uuid.uuid4().hex}"
    controller = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(
                out_file,
                columns=["time", "my_int", "my_string", "even"],
                partition_cols=["even"],
                time_field="time",
            ),
        ]
    ).run()

    my_time = datetime(2020, 2, 15)

    expected = []
    for i in range(10):
        event = "even" if i % 2 == 0 else "odd"
        controller.emit([my_time, i, f"this is {i}", event], key=[i])
        expected.append([my_time, i, f"this is {i}", event])
    columns = ["time", "my_int", "my_string", "even"]
    expected = pd.DataFrame(expected, columns=columns)
    expected["even"] = expected["even"].astype("category")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_file, columns=columns)
    read_back_df.sort_values("my_int", inplace=True)
    read_back_df.reset_index(drop=True, inplace=True)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_with_inference(tmpdir):
    out_dir = f"{tmpdir}/test_write_to_parquet_with_inference{uuid.uuid4().hex}/"
    controller = build_flow([SyncEmitSource(), ParquetTarget(out_dir, index_cols="$key", partition_cols=[])]).run()

    expected = []
    controller.emit({"only_first_event": "first", "my_int": -1}, key="first_key!")
    expected.append(["first_key!", "first", -1, None, None])
    for i in range(10):
        controller.emit({"my_int": i, "my_string": f"this is {i}"}, key=f"key{i}")
        expected.append([f"key{i}", None, i, f"this is {i}", None])
    controller.emit({"only_last_event": "last", "my_int": 1000}, key="last_key!")
    expected.append(["last_key!", None, 1000, None, "last"])
    expected = pd.DataFrame(
        expected,
        columns=["key", "only_first_event", "my_int", "my_string", "only_last_event"],
    )
    expected.set_index(["key"], inplace=True)
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir)
    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_write_to_parquet_with_inference_error_on_partition_index_collision(tmpdir):
    with pytest.raises(ValueError):
        ParquetTarget("out/", index_cols="$key", partition_cols=["$key"])


def test_join_by_key():
    table = Table("test", NoopDriver())
    table._update_static_attrs(9, {"age": 1, "color": "blue9"})
    table._update_static_attrs(7, {"age": 3, "color": "blue7"})

    controller = build_flow(
        [
            SyncEmitSource(),
            Filter(lambda x: x["col1"] > 8),
            JoinWithTable(table, lambda x: x["col1"]),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()
    for i in range(10):
        controller.emit({"col1": i})

    expected = [{"col1": 9, "age": 1, "color": "blue9"}]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected


def test_join_by_key_error():
    table = Table("test", NoopDriver())
    table._update_static_attrs(1, {"age": 1, "color": "blue"})
    table._update_static_attrs(3, {"age": 3, "color": "red"})

    recovery_step = Reduce([], lambda acc, x: append_and_return(acc, x))
    terminal_step = Reduce([], lambda acc, x: append_and_return(acc, x))

    controller = build_flow(
        [
            SyncEmitSource(),
            JoinWithTable(
                table,
                "col1",
                join_function=lambda event, aug: aug["color"],
                recovery_step=recovery_step,
            ),
            terminal_step,
        ]
    ).run()
    for i in range(5):
        controller.emit({"col1": i})

    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == ["blue", "red"]
    assert recovery_step._result == [{"col1": 0}, {"col1": 2}, {"col1": 4}]


def test_join_by_key_full_event():
    table = Table("test", NoopDriver())
    table._update_static_attrs(9, {"age": 1, "color": "blue9"})
    table._update_static_attrs(7, {"age": 3, "color": "blue7"})

    controller = build_flow(
        [
            SyncEmitSource(),
            Filter(lambda x: x["col1"] > 8),
            JoinWithTable(table, "col1", full_event=True),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()
    for i in range(10):
        controller.emit({"col1": i})

    expected = [{"col1": 9, "age": 1, "color": "blue9"}]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected


def test_join_by_string_key():
    table = Table("test", NoopDriver())
    table._update_static_attrs(9, {"age": 1, "color": "blue9"})
    table._update_static_attrs(7, {"age": 3, "color": "blue7"})

    controller = build_flow(
        [
            SyncEmitSource(),
            Filter(lambda x: x["col1"] > 8),
            JoinWithTable(table, "col1"),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()
    for i in range(10):
        controller.emit({"col1": i})

    expected = [{"col1": 9, "age": 1, "color": "blue9"}]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected


def test_join_with_join_function():
    table = Table("test", NoopDriver())
    table._update_static_attrs(2, {"age": 2, "color": "blue"})
    table._update_static_attrs(3, {"age": 3, "color": "red"})

    def join_function(event, aug):
        event.update(aug)
        if event["color"] != "blue":
            event["color"] = "Not blue"
        return event

    controller = build_flow(
        [
            SyncEmitSource(),
            JoinWithTable(table, "col1", inner_join=True, join_function=join_function),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()
    for i in range(5):
        controller.emit({"col1": i})

    expected = [
        {"col1": 2, "age": 2, "color": "blue"},
        {"col1": 3, "age": 3, "color": "Not blue"},
    ]
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == expected


def test_termination_result_order():
    controller = build_flow(
        [
            SyncEmitSource(),
            [Reduce(1, lambda acc, x: acc)],
            [Reduce(2, lambda acc, x: acc)],
        ]
    ).run()

    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 1


def test_termination_result_on_none():
    controller = build_flow(
        [
            SyncEmitSource(),
            [Reduce(None, lambda acc, x: acc)],
            [Reduce(2, lambda acc, x: acc)],
        ]
    ).run()

    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 2


class MockFramesClient:
    def __init__(self):
        self.call_log = []

    def create(self, backend, table, **kwargs):
        kwargs["backend"] = backend
        kwargs["table"] = table
        self.call_log.append(("create", kwargs))

    def write(self, backend, table, dfs, **kwargs):
        kwargs["backend"] = backend
        kwargs["table"] = table
        kwargs["dfs"] = dfs
        self.call_log.append(("write", kwargs))


def test_write_to_tsdb():
    mock_frames_client = MockFramesClient()

    controller = build_flow(
        [
            SyncEmitSource(),
            TSDBTarget(
                path="container/some/path",
                time_col="time",
                index_cols="node",
                columns=["cpu", "disk"],
                rate="1/h",
                max_events=1,
                frames_client=mock_frames_client,
            ),
        ]
    ).run()

    expected_data = []
    date_time_str = "18/09/19 01:55:1"
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + " UTC-0000", "%d/%m/%y %H:%M:%S UTC%z")
        controller.emit([now, i, i + 1, i + 2])
        expected_data.append([now, i, i + 1, i + 2])

    controller.terminate()
    controller.await_termination()

    expected_create = (
        "create",
        {
            "if_exists": 1,
            "rate": "1/h",
            "aggregates": "",
            "aggregation_granularity": "",
            "backend": "tsdb",
            "table": "/some/path",
        },
    )
    assert mock_frames_client.call_log[0] == expected_create
    i = 0
    for write_call in mock_frames_client.call_log[1:]:
        assert write_call[0] == "write"
        expected = pd.DataFrame([expected_data[i]], columns=["time", "node", "cpu", "disk"])
        expected.set_index(keys=["time", "node"], inplace=True)
        res = write_call[1]["dfs"]
        assert expected.equals(res), f"result{res}\n!=\nexpected{expected}"
        del write_call[1]["dfs"]
        assert write_call[1] == {"backend": "tsdb", "table": "/some/path"}
        i += 1


def test_write_dict_to_tsdb():
    mock_frames_client = MockFramesClient()

    controller = build_flow(
        [
            SyncEmitSource(),
            TSDBTarget(
                path="container/some/path",
                time_col="time",
                index_cols="node",
                rate="1/h",
                infer_columns_from_data=True,
                max_events=1,
                frames_client=mock_frames_client,
            ),
        ]
    ).run()

    expected_data = []
    date_time_str = "18/09/19 01:55:1"
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + " UTC-0000", "%d/%m/%y %H:%M:%S UTC%z")
        controller.emit({"time": now, "node": i, "cpu": i + 1, "disk": i + 2})
        expected_data.append([now, i, i + 1, i + 2])

    controller.terminate()
    controller.await_termination()

    expected_create = (
        "create",
        {
            "if_exists": 1,
            "rate": "1/h",
            "aggregates": "",
            "aggregation_granularity": "",
            "backend": "tsdb",
            "table": "/some/path",
        },
    )
    assert mock_frames_client.call_log[0] == expected_create
    i = 0
    for write_call in mock_frames_client.call_log[1:]:
        assert write_call[0] == "write"
        expected = pd.DataFrame([expected_data[i]], columns=["time", "node", "cpu", "disk"])
        expected.set_index(keys=["time", "node"], inplace=True)
        res = write_call[1]["dfs"]
        assert expected.equals(res), f"result{res}\n!=\nexpected{expected}"
        del write_call[1]["dfs"]
        assert write_call[1] == {"backend": "tsdb", "table": "/some/path"}
        i += 1


def test_write_dict_to_tsdb_error():
    mock_frames_client = MockFramesClient()

    controller = build_flow(
        [
            SyncEmitSource(),
            TSDBTarget(
                path="container/some/path",
                time_col="time",
                index_cols="node",
                rate="1/h",
                max_events=1,
                frames_client=mock_frames_client,
            ),
        ]
    ).run()

    expected_data = []
    date_time_str = "18/09/19 01:55:1"
    with pytest.raises(ValueError):
        for i in range(9):
            now = datetime.strptime(date_time_str + str(i) + " UTC-0000", "%d/%m/%y %H:%M:%S UTC%z")
            controller.emit({"time": now, "node": i, "cpu": i + 1, "disk": i + 2})
            expected_data.append([now, i, i + 1, i + 2])

        controller.terminate()
        controller.await_termination()


def test_write_to_tsdb_with_key_index():
    mock_frames_client = MockFramesClient()

    controller = build_flow(
        [
            SyncEmitSource(),
            TSDBTarget(
                path="container/some/path",
                time_col="time",
                index_cols="node=$key",
                columns=["cpu", "disk"],
                rate="1/h",
                max_events=1,
                frames_client=mock_frames_client,
            ),
        ]
    ).run()

    expected_data = []
    date_time_str = "18/09/19 01:55:1"
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + " UTC-0000", "%d/%m/%y %H:%M:%S UTC%z")
        controller.emit([now, i + 1, i + 2], key=i)
        expected_data.append([now, i, i + 1, i + 2])

    controller.terminate()
    controller.await_termination()

    expected_create = (
        "create",
        {
            "if_exists": 1,
            "rate": "1/h",
            "aggregates": "",
            "aggregation_granularity": "",
            "backend": "tsdb",
            "table": "/some/path",
        },
    )
    assert mock_frames_client.call_log[0] == expected_create
    i = 0
    for write_call in mock_frames_client.call_log[1:]:
        assert write_call[0] == "write"
        expected = pd.DataFrame([expected_data[i]], columns=["time", "node", "cpu", "disk"])
        expected.set_index(keys=["time", "node"], inplace=True)
        res = write_call[1]["dfs"]
        assert expected.equals(res), f"result{res}\n!=\nexpected{expected}"
        del write_call[1]["dfs"]
        assert write_call[1] == {"backend": "tsdb", "table": "/some/path"}
        i += 1


def test_write_to_tsdb_with_key_index_and_default_time():
    mock_frames_client = MockFramesClient()

    controller = build_flow(
        [
            SyncEmitSource(),
            TSDBTarget(
                path="container/some/path",
                time_col="time",
                index_cols="node=$key",
                columns=["cpu", "disk"],
                rate="1/h",
                max_events=1,
                frames_client=mock_frames_client,
            ),
        ]
    ).run()

    expected_data = []
    date_time_str = "18/09/19 01:55:1"
    for i in range(9):
        now = datetime.strptime(date_time_str + str(i) + " UTC-0000", "%d/%m/%y %H:%M:%S UTC%z")
        controller.emit([now, i + 1, i + 2], key=i)
        expected_data.append([now, i, i + 1, i + 2])

    controller.terminate()
    controller.await_termination()

    expected_create = (
        "create",
        {
            "if_exists": 1,
            "rate": "1/h",
            "aggregates": "",
            "aggregation_granularity": "",
            "backend": "tsdb",
            "table": "/some/path",
        },
    )
    assert mock_frames_client.call_log[0] == expected_create
    i = 0
    for write_call in mock_frames_client.call_log[1:]:
        assert write_call[0] == "write"
        expected = pd.DataFrame([expected_data[i]], columns=["time", "node", "cpu", "disk"])
        expected.set_index(keys=["time", "node"], inplace=True)
        res = write_call[1]["dfs"]
        assert expected.equals(res), f"result{res}\n!=\nexpected{expected}"
        del write_call[1]["dfs"]
        assert write_call[1] == {"backend": "tsdb", "table": "/some/path"}
        i += 1


def test_csv_reader_parquet_write_microsecs(tmpdir):
    out_file = f"{tmpdir}/test_csv_reader_parquet_write_microsecs_{uuid.uuid4().hex}/"
    columns = ["k", "t"]

    time_format = "%d/%m/%Y %H:%M:%S.%f"
    controller = build_flow(
        [
            CSVSource(
                "tests/test-with-timestamp-microsecs.csv",
                header=True,
                key_field="k",
                parse_dates="t",
                timestamp_format=time_format,
            ),
            ParquetTarget(
                out_file,
                columns=columns,
                partition_cols=["$year", "$month", "$day", "$hour"],
                max_events=2,
            ),
        ]
    ).run()

    expected = pd.DataFrame(
        [
            ["m1", datetime.strptime("15/02/2020 02:03:04.123456", time_format)],
            ["m2", datetime.strptime("16/02/2020 02:03:04.123456", time_format)],
        ],
        columns=columns,
    )
    controller.await_termination()
    read_back_df = pd.read_parquet(out_file, columns=columns)

    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_csv_reader_parquet_write_nanosecs(tmpdir):
    out_file = f"{tmpdir}/test_csv_reader_parquet_write_nanosecs_{uuid.uuid4().hex}/"
    columns = ["k", "t"]

    time_format = "%d/%m/%Y %H:%M:%S.%f"
    controller = build_flow(
        [
            CSVSource(
                "tests/test-with-timestamp-nanosecs.csv",
                header=True,
                key_field="k",
                parse_dates="t",
                timestamp_format=time_format,
            ),
            ParquetTarget(
                out_file,
                columns=columns,
                partition_cols=["$year", "$month", "$day", "$hour"],
                max_events=2,
            ),
        ]
    ).run()

    expected = pd.DataFrame(
        [
            ["m1", datetime.strptime("15/02/2020 02:03:04.123456", time_format)],
            ["m2", datetime.strptime("16/02/2020 02:03:04.123456", time_format)],
        ],
        columns=columns,
    )
    controller.await_termination()
    read_back_df = pd.read_parquet(out_file, columns=columns)

    assert read_back_df.equals(expected), f"{read_back_df}\n!=\n{expected}"


def test_error_in_table_persist():
    table = Table(
        "table",
        V3ioDriver(
            webapi="https://localhost:12345",
            access_key="abc",
            v3io_client_kwargs={"retry_intervals": [0]},
        ),
    )

    controller = build_flow(
        [
            SyncEmitSource(),
            NoSqlTarget(table, columns=["col1"]),
        ]
    ).run()

    controller.emit({"col1": 0}, "tal")

    controller.terminate()
    with pytest.raises(ClientConnectorError):
        controller.await_termination()


def test_async_task_error_and_complete():
    table = Table("table", NoopDriver())

    controller = build_flow([SyncEmitSource(), NoSqlTarget(table), Map(RaiseEx(1).raise_ex), Complete()]).run()

    awaitable_result = controller.emit({"col1": 0}, "tal")
    try:
        with pytest.raises(ATestException):
            awaitable_result.await_result()
    finally:
        controller.terminate()

    with pytest.raises(ATestException):
        controller.await_termination()


def test_async_task_error_and_complete_repeated_emits():
    table = Table("table", NoopDriver())

    controller = build_flow([SyncEmitSource(), NoSqlTarget(table), Map(RaiseEx(1).raise_ex), Complete()]).run()
    for _ in range(3):
        try:
            awaitable_result = controller.emit({"col1": 0}, "tal")
        except ATestException:
            continue
        with pytest.raises(ATestException):
            awaitable_result.await_result()
    controller.terminate()
    with pytest.raises(ATestException):
        controller.await_termination()


def test_push_error():
    class PushErrorContext:
        def push_error(self, event, message, source):
            self.event = event
            self.message = message
            self.source = source

    context = PushErrorContext()
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(RaiseEx(1).raise_ex, context=context),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    controller.emit(0)
    controller.terminate()
    controller.await_termination()
    assert context.event.body == 0
    assert "raise ATestException" in context.message
    assert context.source == "Map"


def test_metadata_fields():
    controller = build_flow(
        [
            SyncEmitSource(key_field="mykey"),
            Reduce([], append_and_return, full_event=True),
        ]
    ).run()

    t1 = datetime(2020, 2, 15, 2, 0)
    t2 = datetime(2020, 2, 15, 2, 1)
    body1 = {"mykey": "k1", "mytime": t1, "otherfield": "x"}
    body2 = {"mykey": "k2", "mytime": t2, "otherfield": "x"}

    controller.emit(body1)
    controller.emit(Event(body2, "k2"))

    controller.terminate()
    result = controller.await_termination()

    assert len(result) == 2

    result1 = result[0]
    assert result1.key == "k1"
    assert result1.body == body1

    result2 = result[1]
    assert result2.key == "k2"
    assert result2.body == body2


async def async_test_async_metadata_fields():
    controller = build_flow(
        [
            AsyncEmitSource(key_field="mykey"),
            Reduce([], append_and_return, full_event=True),
        ]
    ).run()

    body = {"mykey": "k1", "mytime": datetime(2020, 2, 15, 2, 0), "otherfield": "x"}
    await controller.emit(body)
    await controller.terminate()
    result = await controller.await_termination()
    assert len(result) == 1
    result = result[0]
    assert result.key == "k1"
    assert result.body == body


def test_async_metadata_fields():
    asyncio.run(async_test_async_metadata_fields())


def test_uuid():
    def copy_and_set_body(event):
        copy_event = copy.copy(event)
        copy_event.body = copy_event.id
        return copy_event

    controller = build_flow(
        [
            SyncEmitSource(),
            Map(copy_and_set_body, full_event=True),
            Reduce([], append_and_return),
        ]
    ).run()

    for _ in range(1025):
        controller.emit(0)

    controller.terminate()
    result = controller.await_termination()

    assert len(result) == 1025
    base_id = result[0][:32]
    for i, cur_id in enumerate(result[:1024]):
        assert cur_id == f"{base_id}-{i:04}"
    assert result[1024][:32] != base_id
    assert result[1024][32:] == "-0000"


def test_input_path():
    controller = build_flow(
        [
            SyncEmitSource(),
            Filter(lambda x: x < 5, input_path="col2.col3"),  # filter emits the full event
            Map(lambda x: x + 1, input_path="col2.col3"),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    for i in range(10):
        val = 5 if i % 2 == 0 else 1
        controller.emit({"col1": i, "col2": {"col3": val}})
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 10


def test_result_path():
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: {"new_field": 5}, result_path="step_result"),
            Reduce(0, lambda acc, x: x),
        ]
    ).run()

    controller.emit({"col1": 1})
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == {"col1": 1, "step_result": {"new_field": 5}}


def test_to_dict():
    source = SyncEmitSource(name="my_source", buffer_size=5)
    identity = Map(lambda x: x, full_event=True, not_in_use=None)
    assert source.to_dict() == {
        "class_name": "storey.sources.SyncEmitSource",
        "class_args": {"buffer_size": 5},
        "name": "my_source",
    }
    assert identity.to_dict() == {
        "class_name": "storey.flow.Map",
        "class_args": {"not_in_use": None},
        "full_event": True,
        "name": "Map",
    }


def test_flow_reuse():
    flow = build_flow([SyncEmitSource(), Map(lambda x: x + 1), Reduce(0, lambda acc, x: acc + x)])

    for _ in range(3):
        controller = flow.run()
        for i in range(10):
            controller.emit(i)
        controller.terminate()
        result = controller.await_termination()
        assert result == 55


def test_flow_to_dict_read_csv():
    step = CSVSource(
        "tests/test-with-timestamp-microsecs.csv",
        header=True,
        key_field="k",
        time_field="t",
        timestamp_format="%d/%m/%Y %H:%M:%S.%f",
    )
    assert step.to_dict() == {
        "class_name": "storey.sources.CSVSource",
        "class_args": {
            "build_dict": False,
            "header": True,
            "key_field": "k",
            "paths": "tests/test-with-timestamp-microsecs.csv",
            "time_field": "t",
            "timestamp_format": "%d/%m/%Y %H:%M:%S.%f",
            "type_inference": True,
        },
        "name": "CSVSource",
    }


def test_flow_to_dict_write_to_parquet():
    step = ParquetTarget("outdir", columns=["col1", "col2"], max_events=2)
    assert step.to_dict() == {
        "class_name": "storey.targets.ParquetTarget",
        "class_args": {
            "path": "outdir",
            "columns": ["col1", "col2"],
            "max_events": 2,
            "flush_after_seconds": 60,
        },
        "name": "ParquetTarget",
    }


def test_flow_to_dict_write_to_tsdb():
    step = TSDBTarget(
        path="some/path",
        time_col="time",
        index_cols="node",
        columns=["cpu", "disk"],
        rate="1/h",
        max_events=1,
        frames_client=MockFramesClient(),
    )

    assert step.to_dict() == {
        "class_name": "storey.targets.TSDBTarget",
        "class_args": {
            "columns": ["cpu", "disk"],
            "index_cols": "node",
            "max_events": 1,
            "path": "some/path",
            "rate": "1/h",
            "time_col": "time",
        },
        "name": "TSDBTarget",
    }


def test_flow_to_dict_dataframe_source():
    df = pd.DataFrame(
        [["key1", datetime(2020, 2, 15), "id1", 1.1]],
        columns=["my_key", "my_time", "my_id", "my_value"],
    )
    step = DataframeSource(df, key_field="my_key", time_field="my_time", id_field="my_id")

    assert step.to_dict() == {
        "class_name": "storey.sources.DataframeSource",
        "class_args": {
            "id_field": "my_id",
            "key_field": "my_key",
            "time_field": "my_time",
        },
        "name": "DataframeSource",
    }


def test_flow_to_dict_concurrent_job_execution():
    step = _ConcurrentJobExecution(retries=2)
    assert step.to_dict() == {
        "class_args": {"retries": 2},
        "class_name": "storey.flow._ConcurrentJobExecution",
        "name": "_ConcurrentJobExecution",
    }


def test_to_code():
    flow = build_flow(
        [
            SyncEmitSource(),
            Batch(5),
            ToDataFrame(index=[]),
            Reduce([], append_and_return, full_event=True),
        ]
    )

    reconstructed_code = flow.to_code()
    expected = """sync_emit_source0 = SyncEmitSource()
batch0 = Batch(max_events=5)
to_data_frame0 = ToDataFrame()
reduce0 = Reduce(full_event=True, initial_value=[])

sync_emit_source0.to(batch0)
batch0.to(to_data_frame0)
to_data_frame0.to(reduce0)
"""
    assert reconstructed_code == expected


def test_split_flow_to_code():
    flow = build_flow(
        [
            SyncEmitSource(),
            [Batch(5), Reduce([], lambda x: len(x))],
            Batch(5),
            ToDataFrame(index=[]),
            Reduce([], append_and_return, full_event=True),
        ]
    )

    reconstructed_code = flow.to_code()
    expected = """sync_emit_source0 = SyncEmitSource()
batch0 = Batch(max_events=5)
reduce0 = Reduce(initial_value=[])
batch1 = Batch(max_events=5)
to_data_frame0 = ToDataFrame()
reduce1 = Reduce(full_event=True, initial_value=[])

sync_emit_source0.to(batch0)
batch0.to(reduce0)
sync_emit_source0.to(batch1)
batch1.to(to_data_frame0)
to_data_frame0.to(reduce1)
"""
    assert reconstructed_code == expected


def test_reader_writer_to_code():
    flow = build_flow([CSVSource("mycsv.csv"), ParquetTarget("mypq")])

    reconstructed_code = flow.to_code()
    print(reconstructed_code)
    expected = """c_s_v_source0 = CSVSource(paths='mycsv.csv', header=True, build_dict=False, type_inference=True)
parquet_target0 = ParquetTarget(path='mypq', max_events=10000, flush_after_seconds=60)

c_s_v_source0.to(parquet_target0)
"""
    assert reconstructed_code == expected


def test_illegal_step_no_source():
    try:
        Reduce([], append_and_return, full_event=True).run()
        raise AssertionError()
    except ValueError as ex:
        assert str(ex) == "Flow must start with a source"


def test_illegal_step_source_not_first_step():
    df = pd.DataFrame([["hello", 1, 1.5], ["world", 2, 2.5]], columns=["string", "int", "float"])
    try:
        build_flow(
            [
                ParquetSource("tests"),
                DataframeSource(df),
                Reduce([], append_and_return),
            ]
        ).run()
        raise AssertionError()
    except ValueError as ex:
        assert str(ex) == "DataframeSource can only appear as the first step of a flow"


def test_writer_downstream(tmpdir):
    file_path = f"{tmpdir}/test_writer_downstream/out.csv"
    controller = build_flow(
        [
            SyncEmitSource(),
            CSVTarget(file_path, columns=["n", "n*10"], header=True),
            Reduce(0, lambda acc, x: acc + x[0]),
        ]
    ).run()

    for i in range(10):
        controller.emit([i, i * 10])

    controller.terminate()
    result = controller.await_termination()
    assert result == 45


def test_complete_in_error_flow():
    reduce = build_flow([Complete(), Reduce(0, lambda acc, x: acc + x)])
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x: x + 1),
            Map(RaiseEx(5).raise_ex, recovery_step=reduce),
            Map(lambda x: x * 100),
            reduce,
        ]
    ).run()

    for i in range(10):
        awaitable_result = controller.emit(i)
        if i == 4:
            assert awaitable_result.await_result() == i + 1
        else:
            assert awaitable_result.await_result() == (i + 1) * 100
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 5005


def test_non_existing_key_query_by_key():
    df = pd.DataFrame(
        [["katya", "green", "hod hasharon"], ["dina", "blue", "ramat gan"]],
        columns=["name", "color", "city"],
    )
    table = Table("table", NoopDriver())
    controller = build_flow(
        [
            DataframeSource(df, key_field="name"),
            NoSqlTarget(table),
        ]
    ).run()
    controller.await_termination()

    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["color"], table, key_field="name"),
            QueryByKey(["city"], table, key_field="name"),
        ]
    ).run()

    controller.emit({"nameeeee": "katya"}, "katya")
    controller.terminate()
    controller.await_termination()


# ML-2257
def test_query_by_key_edge_case_field_name():
    table = Table("table", NoopDriver())
    QueryByKey(["my_color_5sec"], table, key_field="name")


# ML-3782
def test_query_by_key_non_aggregate():
    table = Table("table", NoopDriver())
    query_by_key = QueryByKey(["my_color_5h"], table, key_field="name")
    assert query_by_key._aggrs == []
    assert query_by_key._enrich_cols == ["my_color_5h"]


def test_csv_source_with_none_values():
    controller = build_flow(
        [
            CSVSource("tests/test-with-none-values.csv", key_field="string"),
            Reduce([], append_and_return, full_event=True),
        ]
    ).run()

    termination_result = controller.await_termination()

    assert len(termination_result) == 2
    assert termination_result[0].key == "a"
    assert termination_result[0].body == [
        "a",
        True,
        False,
        1,
        2.3,
        "2021-04-21 15:56:53.385444",
    ]
    assert termination_result[1].key == "b"
    excepted_result = ["b", True, math.nan, math.nan, math.nan, math.nan]
    for x, y in zip(termination_result[1].body, excepted_result):
        assert (isinstance(x, float) and isinstance(y, float) and math.isnan(x) and math.isnan(y)) or x == y


def test_csv_source_event_metadata():
    controller = build_flow(
        [
            CSVSource(
                "tests/test-with-timestamp.csv",
                header=True,
                build_dict=True,
                key_field="k",
                time_field="t",
                timestamp_format="%d/%m/%Y %H:%M:%S",
                id_field="k",
            ),
            ReifyMetadata({"key", "id"}),
            Reduce([], append_and_return, full_event=False),
        ]
    ).run()

    termination_result = controller.await_termination()

    assert termination_result == [
        {
            "b": True,
            "id": "m1",
            "k": "m1",
            "key": "m1",
            "t": datetime(2020, 2, 15, 2, 0),
            "v": 8,
        },
        {
            "b": False,
            "id": "m2",
            "k": "m2",
            "key": "m2",
            "t": datetime(2020, 2, 16, 2, 0),
            "v": 14,
        },
    ]


def test_none_key_is_not_written():
    data = pd.DataFrame({"first_name": ["moshe", None, "katya"], "some_data": [1, 2, 3]})
    data.set_index(keys=["first_name"], inplace=True)

    controller = build_flow(
        [
            DataframeSource(data, key_field=["first_name"]),
            Reduce([], append_and_return),
        ]
    ).run()
    result = controller.await_termination()
    expected = [
        {"first_name": "moshe", "some_data": 1},
        {"first_name": "katya", "some_data": 3},
    ]

    assert result == expected


def test_none_key_num_is_not_written():
    data = pd.DataFrame({"index": [10, None, 20], "some_data": [1, 2, 3]})
    data.set_index(keys=["index"], inplace=True)

    controller = build_flow(
        [
            DataframeSource(data, key_field=["index"]),
            Reduce([], append_and_return),
        ]
    ).run()
    result = controller.await_termination()
    expected = [{"index": 10, "some_data": 1}, {"index": 20, "some_data": 3}]

    assert result == expected


def test_dataframe_source_missing_key_column():
    with pytest.raises(ValueError) as value_error:
        data = pd.DataFrame({"id": [1, 2, 3], "some_data": ["data", "random_data", "my_data"]})

        controller = build_flow(
            [
                DataframeSource(dfs=data, key_field="non_existent_column"),
            ]
        ).run()
        controller.await_termination()

    assert str(value_error.value) == "key column 'non_existent_column' is missing from dataframe."


def test_dataframe_source_missing_id_column():
    with pytest.raises(ValueError) as value_error:
        data = pd.DataFrame({"id": [1, 2, 3], "some_data": ["data", "random_data", "my_data"]})

        controller = build_flow(
            [
                DataframeSource(dfs=data, id_field="non_existent_column"),
            ]
        ).run()
        controller.await_termination()

    assert str(value_error.value) == "id column 'non_existent_column' is missing from dataframe."


def test_none_key_date_is_not_written():
    data = pd.DataFrame(
        {
            "index": [
                datetime(2020, 6, 27, 10, 23, 8, 420581),
                None,
                datetime(2020, 6, 28, 10, 23, 8, 420581),
            ],
            "some_data": [1, 2, 3],
        }
    )
    data.set_index(keys=["index"], inplace=True)

    controller = build_flow(
        [
            DataframeSource(data, key_field=["index"]),
            Reduce([], append_and_return),
        ]
    ).run()
    result = controller.await_termination()
    expected = [
        {"index": datetime(2020, 6, 27, 10, 23, 8, 420581), "some_data": 1},
        {"index": datetime(2020, 6, 28, 10, 23, 8, 420581), "some_data": 3},
    ]

    assert result == expected


def test_not_string_key_field():
    with pytest.raises(ValueError) as value_error:
        build_flow(
            [
                CSVSource("tests/test.csv", key_field=0),
            ]
        ).run()

    assert str(value_error.value) == "key_field must be a string or list of strings"


def test_csv_none_value_first_row(tmpdir):
    out_file_par = f"{tmpdir}/test_csv_none_value_first_row_{uuid.uuid4().hex}.parquet"
    out_file_csv = f"{tmpdir}/test_csv_none_value_first_row_{uuid.uuid4().hex}.csv"

    columns = ["first_name", "bid", "bool", "time"]
    data = pd.DataFrame(
        [
            ["katya", None, None, None],
            ["dina", 45.7, True, datetime(2021, 4, 21, 15, 56, 53, 385444)],
        ],
        columns=columns,
    )
    data.to_csv(out_file_csv)

    controller = build_flow(
        [
            CSVSource(out_file_csv, key_field="first_name", build_dict=True),
            ParquetTarget(out_file_par),
        ]
    ).run()

    controller.await_termination()
    read_back_df = pd.read_parquet(out_file_par)

    u = pd.read_csv(out_file_csv)
    u.to_parquet(out_file_par)
    r2 = pd.read_parquet(out_file_par)

    for c in columns:
        assert read_back_df.dtypes.to_dict()[c] == r2.dtypes.to_dict()[c]


def test_csv_none_value_string(tmpdir):
    out_file_par = f"{tmpdir}/test_csv_none_value_first_row_{uuid.uuid4().hex}.parquet"
    out_file_csv = f"{tmpdir}/test_csv_none_value_first_row_{uuid.uuid4().hex}.csv"

    columns = ["first_name", "str"]
    data = pd.DataFrame([["katya", "strrrr"], ["dina", None]], columns=columns)
    data.to_csv(out_file_csv)

    controller = build_flow(
        [
            CSVSource(out_file_csv, key_field="first_name", build_dict=True),
            ParquetTarget(out_file_par),
        ]
    ).run()

    controller.await_termination()
    read_back_df = pd.read_parquet(out_file_par)

    u = pd.read_csv(out_file_csv)
    u.to_parquet(out_file_par)
    r2 = pd.read_parquet(out_file_par)

    assert r2["str"].compare(read_back_df["str"]).empty


def test_csv_multiple_time_columns(tmpdir):
    controller = build_flow(
        [
            CSVSource(
                "tests/test-multiple-time-columns.csv",
                header=True,
                time_field="t1",
                parse_dates=["t2"],
            ),
            Reduce([], append_and_return),
        ]
    ).run()

    termination_result = controller.await_termination()

    expected = [
        [
            "m1",
            datetime(2020, 6, 27, 10, 23, 8, 420581),
            "katya",
            datetime(2020, 6, 27, 12, 23, 8, 420581),
        ],
        [
            "m2",
            datetime(2021, 6, 27, 10, 23, 8, 420581),
            "dina",
            datetime(2021, 6, 27, 10, 21, 8, 420581),
        ],
    ]

    assert termination_result == expected


# ML-846 (inserting multiple columns in pandas 1.3)
def test_reduce_to_df_multiple_indexes():
    index_columns = ["szc", "gca", "pzi"]
    controller = build_flow(
        [
            SyncEmitSource(key_field=index_columns),
            ReduceToDataFrame(index=index_columns, insert_key_column_as=index_columns),
        ]
    ).run()

    a1 = {
        "time_stamp": pd.Timestamp("2002-04-01 04:32:34"),
        "szc": 0.4,
        "itz": False,
        "pzi": 2922242126195791,
        "gca": 0.05,
    }
    a2 = {
        "time_stamp": pd.Timestamp("2002-04-01 15:05:37"),
        "szc": 0.5,
        "itz": True,
        "pzi": -9144607787498184,
        "gca": 0.79,
    }

    controller.emit(a1)
    controller.emit(a2)

    expected = pd.DataFrame([a1, a2], columns=None)
    expected.set_index(index_columns, inplace=True)
    controller.terminate()
    termination_result = controller.await_termination()

    assert_frame_equal(expected, termination_result)


def test_func_parquet_target_terminate(tmpdir):
    out_file = f"{tmpdir}/test_func_parquet_target_terminate_{uuid.uuid4().hex}/"

    dictionary = {}

    def my_func(param1, param2):
        dictionary[param1] = param2

    data = [
        ["dina", pd.Timestamp("2019-07-01 00:00:00"), "tel aviv"],
        ["uri", pd.Timestamp("2018-12-30 09:00:00"), "tel aviv"],
        ["katya", pd.Timestamp("2020-12-31 14:00:00"), "hod hasharon"],
    ]

    df = pd.DataFrame(data, columns=["my_string", "my_time", "my_city"])
    df.set_index("my_string")

    controller = build_flow([DataframeSource(df), ParquetTarget(path=out_file, update_last_written=my_func)]).run()

    controller.await_termination()

    assert len(dictionary) == 1


def test_completion_on_error_in_concurrent_execution_step():
    class _ErrorInConcurrentExecution(_ConcurrentJobExecution):
        async def _process_event(self, event):
            pass

        async def _handle_completed(self, event, response):
            raise ATestException()

    controller = build_flow([SyncEmitSource(), _ErrorInConcurrentExecution(), Complete()]).run()

    awaitable_result = controller.emit(1)
    try:
        with pytest.raises(ATestException):
            awaitable_result.await_result()
    finally:
        controller.terminate()


@pytest.mark.parametrize("backoff_factor", [(0, 2, 0), (1, 2, 3), (0, 1, None)])
def test_completion_after_retry_in_concurrent_execution_step(backoff_factor):
    backoff_factor, retries, expected_sleep = backoff_factor

    class _ErrorInConcurrentExecution(_ConcurrentJobExecution):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self._nums_called = 0

        async def _process_event(self, event):
            self._nums_called += 1
            if self._nums_called <= 2:  # fail twice
                raise ATestException()

        async def _handle_completed(self, event, response):
            return await self._do_downstream(event)

    controller = build_flow(
        [
            SyncEmitSource(),
            _ErrorInConcurrentExecution(retries=retries, backoff_factor=backoff_factor),
            Complete(),
        ]
    ).run()

    awaitable_result = controller.emit(1)
    try:
        start = time.time()
        if expected_sleep is None:
            with pytest.raises(ATestException):
                awaitable_result.await_result()
        else:
            awaitable_result.await_result()
        end = time.time()
    finally:
        controller.terminate()
    if expected_sleep is None:
        with pytest.raises(ATestException):
            controller.await_termination()
    else:
        controller.await_termination()
        assert end - start > expected_sleep


# ML-1506
@pytest.mark.parametrize("max_in_flight", [1, 2, 4])
def test_concurrent_execution_max_in_flight(max_in_flight):
    class _TestConcurrentExecution(_ConcurrentJobExecution):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self._ongoing_processing = 0
            self.lazy_init_called = 0
            self.handle_completed_called = 0

        async def _lazy_init(self):
            self.lazy_init_called += 1

        async def _process_event(self, event):
            self._ongoing_processing += 1
            assert self._ongoing_processing <= max_in_flight
            await asyncio.sleep(1)
            self._ongoing_processing -= 1

        async def _handle_completed(self, event, response):
            self.handle_completed_called += 1

    concurrent_step = _TestConcurrentExecution(max_in_flight=max_in_flight)
    controller = build_flow(
        [
            SyncEmitSource(),
            concurrent_step,
        ]
    ).run()

    num_events = max_in_flight + 1
    for i in range(num_events):
        controller.emit(i)
    controller.terminate()
    controller.await_termination()

    assert concurrent_step.lazy_init_called == 1
    assert concurrent_step.handle_completed_called == num_events


def test_concurrent_execution_max_in_flight_error():
    class _TestConcurrentExecution(_ConcurrentJobExecution):
        async def _process_event(self, event):
            raise ATestException()

        async def _handle_completed(self, event, response):
            pass

    concurrent_step = _TestConcurrentExecution(max_in_flight=2)
    controller = build_flow([SyncEmitSource(), concurrent_step, Complete()]).run()

    awaitable_result = controller.emit(0)
    with pytest.raises(ATestException):
        awaitable_result.await_result()
    controller.terminate()
    with pytest.raises(ATestException):
        controller.await_termination()


def test_concurrent_execution_max_in_flight_push_error():
    class _TestConcurrentExecution(_ConcurrentJobExecution):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self._should_raise = True

        async def _process_event(self, event):
            if self._should_raise:
                self._should_raise = False
                raise ATestException()

        async def _handle_completed(self, event, response):
            await self._do_downstream(event)

    class ContextWithPushError(Context):
        def push_error(self, event, message, source):
            pass

    context = ContextWithPushError()

    concurrent_step = _TestConcurrentExecution(max_in_flight=2, context=context)
    controller = build_flow([SyncEmitSource(), concurrent_step, Complete()]).run()

    awaitable_result = controller.emit(0)
    with pytest.raises(ATestException):
        awaitable_result.await_result()
    for i in range(1, 5):
        awaitable_result = controller.emit(i)
        awaitable_result.await_result()
    controller.terminate()
    controller.await_termination()


def test_event_to_string():
    event = Event("body", "key")
    assert str(event) == "Event(id=None, key=key, body=body)"


class MockLogger:
    def __init__(self):
        self.logs = []

    def error(self, *args, **kwargs):
        self.logs.append(("error", args, kwargs))

    def warn(self, *args, **kwargs):
        self.logs.append(("warn", args, kwargs))

    def info(self, *args, **kwargs):
        self.logs.append(("info", args, kwargs))

    def debug(self, *args, **kwargs):
        self.logs.append(("debug", args, kwargs))


class MockContext:
    def __init__(self, logger, verbose):
        self.logger = logger
        self.verbose = verbose


def test_verbose_logs():
    logger = MockLogger()
    context = MockContext(logger, True)

    controller = build_flow(
        [
            SyncEmitSource(context=context),
            Map(lambda x: x, name="Map1", context=context),
            Map(lambda x: x, name="Map2", context=context),
        ]
    ).run()

    controller.emit(Event(id="myid", body={}))
    controller.terminate()
    controller.await_termination()

    assert len(logger.logs) == 2

    level, args, kwargs = logger.logs[0]
    assert level == "debug"
    assert args == ("SyncEmitSource -> Map1 | Event(id=myid, path=/, body={})",)
    assert kwargs == {}

    level, args, kwargs = logger.logs[1]
    assert level == "debug"
    assert args == ("Map1 -> Map2 | Event(id=myid, path=/, body={})",)
    assert kwargs == {}


# ML-1716
def test_init_of_recovery_step():
    class WasInitCalled(storey.Flow):
        def __init__(self):
            super().__init__()
            self.times_init_called = 0

        def _init(self):
            super()._init()
            self.times_init_called += 1

        async def _do(self, event):
            return event

    was_init_called_step = WasInitCalled()

    controller = build_flow([SyncEmitSource(), Map(lambda x: x, recovery_step=was_init_called_step)]).run()

    controller.terminate()
    controller.await_termination()

    assert was_init_called_step.times_init_called == 1


# ML-1727
@pytest.mark.parametrize(
    ["long_running", "use_mapclass"],
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_long_running_parameter(long_running, use_mapclass):
    class CheckTime(storey.Flow):
        def __init__(self):
            super().__init__()
            self.failed = False
            self._worker_task = None
            self._terminate = False

        async def worker(self):
            last_time = time.monotonic()
            while not self._terminate:
                await asyncio.sleep(0)
                time_now = time.monotonic()
                self.failed = self.failed or time_now > last_time + 0.5
                last_time = time_now

        async def _do(self, event):
            if not self._worker_task:
                self._worker_task = asyncio.create_task(self.worker())
            if event is storey.dtypes._termination_obj:
                self._terminate = True
                await self._worker_task
            return await self._do_downstream(event)

    def sleep_and_return(event):
        time.sleep(0.6)
        return event

    class MyLongMap(MapClass):
        def do(self, event):
            return sleep_and_return(event)

    check_time = CheckTime()
    if use_mapclass:
        map_step = MyLongMap(long_running=long_running)
    else:
        map_step = Map(sleep_and_return, long_running=long_running)
    controller = build_flow(
        [
            SyncEmitSource(),
            map_step,
            check_time,
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    controller.emit(1)
    controller.emit(2)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [1, 2]

    should_fail = not long_running
    assert check_time.failed == should_fail


def test_rename():
    controller = build_flow(
        [
            SyncEmitSource(),
            Rename({}),
            Rename({"a": "b", "c": "d"}),
            Rename({"d": "c"}),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    controller.emit({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [{"b": 1, "c": 3, "e": 5}]


def test_read_sql_db():
    # using `table_1; DROP DATABASE test;` as the table name for testing injection
    import sqlalchemy as db

    engine = db.create_engine(integration.conftest.SQLITE_DB)
    with engine.connect() as conn:
        origin_df = pd.DataFrame(
            {
                "string": ["hello", "world"],
                "int": [1, 2],
                "float": [1.5, 2.5],
                "time": [pd.Timestamp(2017, 1, 1, 12), pd.Timestamp(2017, 1, 1, 12)],
            }
        )
        origin_df.to_sql("table_1; DROP DATABASE test;", conn, if_exists="replace", index=False)
    controller = build_flow(
        [
            SQLSource(
                "sqlite:///test.db", "table_1; DROP DATABASE test;", "string", id_field="int", time_fields=["time"]
            ),
            Reduce([], append_and_return),
        ]
    ).run()

    actual = controller.await_termination()
    expected = [
        {"string": "hello", "int": 1, "float": 1.5, "time": pd.Timestamp(2017, 1, 1, 12)},
        {"string": "world", "int": 2, "float": 2.5, "time": pd.Timestamp(2017, 1, 1, 12)},
    ]
    assert actual == expected


# Time zone related parameterization is to test for ML-3566
@pytest.mark.parametrize(
    ["data_with_timezone", "filter_with_timezone"], [[True, True], [True, False], [False, True], [False, False]]
)
def test_filter_by_time_non_partitioned(data_with_timezone, filter_with_timezone):
    columns = ["my_string", "my_time", "my_city"]

    data_timezone_suffix = "Z" if data_with_timezone else ""

    df = pd.DataFrame(
        [
            ["dina", pd.Timestamp(f"2019-07-01 00:00:00{data_timezone_suffix}"), "tel aviv"],
            ["uri", pd.Timestamp(f"2018-12-30 09:00:00{data_timezone_suffix}"), "tel aviv"],
            ["katya", pd.Timestamp(f"2020-12-31 14:00:00{data_timezone_suffix}"), "hod hasharon"],
        ],
        columns=columns,
    )
    df.set_index("my_string")
    path = "/tmp/test_filter_by_time_non_partitioned.parquet"
    df.to_parquet(path)
    start = datetime.fromisoformat("2019-07-01 00:00:00" + ("+00:00" if data_with_timezone else ""))
    end = pd.Timestamp("2020-12-31 14:00:00" + ("Z" if data_with_timezone else ""))

    controller = build_flow(
        [
            ParquetSource(path, start_filter=start, end_filter=end, filter_column="my_time"),
            Reduce([], append_and_return),
        ]
    ).run()

    read_back_result = controller.await_termination()

    expected = [
        {
            "my_string": "katya",
            "my_time": pd.Timestamp(f"2020-12-31 14:00:00{data_timezone_suffix}"),
            "my_city": "hod hasharon",
        }
    ]

    try:
        assert read_back_result == expected, f"{read_back_result}\n!=\n{expected}"
    finally:
        os.remove(path)


def test_empty_filter_result():
    columns = ["my_string", "my_time", "my_city"]

    df = pd.DataFrame(
        [
            ["dina", pd.Timestamp("2019-07-01 00:00:00"), "tel aviv"],
            ["uri", pd.Timestamp("2018-12-30 09:00:00"), "tel aviv"],
            ["katya", pd.Timestamp("2020-12-31 14:00:00"), "hod hasharon"],
        ],
        columns=columns,
    )
    df.set_index("my_string")
    path = "/tmp/test_empty_filter_result.parquet"
    df.to_parquet(path)
    start = pd.Timestamp("2022-07-01 00:00:00")
    end = pd.Timestamp("2022-12-31 14:00:00")

    controller = build_flow(
        [
            ParquetSource(path, start_filter=start, end_filter=end, filter_column="my_time"),
            ReduceToDataFrame(index=["my_string"], insert_key_column_as=["my_string"]),
        ]
    ).run()

    read_back_result = controller.await_termination()

    try:
        pd.testing.assert_frame_equal(read_back_result, pd.DataFrame({}))
    finally:
        os.remove(path)
