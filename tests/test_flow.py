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
import multiprocessing.context
import os
import queue
import tempfile
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
from packaging import version
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
from storey.flow import (
    ConcurrentExecution,
    Context,
    ParallelExecution,
    ParallelExecutionRunnable,
    ReifyMetadata,
    Rename,
    RunnableExecutor,
    _Batching,
    _ConcurrentJobExecution,
)
from tests.helpers import MockContext, MockLogger


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


def test_pass_context_to_function():
    controller = build_flow(
        [
            SyncEmitSource(),
            Map(lambda x, context: x + context, pass_context=True, context=10),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    for i in range(5):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 60


class Committer:
    def __init__(self):
        self.offsets = {}

    async def explicit_ack(self, qualified_offset):
        qualified_shard = (qualified_offset.topic, qualified_offset.partition)
        offset = qualified_offset.offset
        current_offset = self.offsets.get(qualified_shard, 0)
        assert current_offset < offset
        self.offsets[qualified_shard] = offset


class BadCommitter:
    def __init__(self):
        self.offsets = {}

    async def explicit_ack(self, qualified_offset):
        raise RuntimeError("Something went wrong")


class CommitterContext:
    def __init__(self, platform, logger=None, verbose=None):
        self.platform = platform
        self.logger = logger
        self.verbose = verbose


class EventHoarder(storey.Flow):
    events = []

    async def _do(self, event):
        if event is storey.dtypes._termination_obj:
            self.events = []
            print("Hoarder terminated")
        else:
            self.events.append(event)
            print("Hoarder hoards!")
        return await self._do_downstream(event)


class ErrorOnTermination(storey.Flow):
    async def _do(self, event):
        if event is storey.dtypes._termination_obj:
            raise ATestException("We raise this error on termination on purpose")
        return await self._do_downstream(event)


def test_offset_commit():
    platform = Committer()
    context = CommitterContext(platform)

    controller = build_flow(
        [
            SyncEmitSource(context=context, explicit_ack=True),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            FlatMap(lambda x: [x, x * 10]),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    num_shards = 10
    num_records_per_shard = 10

    for offset in range(1, num_records_per_shard + 1):
        for shard in range(num_shards):
            event = Event(shard)
            event.shard_id = shard
            event.offset = offset
            controller.emit(event)
    termination_result = controller.terminate(wait=True)
    assert termination_result == 330

    offsets = copy.copy(platform.offsets)
    assert offsets == {("/", i): num_records_per_shard for i in range(num_shards)}


async def async_offset_commit():
    platform = Committer()
    context = CommitterContext(platform)

    controller = build_flow(
        [
            AsyncEmitSource(context=context, explicit_ack=True, max_wait_before_commit=1),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            FlatMap(lambda x: [x, x * 10]),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    num_shards = 10
    num_records_per_shard = 10

    for offset in range(1, num_records_per_shard + 1):
        for shard in range(num_shards):
            event = Event(shard)
            event.shard_id = shard
            event.offset = offset
            await controller.emit(event)
    del event

    # Make sure that offsets are committed even before termination
    await asyncio.sleep(2)
    offsets = copy.copy(platform.offsets)

    try:
        assert offsets == {("/", i): num_records_per_shard for i in range(num_shards)}
    finally:
        termination_result = await controller.terminate(wait=True)

    assert termination_result == 330


def test_async_offset_commit():
    asyncio.run(async_offset_commit())


def test_offset_commit_before_termination():
    platform = Committer()
    context = CommitterContext(platform)

    max_wait_before_commit = 1

    controller = build_flow(
        [
            SyncEmitSource(context=context, explicit_ack=True, max_wait_before_commit=max_wait_before_commit),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            FlatMap(lambda x: [x, x * 10]),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    num_shards = 10
    num_records_per_shard = 10

    for offset in range(1, num_records_per_shard + 1):
        for shard in range(num_shards):
            event = Event(shard)
            event.shard_id = shard
            event.offset = offset
            controller.emit(event)

    del event

    time.sleep(max_wait_before_commit + 1)

    expected_offsets = {("/", i): num_records_per_shard for i in range(num_shards)}

    try:
        offsets = copy.copy(platform.offsets)
        assert offsets == expected_offsets
    finally:
        controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 330


async def async_offset_commit_before_termination():
    platform = Committer()
    context = CommitterContext(platform)

    max_wait_before_commit = 1

    controller = build_flow(
        [
            AsyncEmitSource(context=context, explicit_ack=True, max_wait_before_commit=max_wait_before_commit),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            FlatMap(lambda x: [x, x * 10]),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    num_shards = 10
    num_records_per_shard = 10

    for offset in range(1, num_records_per_shard + 1):
        for shard in range(num_shards):
            event = Event(shard)
            event.shard_id = shard
            event.offset = offset
            await controller.emit(event)

    del event

    await asyncio.sleep(max_wait_before_commit + 1)

    try:
        offsets = copy.copy(platform.offsets)
        assert offsets == {("/", i): num_records_per_shard for i in range(num_shards)}
    finally:
        await controller.terminate()
    termination_result = await controller.await_termination()
    assert termination_result == 330


def test_async_offset_commit_before_termination():
    asyncio.run(async_offset_commit_before_termination())


async def async_offset_commit_before_termination_with_nosqltarget():
    platform = Committer()
    context = CommitterContext(platform)

    max_wait_before_commit = 1

    controller = build_flow(
        [
            AsyncEmitSource(context=context, explicit_ack=True, max_wait_before_commit=max_wait_before_commit),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            FlatMap(lambda x: [x, x * 10]),
            NoSqlTarget(Table("/", NoopDriver(), flush_interval_secs=None)),
        ]
    ).run()

    num_shards = 10
    num_records_per_shard = 10

    for offset in range(1, num_records_per_shard + 1):
        for shard in range(num_shards):
            event = Event(shard, "abc")
            event.shard_id = shard
            event.offset = offset
            await controller.emit(event)

    del event

    await asyncio.sleep(max_wait_before_commit + 1)

    try:
        offsets = copy.copy(platform.offsets)
        assert offsets == {("/", i): num_records_per_shard for i in range(num_shards)}
    finally:
        await controller.terminate()
    await controller.await_termination()


# ML-4421
def test_async_offset_commit_before_termination_with_nosqltarget():
    asyncio.run(async_offset_commit_before_termination_with_nosqltarget())


async def async_offset_commit_before_termination_with_concurrent_execution():
    platform = Committer()
    context = CommitterContext(platform)

    max_wait_before_commit = 1

    controller = build_flow(
        [
            AsyncEmitSource(context=context, explicit_ack=True, max_wait_before_commit=max_wait_before_commit),
            ConcurrentExecution(event_processor=lambda x: x + 1),
            Filter(lambda x: x < 3),
            FlatMap(lambda x: [x, x * 10]),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    num_shards = 10
    num_records_per_shard = 10

    for offset in range(1, num_records_per_shard + 1):
        for shard in range(num_shards):
            event = Event(shard)
            event.shard_id = shard
            event.offset = offset
            await controller.emit(event)

    del event

    await asyncio.sleep(max_wait_before_commit + 1)

    try:
        offsets = copy.copy(platform.offsets)
        assert offsets == {("/", i): num_records_per_shard for i in range(num_shards)}
    finally:
        await controller.terminate()
    termination_result = await controller.await_termination()
    assert termination_result == 330


# ML-8799
def test_async_offset_commit_before_termination_with_concurrent_execution():
    asyncio.run(async_offset_commit_before_termination_with_concurrent_execution())


def test_offset_not_committed_prematurely():
    platform = Committer()
    context = CommitterContext(platform)

    controller = build_flow(
        [
            SyncEmitSource(context=context, explicit_ack=True),
            EventHoarder(),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    num_shards = 10
    num_records_per_shard = 10

    for offset in range(1, num_records_per_shard + 1):
        for shard in range(num_shards):
            event = Event(shard)
            event.shard_id = shard
            event.offset = offset
            controller.emit(event)

    time.sleep(1)

    try:
        offsets = copy.copy(platform.offsets)
        assert offsets == {}
    finally:
        controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == 450
    offsets = copy.copy(platform.offsets)
    assert offsets == {("/", i): num_records_per_shard for i in range(num_shards)}


async def async_offset_not_committed_prematurely_with_batch():
    """ML-11979: AsyncEmitSource must not commit offsets for events still in batch buffers."""
    platform = Committer()
    context = CommitterContext(platform)

    controller = build_flow(
        [
            AsyncEmitSource(context=context, explicit_ack=True, max_wait_before_commit=1),
            Batch(max_events=100, flush_after_seconds=120),
            Reduce(0, lambda acc, x: acc + len(x)),
        ]
    ).run()

    # Emit 5 events to shard 0 — all stay in Batch buffer (batch needs 100 to flush)
    for offset in range(1, 6):
        event = Event(offset)
        event.shard_id = 0
        event.offset = offset
        await controller.emit(event)
    del event

    # Wait for the commit loop to run (max_wait_before_commit=1s)
    await asyncio.sleep(3)

    # Events are still in the Batch buffer (not flushed).
    # Offsets must NOT be committed — they are not fully processed.
    offsets_before = copy.copy(platform.offsets)
    assert offsets_before == {}, f"Offsets committed prematurely while events in batch buffer: {offsets_before}"

    termination_result = await controller.terminate(wait=True)

    # After termination, Batch._emit_all flushes remaining events,
    # then commit_all=True fires correctly.
    assert termination_result == 5
    offsets_after = copy.copy(platform.offsets)
    assert offsets_after == {("/", 0): 5}


def test_async_offset_not_committed_prematurely_with_batch():
    asyncio.run(async_offset_not_committed_prematurely_with_batch())


async def async_offset_commit_error():
    platform = BadCommitter()
    logger = MockLogger()
    context = CommitterContext(platform, logger=logger)

    controller = build_flow(
        [
            AsyncEmitSource(context=context, explicit_ack=True, max_wait_before_commit=1),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            FlatMap(lambda x: [x, x * 10]),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    num_shards = 10
    num_records_per_shard = 10

    for offset in range(1, num_records_per_shard + 1):
        for shard in range(num_shards):
            event = Event(shard)
            event.shard_id = shard
            event.offset = offset
            await controller.emit(event)
    del event

    # Make sure that offsets are committed even before termination
    await asyncio.sleep(2)
    offsets = copy.copy(platform.offsets)

    try:
        assert offsets == {}
    finally:
        termination_result = await controller.terminate(wait=True)

    assert termination_result == 330

    log_level, (log_message,), _ = logger.logs[0]
    assert log_level == "error"
    assert "Failed to commit offsets due to error" in log_message
    assert "RuntimeError: Something went wrong" in log_message


# ML-10538
def test_async_offset_commit_error():
    asyncio.run(async_offset_commit_error())


def test_offset_commit_error():
    platform = BadCommitter()
    logger = MockLogger()
    context = CommitterContext(platform, logger=logger)

    controller = build_flow(
        [
            SyncEmitSource(context=context, explicit_ack=True, max_wait_before_commit=1),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            FlatMap(lambda x: [x, x * 10]),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    num_shards = 10
    num_records_per_shard = 10

    for offset in range(1, num_records_per_shard + 1):
        for shard in range(num_shards):
            event = Event(shard)
            event.shard_id = shard
            event.offset = offset
            controller.emit(event)
    del event

    # Make sure that offsets are committed even before termination
    time.sleep(2)
    offsets = copy.copy(platform.offsets)

    try:
        assert offsets == {}
    finally:
        termination_result = controller.terminate(wait=True)

    assert termination_result == 330

    log_level, (log_message,), _ = logger.logs[0]
    assert log_level == "error"
    assert "Failed to commit offsets due to error" in log_message
    assert "RuntimeError: Something went wrong" in log_message


async def async_offset_commit_error_on_termination():
    platform = Committer()
    logger = MockLogger()
    context = CommitterContext(platform, logger=logger)

    controller = build_flow(
        [
            AsyncEmitSource(context=context, explicit_ack=True, max_wait_before_commit=1),
            ErrorOnTermination(),
        ]
    ).run()

    num_shards = 10
    num_records_per_shard = 10

    for offset in range(1, num_records_per_shard + 1):
        for shard in range(num_shards):
            event = Event(shard)
            event.shard_id = shard
            event.offset = offset
            await controller.emit(event)
    del event

    with pytest.raises(ATestException):
        await controller.terminate(wait=True)

    assert platform.offsets == {("/", shard): 10 for shard in range(num_shards)}


# ML-11919
def test_async_offset_commit_error_on_termination():
    asyncio.run(async_offset_commit_error_on_termination())


async def async_offset_commit_with_failing_step(with_failing_recovery_step):
    # Scenario: explicit-ack stream + a step that always raises. In one variant the step also has a
    # recovery step (error handler) that ALSO always raises. Emit 5 events on a single shard. Even
    # though every event fails, each one should still be committed so the stream keeps advancing and
    # the source is not permanently poisoned (currently only the first failing event is committed,
    # then the flow is stuck). The outcome is the same with or without the failing recovery step.
    platform = Committer()
    logger = MockLogger()
    context = CommitterContext(platform, logger=logger)

    # Model the nuclio/mlrun context, which always provides an error stream. Failed events should
    # be routed here (dead-lettered) and then committed, rather than poisoning the source.
    errors = []
    context.push_error = lambda event, message, source=None: errors.append(event.offset)

    def always_raise(_):
        raise ATestException("step failed")

    def recovery_always_raises(event):
        # event.error holds the original exception that triggered recovery
        raise RuntimeError(f"error handler failed (orig={type(event.error).__name__})")

    # In mlrun every step carries the context (and thus the error stream), so pass it down here too.
    steps = [AsyncEmitSource(context=context, explicit_ack=True, max_wait_before_commit=1)]
    if with_failing_recovery_step:
        error_handler = Map(recovery_always_raises, full_event=True, context=context)
        steps.append(Map(always_raise, recovery_step=error_handler, context=context))
        steps.append(error_handler)
    else:
        steps.append(Map(always_raise, context=context))
    controller = build_flow(steps).run()

    for offset in range(1, 6):
        event = Event(offset)
        event.shard_id = 0
        event.offset = offset
        await controller.emit(event)
    del event

    await asyncio.sleep(2)

    try:
        await controller.terminate(wait=True)
    except Exception:
        pass  # flow is poisoned today; the contract we're asserting is about offsets

    # All 5 events failed, but each should be routed to the error stream and committed so the
    # stream keeps advancing (the source is no longer poisoned).
    assert platform.offsets == {("/", 0): 5}
    assert errors == [1, 2, 3, 4, 5]


@pytest.mark.parametrize("with_failing_recovery_step", [False, True])
def test_async_offset_commit_with_failing_step(with_failing_recovery_step):
    asyncio.run(async_offset_commit_with_failing_step(with_failing_recovery_step))


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


def test_multiple_upstreams_csv_source():
    source = CSVSource("tests/test.csv")
    map1 = Map(lambda x: append_and_return(x, "map1"))
    map2 = Map(lambda x: append_and_return(x, "map2"))
    reduce = Reduce([], append_and_return)
    source.to(map1)
    source.to(map2)
    map1.to(reduce)
    map2.to(reduce)
    controller = source.run()

    termination_result = controller.await_termination()
    assert termination_result == [[1, 2, 3, "map1"], [1, 2, 3, "map2"], [4, 5, 6, "map1"], [4, 5, 6, "map2"]]


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


def batch_append_and_return(lst, x):
    extract_event_bodies = [sub_event.body for sub_event in x]
    lst.append(extract_event_bodies)
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
    assert "Encountered null values in the following key fields: k" in logger.logs[0][1][0]


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


@pytest.mark.parametrize("key_field ,key1, key2", [("my_key", "key1", "key2"), (["my_key"], ["key1"], ["key2"])])
def test_dataframe_source_with_metadata(key_field, key1, key2):
    t1 = datetime(2020, 2, 15)
    t2 = datetime(2020, 2, 16)
    df = pd.DataFrame(
        [["key1", t1, "id1", 1.1], ["key2", t2, "id2", 2.2]],
        columns=["my_key", "my_time", "my_id", "my_value"],
    )
    controller = build_flow(
        [
            DataframeSource(df, key_field=key_field, time_field="my_time", id_field="my_id"),
            Reduce([], append_and_return, full_event=True),
        ]
    ).run()

    termination_result = controller.await_termination()
    expected = [
        Event(
            {"my_key": "key1", "my_time": t1, "my_id": "id1", "my_value": 1.1},
            key=key1,
            id="id1",
        ),
        Event(
            {"my_key": "key2", "my_time": t2, "my_id": "id2", "my_value": 2.2},
            key=key2,
            id="id2",
        ),
    ]
    assert termination_result == expected
    assert list(map(lambda event: event.key, termination_result)) == [key1, key2]


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


def test_parquet_flush_with_inconsistent_schema_logs_error(tmpdir):
    out_dir = f"{tmpdir}/test_parquet_flush_with_inconsistent_schema_logs_error/{uuid.uuid4().hex}/"

    logger = MockLogger()
    context = MockContext(logger, False)

    columns = [("my_int_or_string", "int"), ("my_string", "str")]
    target = ParquetTarget(
        out_dir,
        columns=columns,
        partition_cols=[],
        flush_after_seconds=0.5,
        context=context,
    )
    controller = build_flow([SyncEmitSource(), target]).run()
    controller.emit(["it is actually a string", "abc"])
    time.sleep(1)

    assert logger.logs[0][1][0].startswith("Failed to flush batch in step 'ParquetTarget':")

    controller.terminate()
    controller.await_termination()


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
    class MyChoice(Choice):
        def select_outlets(self, event):
            outlets = ["all_events"]
            if event > 5:
                outlets.append("more_than_five")
            else:
                outlets.append("up_to_five")
            return outlets

    source = SyncEmitSource()
    my_choice = MyChoice(termination_result_fn=lambda x, y: x + y)
    all_events = Map(lambda x: x, name="all_events")
    more_than_five = Map(lambda x: x * 10, name="more_than_five")
    up_to_five = Map(lambda x: x * 100, name="up_to_five")
    sum_up_all_events = Reduce(0, lambda acc, x: acc + x)
    sum_up_more_than_five = Reduce(0, lambda acc, x: acc + x)
    sum_up_up_to_five = Reduce(0, lambda acc, x: acc + x)

    source.to(my_choice)
    my_choice.to(all_events)
    my_choice.to(more_than_five)
    my_choice.to(up_to_five)
    all_events.to(sum_up_all_events)
    more_than_five.to(sum_up_more_than_five)
    up_to_five.to(sum_up_up_to_five)

    controller = source.run()

    for i in range(4, 8):
        controller.emit(i)

    controller.terminate()
    termination_result = controller.await_termination()

    expected = sum(range(4, 8)) + sum(range(6, 8)) * 10 + sum(range(4, 6)) * 100
    assert termination_result == expected


def test_duplicate_choice():
    class DuplicateChoice(Choice):
        def select_outlets(self, event):
            outlets = ["all_events", "all_events"]
            return outlets

    source = SyncEmitSource()
    duplicate_choice = DuplicateChoice(termination_result_fn=lambda x, y: x + y)
    all_events = Map(lambda x: x, name="all_events")

    source.to(duplicate_choice).to(all_events)

    controller = source.run()
    controller.emit(0)
    controller.terminate()
    with pytest.raises(
        ValueError,
        match=r"Invalid outlet selection for 'DuplicateChoice': duplicate outlet names were provided "
        r"\(all_events, all_events\)\.",
    ):
        controller.await_termination()


def test_nonexistent_choice():
    class NonexistentChoice(Choice):
        def select_outlets(self, event):
            outlets = ["wrong"]
            return outlets

    source = SyncEmitSource()
    nonexistent_choice = NonexistentChoice(termination_result_fn=lambda x, y: x + y)
    all_events = Map(lambda x: x, name="all_events")

    source.to(nonexistent_choice).to(all_events)

    controller = source.run()
    controller.emit(0)
    controller.terminate()
    with pytest.raises(
        ValueError,
        match=r"Invalid outlet 'wrong' for 'NonexistentChoice'. Allowed outlets are: all_events.",
    ):
        controller.await_termination()


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
            Batch(4, 100, full_event=False),
            Reduce([], lambda acc, x: append_and_return(acc, x), full_event=True),
        ]
    ).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert len(termination_result) == 3
    assert termination_result[0].id
    assert termination_result[0].body == [0, 1, 2, 3]
    assert termination_result[1].id
    assert termination_result[1].body == [4, 5, 6, 7]
    assert termination_result[2].id
    assert termination_result[2].body == [8, 9]


@pytest.mark.parametrize("full_event", [True, None])
def test_batch_full_event(full_event):
    def append_body_and_return(lst, x):
        ll = []
        for item in x:
            ll.append(item.body)
        lst.append(ll)
        return lst

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(4, 100, full_event=full_event),
            Reduce([], lambda acc, x: append_body_and_return(acc, x)),
        ]
    ).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]


@pytest.mark.parametrize("full_event", [True, False, None])
def test_batch_by_user_key(full_event):
    batch_kwargs = {"full_event": full_event} if full_event is not None else {}
    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(2, 100, "value", **batch_kwargs),
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
        if full_event in (True, None):
            assert len(element) == 2
            previous_number = None
            for sub_event in element:
                assert isinstance(sub_event, Event)
                if previous_number is None:
                    previous_number = sub_event.body["value"]
                else:
                    assert sub_event.body["value"] == previous_number
        else:
            numbers = [e["value"] for e in element]
            assert numbers[0] == numbers[1]


@pytest.mark.parametrize(
    "full_event, reduce_fn",
    [
        (False, append_and_return),
        (True, batch_append_and_return),
        (None, batch_append_and_return),
    ],
)
def test_batch_by_event_key(full_event, reduce_fn):
    batch_kwargs = {"full_event": full_event} if full_event is not None else {}

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(5, 100, "$key", **batch_kwargs),
            Reduce([], lambda acc, x: reduce_fn(acc, x)),
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


@pytest.mark.parametrize(
    "full_event, reduce_fn",
    [
        (False, append_and_return),
        (True, batch_append_and_return),
        (None, batch_append_and_return),
    ],
)
def test_batch_by_field_value_key_extractor(full_event, reduce_fn):
    batch_kwargs = {"full_event": full_event} if full_event is not None else {}

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(3, 100, "field", **batch_kwargs),
            Reduce([], lambda acc, x: reduce_fn(acc, x)),
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


@pytest.mark.parametrize(
    "full_event, reduce_fn",
    [
        (False, append_and_return),
        (True, batch_append_and_return),
        (None, batch_append_and_return),
    ],
)
def test_batch_by_function_key_extractor(full_event, reduce_fn):
    batch_kwargs = {"full_event": full_event} if full_event is not None else {}

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(10, 100, lambda event: event.body % 3 == 0, **batch_kwargs),
            Reduce([], lambda acc, x: reduce_fn(acc, x)),
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


@pytest.mark.parametrize(
    "full_event",
    (False, True),
)
def test_batch_grouping_with_timeout(full_event):
    q = queue.Queue(1)

    def reduce_fn(acc, event):
        if event == [1]:
            q.put(None)
        acc.append(event)
        return acc

    def reduce_fn_batch(acc, event):
        if len(event) == 1 and event[0].body == 1:
            q.put(None)
        acc.append([sub_event.body for sub_event in event])
        return acc

    reduce_function = reduce_fn_batch if full_event else reduce_fn

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(max_events=3, flush_after_seconds=1, key_field="$key", full_event=full_event),
            Reduce([], lambda acc, x: reduce_function(acc, x)),
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


@pytest.mark.parametrize(
    "full_event",
    (False, True),
)
def test_batch_with_timeout(full_event):
    q = queue.Queue(1)

    def reduce_fn(acc, x):
        if x[0] == 0:
            q.put(None)
        acc.append(x)
        return acc

    def reduce_fn_batch(acc, x):
        if x[0].body == 0:
            q.put(None)
        events_body_list = [event.body for event in x]
        acc.append(events_body_list)
        return acc

    reduce_function = reduce_fn_batch if full_event else reduce_fn

    controller = build_flow(
        [
            SyncEmitSource(),
            Batch(4, 1, full_event=full_event),
            Reduce([], reduce_function),
        ]
    ).run()

    for i in range(10):
        if i == 3:
            q.get()
        controller.emit(Event(i, processing_time=datetime(2020, 2, 15, 2, 0)))
    controller.terminate()
    termination_result = controller.await_termination()
    assert termination_result == [[0, 1, 2], [3, 4, 5, 6], [7, 8, 9]]


def test_batch_warns_when_full_event_not_specified():
    with pytest.warns(Warning, match="The default value of full_event in Batch changed to True"):
        Batch(4, 100)


@pytest.mark.parametrize(
    "flush_after_seconds, expected_exc, expected_msg",
    [
        (None, ValueError, "At least one of flush_after_seconds or max_events must be provided"),
        (0, ValueError, "At least one of flush_after_seconds or max_events must be provided"),
        ("10", TypeError, "flush_after_seconds must be a number"),
        ([10], TypeError, "flush_after_seconds must be a number"),
        (-1, ValueError, "flush_after_seconds cannot be negative"),
    ],
)
def test_batch_invalid_flush_after_seconds(flush_after_seconds, expected_exc, expected_msg):
    with pytest.raises(expected_exc, match=expected_msg):
        build_flow(
            [
                SyncEmitSource(),
                Batch(flush_after_seconds=flush_after_seconds, full_event=True),
            ]
        )


def test_batch_neither_raises():
    """Neither flush_after_seconds nor max_events should raise"""
    with pytest.raises(ValueError, match="At least one of flush_after_seconds or max_events must be provided"):
        build_flow(
            [
                SyncEmitSource(),
                Batch(full_event=True),
            ]
        )


@pytest.mark.parametrize(
    "max_events, expected_exc, expected_msg",
    [
        ("5", TypeError, "max_events must be an integer"),
        (5.1, TypeError, "max_events must be an integer"),
        (-1, ValueError, "max_events must be a positive integer"),
    ],
)
def test_batch_invalid_max_events(max_events, expected_exc, expected_msg):
    with pytest.raises(expected_exc, match=expected_msg):
        build_flow(
            [
                SyncEmitSource(),
                Batch(max_events=max_events, flush_after_seconds=10, full_event=True),
            ]
        )


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


# ML-5299
def test_write_csv_with_zero_records(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_with_zero_records.csv"
    controller = build_flow([SyncEmitSource(), CSVTarget(file_path, columns=["n", "n*10"], header=True)]).run()

    controller.terminate()
    controller.await_termination()

    assert not os.path.isfile(file_path)


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


def test_append_csv(tmpdir):
    file_path = f"{tmpdir}/test_append_csv.csv"

    flow = build_flow([SyncEmitSource(), CSVTarget(file_path, columns=["n", "n*10"], header=True)])

    for _ in range(2):
        controller = flow.run()
        for i in range(3):
            controller.emit({"n": i, "n*10": 10 * i})
        controller.terminate()
        controller.await_termination()

    with open(file_path) as file:
        result = file.read()

    expected = "n,n*10\n0,0\n1,10\n2,20\n0,0\n1,10\n2,20\n"
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


# ML-5298
def test_write_csv_infer_columns_after_flow_restart(tmpdir):
    file_path = f"{tmpdir}/test_write_csv_infer_columns_after_flow_restart.csv"
    flow = build_flow([SyncEmitSource(), CSVTarget(file_path, header=True)])

    for r in [range(3), range(3, 6), range(6, 10)]:
        controller = flow.run()
        for i in r:
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


@pytest.mark.parametrize(
    "full_event",
    (False, True),
)
def test_to_dataframe_with_index(full_event):
    # Note: This test validates to_dataframe() and batching in isolation
    # Event IDs are not preserved, so this specific pattern won't work on remote serving function

    def extract_batch_bodies(event):
        event_bodies = [sub_event.body for sub_event in event.body]
        event.body = event_bodies
        return event

    index = "my_int"
    map_step = [Map(fn=extract_batch_bodies, full_event=True)] if full_event else []

    steps = [
        SyncEmitSource(),
        Batch(5, full_event=full_event),
        *map_step,
        ToDataFrame(index=index),
        Reduce([], append_and_return, full_event=True),
    ]
    controller = build_flow(steps).run()

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
    expected_df = pd.DataFrame(expected, columns=columns)
    expected_df["my_int"] = expected_df["my_int"].astype("category")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    pd.testing.assert_frame_equal(read_back_df, expected_df, check_categorical=False)


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


@pytest.mark.parametrize("max_events", [1, 5])
def test_write_to_single_partition_parquet(tmpdir, max_events):
    out_dir = f"{tmpdir}/test_write_to_parquet_partition_by_datetime/"

    def check_target_parquets(ids):
        expected_files = [os.path.join(out_dir, f"id={i}", "target.parquet") for i in ids]
        missing = [f for f in expected_files if not os.path.exists(f)]
        assert not missing, f"Missing expected parquet files: {missing}"

    columns = ["my_int", "my_string", "id"]
    flow = build_flow(
        [
            SyncEmitSource(),
            ParquetTarget(out_dir, partition_cols="id", columns=columns, max_events=max_events, single_file=True),
        ]
    )

    controller = flow.run()
    expected = []
    for i in range(10):
        controller.emit([i, f"this is {i}", i % 3])
        if max_events == 5:
            expected.append([i, f"this is {i}", i % 3])
        elif max_events == 1 and i >= 7:
            expected.append([i, f"this is {i}", i % 3])
    expected_df = pd.DataFrame(expected, columns=columns)
    expected_df["id"] = expected_df["id"].astype("int32").astype("category")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    read_back_df.sort_values("my_int", inplace=True)
    read_back_df.reset_index(drop=True, inplace=True)
    pd.testing.assert_frame_equal(read_back_df, expected_df)
    check_target_parquets(ids=(0, 1, 2))

    controller = flow.run()
    expected = []
    for i in range(10, 20):
        controller.emit([i, f"this is {i}", i % 3])
        if max_events == 5:
            expected.append([i, f"this is {i}", i % 3])
        elif max_events == 1 and i >= 17:
            expected.append([i, f"this is {i}", i % 3])
    expected_df = pd.DataFrame(expected, columns=columns)
    expected_df["id"] = expected_df["id"].astype("int32").astype("category")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    read_back_df.sort_values("my_int", inplace=True)
    read_back_df.reset_index(drop=True, inplace=True)
    pd.testing.assert_frame_equal(read_back_df, expected_df)
    check_target_parquets(ids=(0, 1, 2))


def test_write_to_parquet_string_as_datetime(tmpdir):
    out_dir = f"{tmpdir}/test_write_to_parquet_string_to_datetime/{uuid.uuid4().hex}/"
    columns = ["my_int", "my_string", "my_datetime"]
    columns_with_type = [
        ("my_int", "int8"),  # ML-4162
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
    expected_df["my_int"] = expected_df["my_int"].astype("int8")
    expected_df["my_datetime"] = expected_df["my_datetime"].astype("datetime64[us]")
    controller.terminate()
    controller.await_termination()

    read_back_df = pd.read_parquet(out_dir, columns=columns)
    read_back_df.sort_values("my_int", inplace=True)
    read_back_df.reset_index(drop=True, inplace=True)
    pd.testing.assert_frame_equal(read_back_df, expected_df)


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
    # ML-5119 – make sure max_events and flush_after_seconds are ignored
    controller = build_flow(
        [SyncEmitSource(), ParquetTarget(out_file, columns=columns, max_events=1, flush_after_seconds=1)]
    ).run()

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
    # with the introduction of s, ms, us time resolutions in pandas-2.0, the dtype of the parquet data
    # is set to datetime64[us], while default DataFrame dtype is datetime64[ns]
    assert_frame_equal(expected, read_back_df, check_dtype=version.parse(pd.__version__) < version.parse("2.0.0"))


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
    # with the introduction of s, ms, us time resolutions in pandas-2.0, the dtype of the parquet data
    # is set to datetime64[us], while default DataFrame dtype is datetime64[ns]
    assert_frame_equal(expected, read_back_df, check_dtype=version.parse(pd.__version__) < version.parse("2.0.0"))


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
    table._update_static_attrs("9", {"age": 1, "color": "blue9"})
    table._update_static_attrs("7", {"age": 3, "color": "blue7"})

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
    table._update_static_attrs("1", {"age": 1, "color": "blue"})
    table._update_static_attrs("3", {"age": 3, "color": "red"})

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
    table._update_static_attrs("9", {"age": 1, "color": "blue9"})
    table._update_static_attrs("7", {"age": 3, "color": "blue7"})

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
    table._update_static_attrs("9", {"age": 1, "color": "blue9"})
    table._update_static_attrs("7", {"age": 3, "color": "blue7"})

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
    table._update_static_attrs("2", {"age": 2, "color": "blue"})
    table._update_static_attrs("3", {"age": 3, "color": "red"})

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

    # with the introduction of s, ms, us time resolutions in pandas-2.0, the dtype of the parquet data
    # is set to datetime64[us], while default DataFrame dtype is datetime64[ns]
    assert_frame_equal(expected, read_back_df, check_dtype=version.parse(pd.__version__) < version.parse("2.0.0"))


def test_csv_reader_parquet_write_nanosecs_truncation(tmpdir):
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

    # with the introduction of s, ms, us time resolutions in pandas-2.0, the dtype of the parquet data
    # is set to datetime64[us], while default DataFrame dtype is datetime64[ns]
    assert_frame_equal(expected, read_back_df, check_dtype=version.parse(pd.__version__) < version.parse("2.0.0"))


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


# ML-5442
def test_key_field_and_non_dict_event_body():
    controller = build_flow(
        [
            SyncEmitSource(key_field="my_key_field"),
            Reduce([], append_and_return, full_event=True),
        ]
    ).run()

    body1 = b"a"
    body2 = b"b"

    controller.emit(body1)
    controller.emit(Event(body2, "my_key"))

    controller.terminate()
    result = controller.await_termination()

    assert len(result) == 2

    result1 = result[0]
    assert result1.body == body1
    assert result1.key is None

    result2 = result[1]
    assert result2.key == "my_key"


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
batch0 = Batch(full_event=True, max_events=5)
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
batch0 = Batch(full_event=True, max_events=5)
reduce0 = Reduce(initial_value=[])
batch1 = Batch(full_event=True, max_events=5)
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
parquet_target0 = ParquetTarget(path='mypq')

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
            CSVSource("tests/test-with-none-values.csv", key_field="string", parse_dates="date_with_none"),
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
        pd.to_datetime("2021-04-21 15:56:53.385444"),
    ]
    assert termination_result[1].key == "b"
    excepted_result = ["b", True, math.nan, math.nan, math.nan, pd.NaT]
    assert len(termination_result[1].body) == len(excepted_result)
    for x, y in zip(termination_result[1].body, excepted_result):
        if isinstance(x, float):
            assert isinstance(y, float)
            if math.isnan(x):
                assert math.isnan(y)
            else:
                assert x == y
        elif isinstance(x, type(pd.NaT)):
            assert isinstance(y, type(pd.NaT))
        else:
            assert x == y


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
            ReifyMetadata({"key": "id"}),
            Reduce([], append_and_return, full_event=False),
        ]
    ).run()

    termination_result = controller.await_termination()

    assert termination_result == [
        {
            "b": True,
            "id": "m1",
            "k": "m1",
            "t": datetime(2020, 2, 15, 2, 0),
            "v": 8,
        },
        {
            "b": False,
            "id": "m2",
            "k": "m2",
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
    assert str(event) == "Event(id=None, key='key', body='body')"


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

    debug_logs = [log for log in logger.logs if log[0] == "debug"]

    assert len(debug_logs) == 2

    level, args, kwargs = debug_logs[0]
    assert level == "debug"
    assert args == ("SyncEmitSource -> Map1 | Event(id=myid, path=/, body={})",)
    assert kwargs == {}

    level, args, kwargs = debug_logs[1]
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
    df.to_parquet(path, coerce_timestamps="us")
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
    df.to_parquet(path, coerce_timestamps="us")
    start = pd.Timestamp("2022-07-01 00:00:00")
    end = pd.Timestamp("2022-12-31 14:00:00")

    controller = build_flow(
        [
            ParquetSource(path, start_filter=start, end_filter=end, filter_column="my_time"),
            ReduceToDataFrame(index="my_string", insert_key_column_as="my_string"),
        ]
    ).run()

    read_back_result = controller.await_termination()

    try:
        pd.testing.assert_frame_equal(read_back_result, pd.DataFrame({}))
    finally:
        os.remove(path)


@pytest.mark.parametrize("include_datetime_filter", [True, False])
def test_filter_by_filters(include_datetime_filter):
    columns = ["my_string", "my_time", "my_city"]
    tel_aviv_data = [
        ["dina", pd.Timestamp("2019-07-01 00:00:00"), "tel aviv"],
        ["uri", pd.Timestamp("2018-12-30 09:00:00"), "tel aviv"],
    ]
    df = pd.DataFrame(
        [
            *tel_aviv_data,
            ["katya", pd.Timestamp("2020-12-31 14:00:00"), "hod hasharon"],
        ],
        columns=columns,
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        df.to_parquet(temp_dir, partition_cols=["my_city"])
        source_kwargs = {"additional_filters": [("my_city", "=", "tel aviv")]}
        expected_df = pd.DataFrame(tel_aviv_data, columns=columns)
        if include_datetime_filter:
            source_kwargs["start_filter"] = pd.Timestamp("2019-01-01 00:00:00")
            source_kwargs["end_filter"] = pd.Timestamp("2021-01-01 00:00:00")
            source_kwargs["filter_column"] = "my_time"
            expected_df = pd.DataFrame([tel_aviv_data[0]], columns=columns)
        expected_df.set_index("my_string", inplace=True)

        controller = build_flow([ParquetSource(temp_dir, **source_kwargs), ReduceToDataFrame(index="my_string")]).run()
        read_back_result = controller.await_termination()
        pd.testing.assert_frame_equal(read_back_result, expected_df)


def test_filters_type():
    with pytest.raises(ValueError, match="ParquetSource supports additional_filters only as a list of tuples."):
        ParquetSource(
            "/my_dir",
            additional_filters=[[("city", "=", "Tel Aviv")], [("age", ">=", "40")]],
            filter_column="start_time",
        )


class RunnableBusyWait(ParallelExecutionRunnable):
    def init(self):
        self._result = 1

    def run(self, data, path, origin_name=None):
        start = time.monotonic()
        while time.monotonic() - start < 1:
            pass
        return self._result


class RunnableSleep(ParallelExecutionRunnable):
    _result = 0

    def init(self):
        self._result = 1

    def run(self, data, path, origin_name=None):
        time.sleep(1)
        return self._result


class RunnableAsyncSleep(ParallelExecutionRunnable):
    _result = 0

    def init(self):
        self._result = 1

    async def run_async(self, data, path, origin_name=None):
        await asyncio.sleep(1)
        return self._result


class RunnableNaiveNoOp(ParallelExecutionRunnable):
    _result = 0

    def init(self):
        self._result = 1

    def run(self, data, path, origin_name=None):
        return self._result


class RunnableWithError(ParallelExecutionRunnable):
    def run(self, data, path, origin_name=None):
        raise Exception("This shouldn't run!")


def test_parallel_execution_runnable_uniqueness():
    runnables = [
        RunnableBusyWait("x"),
        RunnableBusyWait("x"),
    ]
    parallel_execution = ParallelExecution(runnables, execution_mechanism_by_runnable_name={"x": "process_pool"})
    with pytest.raises(ValueError, match="ParallelExecutionRunnable name 'x' is not unique"):
        parallel_execution._init()


# ML-11128
@pytest.mark.parametrize("execution_mechanism", ["process_pool", "dedicated_process"])
def test_parallel_execution_spawn(execution_mechanism):
    runnables = [
        RunnableBusyWait("x"),
    ]
    parallel_execution = ParallelExecution(runnables, execution_mechanism_by_runnable_name={"x": execution_mechanism})
    parallel_execution._init()
    mp_context = parallel_execution.runnable_executor._mp_context
    assert isinstance(mp_context, multiprocessing.context.SpawnContext)


def test_select_runnable_uniqueness():
    runnables = [
        RunnableNaiveNoOp("x"),
        RunnableNaiveNoOp("y"),
    ]

    class MyParallelExecution(ParallelExecution):
        def select_runnables(self, event):
            return ["x", "x"]

    parallel_execution = MyParallelExecution(
        runnables,
        execution_mechanism_by_runnable_name={"x": "naive", "y": "naive"},
    )

    source = SyncEmitSource()
    source.to(parallel_execution)

    controller = source.run()
    controller.emit(0)
    controller.terminate()
    with pytest.raises(ValueError, match=r"select_runnables\(\) returned more than one outlet named 'x'"):
        controller.await_termination()


def test_select_runnable_not_exist():
    runnables = [
        RunnableNaiveNoOp("x"),
        RunnableNaiveNoOp("y"),
    ]

    class MyParallelExecution(ParallelExecution):
        def select_runnables(self, event):
            return ["x", "z"]

    parallel_execution = MyParallelExecution(
        runnables,
        execution_mechanism_by_runnable_name={"x": "naive", "y": "naive"},
    )

    source = SyncEmitSource()
    source.to(parallel_execution)

    controller = source.run()
    controller.emit(0)
    controller.terminate()
    with pytest.raises(ValueError, match="The following selected Runnables are not registered: z"):
        controller.await_termination()


def test_select_runnable_wrong_type():
    runnables = [
        RunnableNaiveNoOp("x"),
        RunnableNaiveNoOp("y"),
    ]

    class MyParallelExecution(ParallelExecution):
        def select_runnables(self, event):
            return ["x", 6.0]

    parallel_execution = MyParallelExecution(
        runnables,
        execution_mechanism_by_runnable_name={"x": "naive", "y": "naive"},
    )

    source = SyncEmitSource()
    source.to(parallel_execution)

    controller = source.run()
    controller.emit(0)
    controller.terminate()
    with pytest.raises(TypeError, match="Expected a ParallelExecutionRunnable or str, but got: float"):
        controller.await_termination()


def test_parallel_execution():
    busy_wait_pool = RunnableBusyWait("busy1")
    busy_wait_dedicated = RunnableBusyWait("busy2")

    runnables = [
        RunnableWithError("error"),
        busy_wait_pool,
        busy_wait_dedicated,
        RunnableSleep("sleep1"),
        RunnableSleep("sleep2"),
        RunnableAsyncSleep("asleep1"),
        RunnableAsyncSleep("asleep2"),
        RunnableNaiveNoOp("naive"),
    ]

    class MyParallelExecution(ParallelExecution):
        def select_runnables(self, event):
            return [runnable.name for runnable in runnables if runnable.name != "error"]

    parallel_execution = MyParallelExecution(
        runnables,
        execution_mechanism_by_runnable_name={
            "error": "naive",
            "busy1": "process_pool",
            "busy2": "dedicated_process",
            "sleep1": "thread_pool",
            "sleep2": "thread_pool",
            "asleep1": "asyncio",
            "asleep2": "asyncio",
            "naive": "naive",
        },
    )
    reduce = Reduce([], lambda acc, x: acc + [x])

    source = SyncEmitSource()
    source.to(parallel_execution).to(reduce)

    start = time.monotonic()
    controller = source.run()
    controller.emit(0)
    controller.terminate()
    termination_result = controller.await_termination()
    end = time.monotonic()

    assert end - start < 6
    termination_result = termination_result[0]
    assert termination_result == {
        "asleep1": 1,
        "asleep2": 1,
        "busy1": 1,
        "busy2": 1,
        "naive": 1,
        "sleep1": 1,
        "sleep2": 1,
    }


def test_invalid_execution_mechanism():
    with pytest.raises(
        ValueError,
        match="Execution mechanism 'nonexistent execution mechanism' is invalid. It must be one of:",
    ):
        runnables = [ParallelExecutionRunnable("my_runnable")]
        ParallelExecution(
            runnables,
            execution_mechanism_by_runnable_name={"my_runnable": "nonexistent execution mechanism"},
        )


class RunnableMultiprocessingWithLargeData(ParallelExecutionRunnable):
    def __init__(self, data_size: int, gpu_number: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = None
        self.data_size = data_size
        self.gpu_number = gpu_number

    def init(self):
        self.data = list(range(self.data_size))

    def run(self, data, path, origin_name=None):
        data["data_size"] = len(self.data)
        data["gpu"] = self.gpu_number
        return data


def test_parallel_execution_with_large_data():
    data_size = 1_000_000
    num_records = 100
    num_runnables = 3

    runnables = [
        RunnableMultiprocessingWithLargeData(data_size, gpu_number=i, name=f"runnable_{i}")
        for i in range(num_runnables)
    ]
    reduce = Reduce([], lambda acc, x: acc + [x])

    source = SyncEmitSource()
    source.to(
        ParallelExecution(
            runnables,
            execution_mechanism_by_runnable_name={runnable.name: "dedicated_process" for runnable in runnables},
            max_processes=1,
        )
    ).to(reduce)

    controller = source.run()

    for n in range(num_records):
        controller.emit({"n": n})
    termination_result = controller.terminate(wait=True)

    assert len(termination_result) == num_records
    for n, result in enumerate(termination_result):
        if num_runnables == 1:
            assert result == {"data_size": data_size, "n": n, "gpu": 0}
        else:
            for expected_gpu, runnable_result in enumerate(result.values()):
                assert runnable_result == {"data_size": data_size, "n": n, "gpu": expected_gpu}


class RunnableShared(ParallelExecutionRunnable):
    pass


def test_parallel_execution_with_shared():
    busy_wait_pool = RunnableBusyWait("busy1")
    busy_wait_dedicated = RunnableBusyWait("busy2")

    runnables = [
        RunnableShared("busy2", shared_runnable_name="busy2"),
        RunnableShared("busy3", shared_runnable_name="busy2"),
        busy_wait_pool,
        RunnableShared("thread1", shared_runnable_name="thread1"),
    ]

    class MyParallelExecution(ParallelExecution):
        def select_runnables(self, event):
            return None

    class MyContext:
        def __init__(self, executor: RunnableExecutor):
            self.executor = executor

    my_executor = RunnableExecutor()
    my_executor.add_runnable(busy_wait_dedicated, "dedicated_process")
    my_executor.add_runnable(
        RunnableSleep(
            "thread1",
        ),
        "thread_pool",
    )
    my_context = MyContext(executor=my_executor)

    parallel_execution = MyParallelExecution(
        runnables,
        execution_mechanism_by_runnable_name={
            "busy1": "process_pool",
            "busy2": "shared_executor",
            "busy3": "shared_executor",
            "thread1": "shared_executor",
        },
        context=my_context,
    )
    reduce = Reduce([], lambda acc, x: acc + [x])

    source = SyncEmitSource()
    source.to(parallel_execution).to(reduce)

    start = time.monotonic()
    controller = source.run()
    controller.emit(0)
    controller.terminate()
    termination_result = controller.await_termination()
    end = time.monotonic()

    assert end - start < 4
    termination_result = termination_result[0]
    assert termination_result == {
        "busy1": 1,
        "busy2": 1,
        "busy3": 1,
        "thread1": 1,
    }


def test_parallel_execution_with_shared_with_selector():
    busy_wait_pool = RunnableBusyWait("busy1")
    busy_wait_dedicated = RunnableBusyWait("busy2")

    runnables = [
        RunnableShared("busy2", shared_runnable_name="busy2"),
        RunnableShared("busy3", shared_runnable_name="busy2"),
        busy_wait_pool,
        RunnableShared("thread1", shared_runnable_name="thread1"),
    ]

    class MyParallelExecution(ParallelExecution):
        def select_runnables(self, event):
            return ["busy1", "busy2", "busy3", "thread1"]

    class MyContext:
        def __init__(self, executor: RunnableExecutor):
            self.executor = executor

    my_executor = RunnableExecutor()
    my_executor.add_runnable(busy_wait_dedicated, "dedicated_process")
    my_executor.add_runnable(
        RunnableSleep(
            "thread1",
        ),
        "thread_pool",
    )
    my_context = MyContext(executor=my_executor)

    parallel_execution = MyParallelExecution(
        runnables,
        execution_mechanism_by_runnable_name={
            "busy1": "process_pool",
            "busy2": "shared_executor",
            "busy3": "shared_executor",
            "thread1": "shared_executor",
        },
        context=my_context,
    )
    reduce = Reduce([], lambda acc, x: acc + [x])

    source = SyncEmitSource()
    source.to(parallel_execution).to(reduce)

    start = time.monotonic()
    controller = source.run()
    controller.emit(0)
    controller.terminate()
    termination_result = controller.await_termination()
    end = time.monotonic()

    assert end - start < 4
    termination_result = termination_result[0]
    assert termination_result == {
        "busy1": 1,
        "busy2": 1,
        "busy3": 1,
        "thread1": 1,
    }


def test_parallel_execution_single_selection_from_multiple_runnables():
    """When multiple runnables are registered but only one is selected,
    results should still be wrapped with runnable names (dict format).

    This ensures backward compatibility - the wrapping behavior depends on the number
    of *registered* runnables, not the number of *selected* runnables.
    """
    runnable1 = RunnableNaiveNoOp("model1")
    runnable2 = RunnableNaiveNoOp("model2")

    runnables = [runnable1, runnable2]

    class SelectiveParallelExecution(ParallelExecution):
        def select_runnables(self, event):
            # Select only one runnable based on event body
            selected = event.body.get("select")
            return [selected] if selected else None

    parallel_execution = SelectiveParallelExecution(
        runnables,
        execution_mechanism_by_runnable_name={
            "model1": "naive",
            "model2": "naive",
        },
    )
    reduce = Reduce([], lambda acc, x: acc + [x])

    source = SyncEmitSource()
    source.to(parallel_execution).to(reduce)

    controller = source.run()
    # Select only model2 for this event
    controller.emit({"select": "model2", "value": 42})
    controller.terminate()
    termination_result = controller.await_termination()

    # Result should be wrapped with runnable name even though only one was selected
    # (because multiple runnables are *registered*)
    # RunnableNaiveNoOp returns 1, so we expect {"model2": 1}
    result = termination_result[0]
    assert "model2" in result, f"Expected result wrapped with 'model2' key, got: {result}"
    assert result == {"model2": 1}


def test_parallel_execution_empty_selection():
    """When 1 runnable is registered but 0 are selected, event should not be emitted."""
    runnable = RunnableNaiveNoOp("model1")

    class EmptySelectParallelExecution(ParallelExecution):
        def select_runnables(self, event):
            # Return empty list - select no runnables
            return []

    parallel_execution = EmptySelectParallelExecution(
        [runnable],
        execution_mechanism_by_runnable_name={"model1": "naive"},
    )

    controller = build_flow(
        [
            SyncEmitSource(),
            parallel_execution,
            Reduce([], lambda acc, x: acc + [x]),
        ]
    ).run()
    controller.emit({"value": 42})
    controller.terminate()
    termination_result = controller.await_termination()

    # When no runnables are selected, event should not be emitted downstream
    assert termination_result == []


def test_enrichment():
    busy_wait_pool = RunnableBusyWait("busy1")
    busy_wait_dedicated = RunnableBusyWait("busy2")

    runnables = [
        busy_wait_pool,
        busy_wait_dedicated,
    ]

    class MyParallelExecution(ParallelExecution):

        def preprocess_event(self, event):
            event._metadata = {"name": self.name}
            return event

    parallel_execution = MyParallelExecution(
        runnables,
        execution_mechanism_by_runnable_name={"busy1": "process_pool", "busy2": "dedicated_process"},
    )
    reduce = Reduce([], lambda acc, x: acc + [x], full_event=True)

    source = SyncEmitSource()
    source.to(parallel_execution).to(reduce)

    start = time.monotonic()
    controller = source.run()
    controller.emit(0)
    controller.terminate()
    termination_result = controller.await_termination()
    end = time.monotonic()

    assert end - start < 3
    result = termination_result[0].body
    total_metadata = termination_result[0]._metadata

    assert result == {
        "busy1": 1,
        "busy2": 1,
    }
    assert (
        "name" in total_metadata and total_metadata.pop("name") == "MyParallelExecution"
    ), "Expected name in _metadata field"
    assert all(
        list(("when" in metadata and "microsec" in metadata) for metadata in total_metadata.values())
    ), "Expected _metadata to include 'when' and 'microsec' fields "


def test_metadata_without_enrichment():
    busy_wait_pool = RunnableBusyWait("busy1")
    busy_wait_dedicated = RunnableBusyWait("busy2")

    runnables = [
        busy_wait_pool,
        busy_wait_dedicated,
    ]

    parallel_execution = ParallelExecution(
        runnables,
        execution_mechanism_by_runnable_name={"busy1": "process_pool", "busy2": "dedicated_process"},
    )
    reduce = Reduce([], lambda acc, x: acc + [x], full_event=True)

    source = SyncEmitSource()
    source.to(parallel_execution).to(reduce)

    start = time.monotonic()
    controller = source.run()
    controller.emit(0)
    controller.terminate()
    termination_result = controller.await_termination()
    end = time.monotonic()

    assert end - start < 3
    result = termination_result[0].body
    total_metadata = termination_result[0]._metadata

    assert result == {
        "busy1": 1,
        "busy2": 1,
    }
    assert all(
        list(("when" in metadata and "microsec" in metadata) for metadata in total_metadata.values())
    ), "Expected _metadata to include 'when' and 'microsec' fields "


class ErrorRaisingBatchTarget(_Batching):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._raised_error = False
        self.sum = 0

    async def _emit(self, batch, batch_key, batch_time, batch_events, last_event_time=None):
        if not self._raised_error:
            self._raised_error = True
            raise RuntimeError("ErrorRaisingBatchTarget raises an error the first time it tries to emit")
        for number in batch:
            self.sum += number


async def async_test_error_raising_batch_target():
    target = ErrorRaisingBatchTarget(max_events=1000, flush_after_seconds=0)
    controller = build_flow(
        [
            AsyncEmitSource(),
            target,
        ]
    ).run()

    await controller.emit(1)

    await asyncio.sleep(0.1)
    assert target._batch_events == {}
    assert target.sum == 0
    for i in range(2, 5):
        await controller.emit(i)

    await asyncio.sleep(0.1)

    expected_sum = 9  # 2 + 3 + 4

    assert target._batch_events == {}
    assert target.sum == expected_sum

    await controller.terminate()
    await controller.await_termination()

    assert target._batch_events == {}
    assert target.sum == expected_sum


def test_error_raising_batch_target():
    asyncio.run(async_test_error_raising_batch_target())


class MyLoop(Map):
    def __init__(self, iterations: int, end, counter, **kwargs):
        super().__init__(**kwargs)
        self.iterations = iterations
        self.end = end
        self.counter = counter

    def select_outlets(self, event):
        outlets = [self.counter]
        if event > self.iterations:
            outlets = [self.end]
        return outlets


@pytest.mark.parametrize("fn_select_outlets", [True, False])
def test_regular_step_with_choice(fn_select_outlets):
    class MyStep(Map):
        def select_outlets(self, event):
            outlets = ["all_events"]
            if event > 5:
                outlets.append("more_than_five")
            else:
                outlets.append("up_to_five")
            return outlets

    def select(event):
        outlets = ["all_events"]
        if event > 5:
            outlets.append("more_than_five")
        else:
            outlets.append("up_to_five")
        return outlets

    source = SyncEmitSource()
    if fn_select_outlets:
        my_step = Map(fn=lambda x: x, termination_result_fn=lambda x, y: x + y, fn_select_outlets=select)
    else:
        my_step = MyStep(fn=lambda x: x, termination_result_fn=lambda x, y: x + y)

    all_events = Map(lambda x: x, name="all_events")
    more_than_five = Map(lambda x: x * 10, name="more_than_five")
    up_to_five = Map(lambda x: x * 100, name="up_to_five")
    sum_up_all_events = Reduce(0, lambda acc, x: acc + x)
    sum_up_more_than_five = Reduce(0, lambda acc, x: acc + x)
    sum_up_up_to_five = Reduce(0, lambda acc, x: acc + x)

    source.to(my_step)
    my_step.to(all_events)
    my_step.to(more_than_five)
    my_step.to(up_to_five)
    all_events.to(sum_up_all_events)
    more_than_five.to(sum_up_more_than_five)
    up_to_five.to(sum_up_up_to_five)

    controller = source.run()

    for i in range(4, 8):
        controller.emit(i)

    controller.terminate()
    termination_result = controller.await_termination()

    expected = sum(range(4, 8)) + sum(range(6, 8)) * 10 + sum(range(4, 6)) * 100
    assert termination_result == expected


@pytest.mark.parametrize("iterations", [5, 10])
@pytest.mark.parametrize("with_recovery", [True, False])
def test_cyclic_graphs(iterations, with_recovery):
    source = SyncEmitSource()
    my_loop = MyLoop(
        fn=lambda x: x, iterations=iterations, name="my_loop", end="end", counter="counter", max_iterations=5
    )
    start = Map(lambda x: x, name="start")
    counter = Map(lambda x: x + 1, name="counter", max_iterations=5)
    end = Map(lambda x: x, name="end")

    source.to(start)
    start.to(counter)
    counter.to(my_loop)
    my_loop.to(end)
    end.to(Complete())
    my_loop.to(counter)
    if with_recovery:
        recovery_step = Map(lambda x: -1, name="end-2")
        counter.set_recovery_step(recovery_step)
        my_loop.set_recovery_step(recovery_step)
        recovery_step.to(Complete())

    controller = source.run()

    if iterations == 5:
        awaitable_result = controller.emit(1)
        assert awaitable_result.await_result() == iterations + 1
    else:
        if with_recovery:
            awaitable_result = controller.emit(1)
            assert awaitable_result.await_result() == -1
        else:
            with pytest.raises(RuntimeError, match=r"Max iterations exceeded"):
                awaitable_result = controller.emit(1)
                awaitable_result.await_result()

    controller.terminate()
    try:
        controller.await_termination()
    except RuntimeError:
        if iterations == 10 and not with_recovery:
            pass
        else:
            raise


def test_two_cyclic_graphs():
    source = SyncEmitSource()
    my_loop = MyLoop(fn=lambda x: x, iterations=5, end="counter_2", counter="counter_1", name="my_loop")
    start = Map(lambda x: x, name="start")
    counter = Map(lambda x: x + 1, name="counter_1")
    counter_2 = Map(lambda x: x + 1, name="counter_2")
    end = Map(lambda x: x, name="end")
    my_loop_2 = MyLoop(fn=lambda x: x, iterations=10, end="end", counter="counter_2", name="my_loop_2")

    source.to(start)
    start.to(counter)
    counter.to(my_loop)
    my_loop.to(counter_2)
    my_loop.to(counter)
    counter_2.to(my_loop_2)
    my_loop_2.to(end)
    my_loop_2.to(counter_2)
    end.to(Complete())
    controller = source.run()

    awaitable_result = controller.emit(1)
    assert awaitable_result.await_result() == 11
    controller.terminate()
    controller.await_termination()


def test_flow_reuse_with_cycle():
    """Test that flows with cyclic structures can be reused multiple times.

    A cyclic structure is created where MyLoop routes events back to counter
    step when event <= iterations, creating a loop.
    """
    # Build the flow ONCE with a cyclic structure
    source = SyncEmitSource()
    my_loop = MyLoop(fn=lambda x: x, iterations=5, name="my_loop", end="end", counter="counter")
    counter = Map(lambda x: x + 1, name="counter")
    end = Map(lambda x: x, name="end")

    source.to(counter)
    counter.to(my_loop)
    my_loop.to(end)
    end.to(Complete())
    # Create the cycle by appending counter as an outlet of my_loop
    my_loop.to(counter)

    # Run the SAME flow 3 times to test reusability with cyclic structure
    for run_num in range(3):
        controller = source.run()
        awaitable_result = controller.emit(1)
        result = awaitable_result.await_result()
        # Event 1 -> counter: 1+1=2 -> my_loop: 2 <= 5 -> counter: 2+1=3 -> my_loop: 3 <= 5 -> counter: 3+1=4 -> ...
        # -> counter: 5+1=6 -> my_loop: 6 > 5 -> end: 6
        assert result == 6, f"Run {run_num}: Expected 6 but got {result}"

        controller.terminate()
        controller.await_termination()


def test_flow_reuse_resets_closeables():
    """Test that _closeables is properly reset when a flow is reused.

    Baseline test for flow reuse without closeables. This test uses steps (Map, Reduce)
    that don't create closeables, so it verifies the baseline behavior.
    """
    source = SyncEmitSource()
    map_step = Map(lambda x: x + 1)
    reduce_step = Reduce(0, lambda acc, x: acc + x)

    source.to(map_step).to(reduce_step)

    for run_num in range(3):
        controller = source.run()
        try:
            assert len(source._closeables) == 0
            for i in range(5):
                controller.emit(i)
        finally:
            controller.terminate()
        result = controller.await_termination()
        assert result == 15, f"Run {run_num}: Expected 15 but got {result}"


def test_map_with_state_reuse_resets_closeables():
    """Test that MapWithState properly resets _closeables on flow reuse.

    Regression test for ML-11518: Without resetting _closeables in _init(), closeables
    accumulate across runs, causing close() to be called multiple times per run.
    """

    class CloseCounter:
        def __init__(self):
            self.close_count = 0

        def close(self):
            self.close_count += 1

    state = CloseCounter()
    source = SyncEmitSource()
    map_with_state = MapWithState(state, lambda x, s: (x, s))
    reduce_step = Reduce([], lambda acc, x: acc + [x])

    source.to(map_with_state).to(reduce_step)

    for run_num in range(3):
        initial_close_count = state.close_count
        controller = source.run()
        try:
            controller.emit(1)
        finally:
            controller.terminate()
        controller.await_termination()

        # close() should be called exactly once per run
        closes_this_run = state.close_count - initial_close_count
        assert closes_this_run == 1, (
            f"Run {run_num}: state.close() should be called exactly once, "
            f"but was called {closes_this_run} times (total: {state.close_count})"
        )


class Tracer(MapClass):
    def do(self, x):
        loop_count = x.get("loop", 0) + 1
        trace = x.get("trace", []) + [self.name]
        result = {"data": x.get("data"), "trace": trace, "loop": loop_count}
        loop_count = loop_count / 6
        if loop_count % 10 == 0:
            print(f"Loop {loop_count}: Last 10 steps: {trace[-10:]}")
        return result

    def select_outlets(self, event):
        loop_count = event.get("loop", 0)

        if loop_count >= 100000:
            return ["end"]
        if self.name == "step_6":
            return ["step_1"]
        if self.name == "step_7":
            return ["step_1", "step_2"]
        return None


def test_maximum_recursion(monkeypatch):
    monkeypatch.setenv("DEFAULT_MAX_ITERATIONS_FOR_CYCLES", "5")
    source = SyncEmitSource()

    # Create 6 steps WITHOUT max_iterations
    step1 = Tracer(name="step_1", full_event=False)
    step2 = Tracer(name="step_2", full_event=False)
    step3 = Tracer(name="step_3", full_event=False)
    step4 = Tracer(name="step_4", full_event=False)
    step5 = Tracer(name="step_5", full_event=False)
    step6 = Tracer(name="step_6", full_event=False)

    # NO Complete() - just create a pure cycle
    source.to(step1)
    step1.to(step2)
    step2.to(step3)
    step3.to(step4)
    step4.to(step5)
    step5.to(step6)

    # Create the cycle: step6 -> step1
    step6.to(step1)
    step6.to(Complete(name="end"))

    controller = source.run()
    awaitable_result = controller.emit({"data": "test"})

    try:
        with pytest.raises(RuntimeError, match=r"exceeded the default cycle"):
            awaitable_result.await_result()
    finally:
        controller.terminate()
    with pytest.raises(RuntimeError, match=r"exceeded the default cycle"):
        controller.await_termination()


def test_maximum_recursion_two_cycles(monkeypatch):
    monkeypatch.setenv("DEFAULT_MAX_ITERATIONS_FOR_CYCLES", "5")
    source = SyncEmitSource()

    # Create 6 steps WITHOUT max_iterations
    step1 = Tracer(name="step_1", full_event=False)
    step2 = Tracer(name="step_2", full_event=False)
    step3 = Tracer(name="step_3", full_event=False)
    step4 = Tracer(name="step_4", full_event=False)
    step5 = Tracer(name="step_5", full_event=False)
    step7 = Tracer(name="step_7", full_event=False)

    # NO Complete() - just create a pure cycle
    source.to(step1)
    step1.to(step2)
    step2.to(step3)
    step3.to(step4)
    step4.to(step5)
    step5.to(step7)

    # Create the cycle: step6 -> step1
    step7.to(step1)
    step7.to(step2)
    step7.to(Complete(name="end"))

    controller = source.run()
    awaitable_result = controller.emit({"data": "test"})

    try:
        with pytest.raises(RuntimeError, match=r"exceeded the default cycle"):
            awaitable_result.await_result()
    finally:
        controller.terminate()
    with pytest.raises(RuntimeError, match=r"exceeded the default cycle"):
        controller.await_termination()


def test_map_with_state_no_closeables_without_close_method():
    """Test that MapWithState doesn't add state to _closeables if it has no close method.

    Edge case test: When using a plain dict as state (no close method), _closeables
    should remain empty. This verifies the hasattr(self._state, "close") check works.
    """
    initial_state = {"count": 0}

    def state_fn(event, state):
        state["count"] += 1
        return event["value"] * state["count"], state

    source = SyncEmitSource()
    map_with_state = MapWithState(initial_state, state_fn, group_by_key=False)
    reduce_step = Reduce(0, lambda acc, x: acc + x)

    source.to(map_with_state).to(reduce_step)

    for _ in range(3):
        controller = source.run()
        try:
            # Dict has no close method, so _closeables should be empty
            assert map_with_state._closeables == []
            for i in range(3):
                controller.emit({"value": i + 1})
        finally:
            controller.terminate()
        controller.await_termination()


def test_nosql_target_reuse_resets_closeables():
    """Test that NoSqlTarget properly resets _closeables on flow reuse.

    Regression test for ML-11518: Without resetting _closeables in _init(), closeables
    accumulate in the source's _closeables list across runs.
    """
    table = Table("test_nosql", NoopDriver())

    source = SyncEmitSource()
    nosql_target = NoSqlTarget(table)

    source.to(nosql_target)

    for _ in range(3):
        controller = source.run()
        try:
            assert len(source._closeables) == 1
            # Emit some data with keys
            for i in range(3):
                controller.emit(Event(body={"col": i}, key=f"key{i}"))
        finally:
            controller.terminate()
        controller.await_termination()


class TestBatchWithParallelExecution:
    class RunnableMultiplyBy2(ParallelExecutionRunnable):

        def run(self, data, path, origin_name=None):
            if isinstance(data, list):
                return [sub_value * 2 for sub_value in data]
            return data * 2

    class RunnableAdd10(ParallelExecutionRunnable):

        def run(self, data, path, origin_name=None):
            if isinstance(data, list):
                return [sub_value + 10 for sub_value in data]
            return data + 10

    class RunnableGetRandom(ParallelExecutionRunnable):

        def run(self, data, path, origin_name=None):
            random_uuid = str(uuid.uuid4())
            if isinstance(data, list):
                return [random_uuid] * len(data)
            return random_uuid

    class RunnableRaiseIfNegative(ParallelExecutionRunnable):
        def run(self, data, path, origin_name=None):
            if isinstance(data, list):
                results = []
                for item in data:
                    if item < 0:
                        raise ValueError(f"Value {item} is negative!")
                    results.append(item * 2)
                return results
            else:
                if data < 0:
                    raise ValueError(f"Value {data} is negative!")
                return data * 2

    class RunnableReturnWrongType(ParallelExecutionRunnable):
        def run(self, data, path, origin_name=None):
            if isinstance(data, list):
                # Wrong! Should return a list, not a dict
                return {"result": "wrong_type"}
            return data

    class MyParallelExecution(ParallelExecution):
        def select_runnables(self, event):
            return ["multiply", "add", "uuid"]

    def test_basic_batch_with_parallel_execution(self):
        asyncio.run(self.async_test_basic_batch_with_parallel_execution())

    async def async_test_basic_batch_with_parallel_execution(self):
        """Test that Batch step with full_event=True works correctly with ParallelExecution."""
        batch_size = 3
        number_of_events = 10

        runnables = [
            self.RunnableMultiplyBy2("multiply"),
            self.RunnableAdd10("add"),
            self.RunnableGetRandom("uuid"),
        ]
        parallel_execution = self.MyParallelExecution(
            runnables,
            execution_mechanism_by_runnable_name={
                "multiply": "naive",
                "add": "naive",
                "uuid": "naive",
            },
        )
        controller = build_flow(
            [
                AsyncEmitSource(),
                Batch(max_events=batch_size, full_event=True, flush_after_seconds=2),
                parallel_execution,
                FlatMap(fn=lambda x: x.body, full_event=True),
                Complete(),
                Reduce(initial_value=[], fn=lambda acc, x: append_and_return(acc, x)),
            ]
        ).run()

        async def emit_event(i):
            result = await controller.emit(i)
            # Verify each event has the expected fields after parallel execution
            assert "add" in result
            assert "multiply" in result
            assert "uuid" in result
            assert result["add"] == 10 + i
            assert result["multiply"] == i * 2

        # Emit events in parallel using asyncio
        try:
            await self._emit_batch_concurrently(emit_event, range(number_of_events))
        finally:
            await controller.terminate()
            termination_result = await controller.await_termination()
        assert len(termination_result) == number_of_events

        previous_batch_number = -1
        expected_uuid = ""
        for i in range(number_of_events):
            batch_number = math.floor(i / batch_size)
            if previous_batch_number == -1 or batch_number != previous_batch_number:
                expected_uuid = termination_result[i]["uuid"]
            else:
                assert termination_result[i]["uuid"] == expected_uuid
            previous_batch_number = batch_number

    def test_batch_with_parallel_execution_split(self):
        asyncio.run(self.async_test_batch_with_parallel_execution_split())

    async def async_test_batch_with_parallel_execution_split(self):
        """Test batched ParallelExecution with splitting to multiple outlets (len(outlets) > 1 path)."""
        batch_size = 3
        number_of_events = 10

        runnables = [
            self.RunnableMultiplyBy2("multiply"),
            self.RunnableAdd10("add"),
            self.RunnableGetRandom("uuid"),
        ]

        source = AsyncEmitSource()
        batch_step = Batch(max_events=batch_size, full_event=True, flush_after_seconds=2)
        parallel_execution = self.MyParallelExecution(
            runnables,
            execution_mechanism_by_runnable_name={
                "multiply": "naive",
                "add": "naive",
                "uuid": "naive",
            },
        )

        flat_map1 = FlatMap(fn=lambda x: x.body, full_event=True)
        flat_map2 = FlatMap(fn=lambda x: x.body, full_event=True)
        complete = Complete()
        reducer = Reduce([], lambda acc, x: append_and_return(acc, x))

        source.to(batch_step).to(parallel_execution)
        parallel_execution.to(flat_map1).to(complete).to(reducer)
        parallel_execution.to(flat_map2).to(reducer)

        controller = source.run()

        async def emit_event(i):
            result = await controller.emit(i)
            # Each event should get results from both branches (2 completions)
            assert len(result) == 3
            assert result["add"] == 10 + i
            assert result["multiply"] == i * 2
            assert "uuid" in result
            return result

        try:
            await self._emit_batch_concurrently(emit_event, range(number_of_events))
        finally:
            await controller.terminate()
            termination_result = await controller.await_termination()

        # The final result should contain a duplicated value since the flow is split into two branches
        expected_number_of_events = number_of_events * 2
        assert len(termination_result) == expected_number_of_events

        # Sort by value to ensure correct order after split
        termination_result = sorted(termination_result, key=lambda x: (x["add"]))

        previous_batch_number = -1
        expected_uuid = ""
        # because of the split, the batch size of the results is doubled
        batch_size = batch_size * 2
        for i in range(expected_number_of_events):
            # because of the split, we expect alternating add/multiply values every two items
            fixed_index = math.floor(i / 2)
            expected_add = 10 + fixed_index
            expected_multiply = fixed_index * 2
            assert termination_result[i]["add"] == expected_add
            assert termination_result[i]["multiply"] == expected_multiply
            batch_number = math.floor(i / batch_size)
            if previous_batch_number == -1 or batch_number != previous_batch_number:
                expected_uuid = termination_result[i]["uuid"]
            else:
                assert termination_result[i]["uuid"] == expected_uuid
            previous_batch_number = batch_number

    def _create_error_handling_flow(self, runnables, batch_size=3, flush_after_seconds=0.3):
        """Helper to create flow for error handling tests."""
        source = AsyncEmitSource()
        batch_step = Batch(max_events=batch_size, full_event=True, flush_after_seconds=flush_after_seconds)

        execution_mechanism_by_runnable_name = {runnable.name: "naive" for runnable in runnables}
        parallel_execution = ParallelExecution(
            runnables, execution_mechanism_by_runnable_name=execution_mechanism_by_runnable_name
        )
        reducer = Reduce([], append_and_return)

        return build_flow(
            [
                source,
                batch_step,
                parallel_execution,
                FlatMap(fn=lambda x: x.body, full_event=True),
                Complete(),
                reducer,
            ]
        ).run()

    async def _emit_batch_concurrently(self, emit_fn, values):
        """Helper to emit multiple values concurrently."""
        tasks = [asyncio.create_task(emit_fn(v)) for v in values]
        await asyncio.gather(*tasks)

    def test_batch_error_handling_single_runnable(self):
        asyncio.run(self.async_test_batch_error_handling_single_runnable())

    async def async_test_batch_error_handling_single_runnable(self):
        """Test error handling in batched parallel execution with single runnable."""

        async def emit_valid_event(value):
            invocation_result = await controller.emit(value)
            assert invocation_result == value * 2

        async def emit_error_event(value):
            invocation_result = await controller.emit(value)
            assert invocation_result == {"error": "ValueError: Value -5 is negative!"}

        flush_after_seconds = 0.3
        runnables = [self.RunnableRaiseIfNegative("check_positive", raise_exception=False)]
        controller = self._create_error_handling_flow(runnables, flush_after_seconds=flush_after_seconds)

        # Emit valid batch first (should succeed)
        await self._emit_batch_concurrently(emit_valid_event, [1, 2, 3])

        time.sleep(flush_after_seconds + 0.2)  # Ensure different batch window

        # Emit batch with negative value concurrently (should error for all in batch)
        await self._emit_batch_concurrently(emit_error_event, [4, -5, 6])

        await controller.terminate()
        batch_result = await controller.await_termination()

        # Should have 6 results total (3 valid + 3 error)
        assert len(batch_result) == 6

        # First 3 should succeed
        assert batch_result[0] == 2
        assert batch_result[1] == 4
        assert batch_result[2] == 6

        # Next 3 should all have error (error propagated to all in batch)
        for single_result in batch_result[3:]:
            assert single_result == {"error": "ValueError: Value -5 is negative!"}

    def test_batch_error_handling_multiple_runnables(self):
        asyncio.run(self.async_test_batch_error_handling_multiple_runnables())

    async def async_test_batch_error_handling_multiple_runnables(self):
        """Test error handling in batched parallel execution with multiple runnables."""
        flush_after_seconds = 0.3

        class RunnableAddTen(ParallelExecutionRunnable):
            def run(self, data, path, origin_name=None):
                if isinstance(data, list):
                    return [item + 10 for item in data]
                return data + 10

        async def emit_error_event(value):
            invocation_result = await controller.emit(value)
            # check_positive should have error, but add_ten should still work
            assert invocation_result["check_positive"] == {"error": "ValueError: Value -5 is negative!"}
            assert invocation_result["add_ten"] == value + 10

        async def emit_valid_event(value):
            invocation_result = await controller.emit(value)
            assert invocation_result["check_positive"] == value * 2
            assert invocation_result["add_ten"] == value + 10

        runnables = [
            self.RunnableRaiseIfNegative("check_positive", raise_exception=False),
            RunnableAddTen("add_ten", raise_exception=False),
        ]
        controller = self._create_error_handling_flow(runnables, flush_after_seconds=flush_after_seconds)
        # Emit valid batch first (should succeed)
        await self._emit_batch_concurrently(emit_valid_event, [1, 2, 3])

        time.sleep(flush_after_seconds + 0.2)  # Ensure different batch window
        # Emit batch with negative value concurrently (should error for check_positive, but add_ten works)
        await self._emit_batch_concurrently(emit_error_event, [4, -5, 6])

        await controller.terminate()
        batch_result = await controller.await_termination()

        # Should have 6 results total (3 valid + 3 with partial error)
        assert len(batch_result) == 6

        # First 3 should succeed with both runnables
        assert batch_result[0]["check_positive"] == 2
        assert batch_result[0]["add_ten"] == 11
        assert batch_result[1]["check_positive"] == 4
        assert batch_result[1]["add_ten"] == 12
        assert batch_result[2]["check_positive"] == 6
        assert batch_result[2]["add_ten"] == 13

        # Next 3 should have error for check_positive but add_ten should work
        for i, single_result in enumerate(batch_result[3:]):
            assert single_result["check_positive"] == {"error": "ValueError: Value -5 is negative!"}
            # add_ten should still work for values [4, -5, 6]
            expected_add_values = [14, 5, 16]
            assert single_result["add_ten"] == expected_add_values[i]

    def test_batch_unexpected_return_type_single_runnable(self):
        asyncio.run(self.async_test_batch_unexpected_return_type_single_runnable())

    async def async_test_batch_unexpected_return_type_single_runnable(self):
        flush_after_seconds = 0.3
        runnables = [self.RunnableReturnWrongType("wrong_type", raise_exception=False)]
        controller = self._create_error_handling_flow(runnables, flush_after_seconds=flush_after_seconds)

        async def emit_event(value):
            invocation_result = await controller.emit(value)
            # All events in batch should get the same dict result
            assert invocation_result == {"result": "wrong_type"}

        # Emit batch that will trigger the warning (returning dict instead of list)
        await self._emit_batch_concurrently(emit_event, [1, 2, 3])

        await controller.terminate()
        batch_result = await controller.await_termination()

        # All 3 should have the same dict body (warning was logged, but flow continued)
        assert len(batch_result) == 3
        for single_result in batch_result:
            assert single_result == {"result": "wrong_type"}

    def test_batch_unexpected_return_type_multiple_runnables(self):
        asyncio.run(self.async_test_batch_unexpected_return_type_multiple_runnables())

    async def async_test_batch_unexpected_return_type_multiple_runnables(self):
        flush_after_seconds = 0.3
        runnables = [
            self.RunnableReturnWrongType("wrong_type", raise_exception=False),
            self.RunnableAdd10("add_ten", raise_exception=False),
        ]
        controller = self._create_error_handling_flow(runnables, flush_after_seconds=flush_after_seconds)

        async def emit_event(value):
            invocation_result = await controller.emit(value)
            # wrong_type should return same dict for all, add_ten should work correctly
            assert invocation_result["wrong_type"] == {"result": "wrong_type"}
            assert invocation_result["add_ten"] == value + 10

        # Emit batch that will trigger the warning (returning dict instead of list)
        await self._emit_batch_concurrently(emit_event, [1, 2, 3])

        await controller.terminate()
        batch_result = await controller.await_termination()

        # All 3 should have the same dict for wrong_type, correct values for add_ten
        assert len(batch_result) == 3
        for single_result in batch_result:
            assert single_result["wrong_type"] == {"result": "wrong_type"}
            assert single_result["add_ten"] in [11, 12, 13]
