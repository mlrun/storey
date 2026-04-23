# Copyright 2026 Iguazio
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
import inspect
import time
from typing import AsyncGenerator, Generator, Optional

import pytest

from storey import (
    AsyncEmitSource,
    Choice,
    Collector,
    Complete,
    ConcurrentExecution,
    Flow,
    Map,
    MapClass,
    ParallelExecution,
    ParallelExecutionMechanisms,
    ParallelExecutionRunnable,
    Reduce,
    RunnableExecutor,
    StreamingError,
    SyncEmitSource,
    build_flow,
)
from storey.dtypes import Event, StreamChunk, StreamCompletion
from storey.flow import _is_generator
from tests.helpers import MockContext, MockLogger

_SYNC_STREAMING_DELAY = 1.0


def _sync_streaming_fn(event):
    """Module-level sync streaming function for concurrency tests (must be picklable for process_pool)."""
    time.sleep(_SYNC_STREAMING_DELAY)
    for i in range(3):
        yield f"{event}_chunk_{i}"


def _sync_error_streaming_fn(event):
    """Module-level sync generator that yields one chunk then raises (must be picklable for process_pool)."""
    yield f"{event}_chunk_0"
    raise ValueError("sync generator error mid-stream")


class StreamingRunnable(ParallelExecutionRunnable):
    """A streaming runnable that yields 3 chunks for testing."""

    def run(self, body, path: str, origin_name: Optional[str] = None) -> Generator:
        for i in range(3):
            yield f"{body}_chunk_{i}"


class AsyncStreamingRunnable(ParallelExecutionRunnable):
    """An async streaming runnable that yields 3 chunks for testing."""

    async def run_async(self, body, path: str, origin_name: Optional[str] = None) -> AsyncGenerator:
        for i in range(3):
            yield f"{body}_chunk_{i}"


class ErrorStreamingRunnable(ParallelExecutionRunnable):
    """A streaming runnable that yields one chunk then raises an error."""

    def run(self, body, path: str, origin_name: Optional[str] = None) -> Generator:
        yield f"{body}_chunk_0"
        raise ValueError("Simulated streaming error")


class NonStreamingRunnable(ParallelExecutionRunnable):
    """A non-streaming runnable that returns a single value."""

    def run(self, body, path: str, origin_name: Optional[str] = None):
        return f"{body}_result"


class TestStreamingPrimitives:
    """Tests for streaming primitive classes."""

    def test_stream_chunk_creation(self):
        chunk = StreamChunk("test body")
        assert chunk.body == "test body"

    def test_stream_chunk_repr(self):
        chunk = StreamChunk({"key": "value"})
        assert "StreamChunk" in repr(chunk)

    def test_stream_completion_creation(self):
        event = Event(body="test", id="123")
        completion = StreamCompletion("test_step", event)
        assert completion.streaming_step == "test_step"
        assert completion.original_event is event

    def test_stream_completion_repr(self):
        event = Event(body="test", id="abc")
        completion = StreamCompletion("my_step", event)
        assert "StreamCompletion" in repr(completion)
        assert "my_step" in repr(completion)

    def test_stream_completion_with_error(self):
        """Test StreamCompletion with error string."""
        event = Event(body="test", id="123")
        completion = StreamCompletion("test_step", event, error="ValueError: test error")
        assert completion.streaming_step == "test_step"
        assert completion.original_event is event
        assert completion.error == "ValueError: test error"

    def test_stream_completion_with_error_repr(self):
        """Test StreamCompletion repr includes error string."""
        event = Event(body="test", id="abc")
        completion = StreamCompletion("my_step", event, error="RuntimeError: something went wrong")
        repr_str = repr(completion)
        assert "StreamCompletion" in repr_str
        assert "my_step" in repr_str
        assert "RuntimeError" in repr_str


class TestIsGenerator:
    """Tests for the _is_generator utility function."""

    def test_is_generator_sync(self):
        def gen():
            yield 1
            yield 2

        assert _is_generator(gen())

    def test_is_generator_async(self):
        async def async_gen():
            yield 1
            yield 2

        assert _is_generator(async_gen())

    def test_is_generator_non_generator(self):
        assert not _is_generator([1, 2, 3])
        assert not _is_generator("string")
        assert not _is_generator(42)

    def test_is_generator_coroutine(self):
        async def coro():
            return 1

        # Coroutine is not a generator
        c = coro()
        try:
            assert not _is_generator(c)
        finally:
            c.close()


class TestIsStreamingMethod:
    """Tests for ParallelExecutionRunnable.is_streaming() method."""

    def test_is_streaming_sync_generator(self):
        """Test that a runnable with a sync generator run() is detected as streaming."""
        runnable = StreamingRunnable(name="test")
        assert runnable.is_streaming() is True

    def test_is_streaming_async_generator(self):
        """Test that a runnable with an async generator run_async() is detected as streaming."""
        runnable = AsyncStreamingRunnable(name="test")
        assert runnable.is_streaming() is True

    def test_is_streaming_non_generator(self):
        """Test that a runnable with a non-generator run() is not detected as streaming."""
        runnable = NonStreamingRunnable(name="test")
        assert runnable.is_streaming() is False

    def test_is_streaming_base_class(self):
        """Test that the base ParallelExecutionRunnable is not streaming by default."""
        runnable = ParallelExecutionRunnable(name="test")
        assert runnable.is_streaming() is False

    def test_is_streaming_override(self):
        """Test that is_streaming() can be overridden by subclasses."""

        class OverriddenRunnable(ParallelExecutionRunnable):
            """A runnable that overrides is_streaming() to return True."""

            def is_streaming(self) -> bool:
                return True

            def run(self, body, path: str, origin_name: Optional[str] = None):
                # Even though run() is not a generator, is_streaming() returns True
                return f"{body}_result"

        runnable = OverriddenRunnable(name="test")
        assert runnable.is_streaming() is True


class TestMapStreaming:
    """Tests for Map step streaming support."""

    def test_map_sync_generator(self):
        def stream_chunks(x):
            for i in range(3):
                yield f"{x}_chunk_{i}"

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(stream_chunks),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        # Results should contain all chunks
        assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

    def test_map_async_generator(self):
        async def stream_chunks(x):
            for i in range(3):
                yield f"{x}_chunk_{i}"

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(stream_chunks),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

    def test_async_map_sync_generator(self):
        """Async version: Test Map with sync generator using AsyncEmitSource."""

        async def _test():
            def stream_chunks(x):
                for i in range(3):
                    yield f"{x}_chunk_{i}"

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(stream_chunks),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("test")
            await controller.terminate()
            result = await controller.await_termination()

            assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

        asyncio.run(_test())

    def test_async_map_async_generator(self):
        """Async version: Test Map with async generator using AsyncEmitSource."""

        async def _test():
            async def stream_chunks(x):
                for i in range(3):
                    yield f"{x}_chunk_{i}"

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(stream_chunks),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("test")
            await controller.terminate()
            result = await controller.await_termination()

            assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

        asyncio.run(_test())


class TestMapClassStreaming:
    """Tests for MapClass step streaming support."""

    def test_mapclass_sync_generator(self):
        class StreamingMapper(MapClass):
            def do(self, x):
                for i in range(3):
                    yield f"{x}_chunk_{i}"

        controller = build_flow(
            [
                SyncEmitSource(),
                StreamingMapper(),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

    def test_mapclass_async_generator(self):
        class AsyncStreamingMapper(MapClass):
            async def do(self, x):
                for i in range(3):
                    yield f"{x}_chunk_{i}"

        controller = build_flow(
            [
                SyncEmitSource(),
                AsyncStreamingMapper(),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

    def test_async_mapclass_sync_generator(self):
        """Async version: Test MapClass with sync generator using AsyncEmitSource."""

        async def _test():
            class StreamingMapper(MapClass):
                def do(self, x):
                    for i in range(3):
                        yield f"{x}_chunk_{i}"

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    StreamingMapper(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("test")
            await controller.terminate()
            result = await controller.await_termination()

            assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

        asyncio.run(_test())

    def test_async_mapclass_async_generator(self):
        """Async version: Test MapClass with async generator using AsyncEmitSource."""

        async def _test():
            class AsyncStreamingMapper(MapClass):
                async def do(self, x):
                    for i in range(3):
                        yield f"{x}_chunk_{i}"

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    AsyncStreamingMapper(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("test")
            await controller.terminate()
            result = await controller.await_termination()

            assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

        asyncio.run(_test())


class TestCollector:
    """Tests for the Collector step."""

    def test_collector_basic(self):
        """Test that Collector aggregates streaming chunks."""

        def stream_chunks(x):
            for i in range(3):
                yield f"{x}_chunk_{i}"

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(stream_chunks),
                Collector(),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        # Collector should emit a single event with collected chunks
        assert len(result) == 1
        assert result[0] == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

    def test_collector_passthrough_non_streaming(self):
        """Test that Collector passes through non-streaming events."""

        def transform(x):
            return x * 2

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(transform),
                Collector(),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit(5)
        controller.emit(10)
        controller.terminate()
        result = controller.await_termination()

        assert result == [10, 20]

    def test_collector_single_chunk(self):
        """Test that Collector emits a single chunk directly, not in a list."""

        def single_chunk(x):
            yield x * 2

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(single_chunk),
                Collector(),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit(5)
        controller.terminate()
        result = controller.await_termination()

        assert result == [10]

    def test_collector_multiple_streams(self):
        """Test that Collector can handle multiple concurrent streams."""

        def stream_chunks(x):
            for i in range(2):
                yield f"{x}_{i}"

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(stream_chunks),
                Collector(),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("a")
        controller.emit("b")
        controller.terminate()
        result = controller.await_termination()

        assert len(result) == 2
        assert ["a_0", "a_1"] in result
        assert ["b_0", "b_1"] in result

    def test_collector_sets_stream_collected_marker(self):
        """Test that Collector sets stream_collected=True on collected events."""

        def stream_chunks(x):
            for i in range(2):
                yield f"{x}_{i}"

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(stream_chunks),
                Collector(),
                Reduce([], lambda acc, x: acc + [x], full_event=True),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        assert len(result) == 1
        event = result[0]
        assert event.stream_collected is True

    def test_collector_passthrough_no_stream_collected_marker(self):
        """Test that Collector does NOT set stream_collected on non-streaming events."""

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(lambda x: x * 2),
                Collector(),
                Reduce([], lambda acc, x: acc + [x], full_event=True),
            ]
        ).run()

        controller.emit(5)
        controller.terminate()
        result = controller.await_termination()

        assert len(result) == 1
        event = result[0]
        assert getattr(event, "stream_collected", False) is False

    def test_collector_invalid_expected_completions(self):
        """Test that Collector raises error for invalid expected_completions."""
        with pytest.raises(ValueError, match="expected_completions must be at least 1"):
            Collector(expected_completions=0)

    def test_async_collector_basic(self):
        """Async version: Test that Collector aggregates streaming chunks."""

        async def _test():
            def stream_chunks(x):
                for i in range(3):
                    yield f"{x}_chunk_{i}"

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(stream_chunks),
                    Collector(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("test")
            await controller.terminate()
            result = await controller.await_termination()

            assert len(result) == 1
            assert result[0] == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

        asyncio.run(_test())

    def test_async_collector_passthrough_non_streaming(self):
        """Async version: Test that Collector passes through non-streaming events."""

        async def _test():
            def transform(x):
                return x * 2

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(transform),
                    Collector(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit(5)
            await controller.emit(10)
            await controller.terminate()
            result = await controller.await_termination()

            assert result == [10, 20]

        asyncio.run(_test())

    def test_async_collector_multiple_streams(self):
        """Async version: Test that Collector can handle multiple concurrent streams."""

        async def _test():
            def stream_chunks(x):
                for i in range(2):
                    yield f"{x}_{i}"

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(stream_chunks),
                    Collector(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("a")
            await controller.emit("b")
            await controller.terminate()
            result = await controller.await_termination()

            assert len(result) == 2
            assert ["a_0", "a_1"] in result
            assert ["b_0", "b_1"] in result

        asyncio.run(_test())

    def test_async_collector_single_chunk(self):
        """Async version: Test that Collector emits a single chunk directly, not in a list."""

        async def _test():
            def single_chunk(x):
                yield x * 2

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(single_chunk),
                    Collector(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit(5)
            await controller.terminate()
            result = await controller.await_termination()

            assert result == [10]

        asyncio.run(_test())

    def test_collector_empty_stream(self):
        """Test that Collector emits an empty list for a stream with zero chunks."""

        def empty_stream(x):
            return
            yield  # Makes it a generator

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(empty_stream),
                Collector(),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        # Empty stream should emit an empty list
        assert len(result) == 1
        assert result[0] == []

    def test_async_collector_empty_stream(self):
        """Async version: Test that Collector emits an empty list for a stream with zero chunks."""

        async def _test():
            def empty_stream(x):
                return
                yield  # Makes it a generator

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(empty_stream),
                    Collector(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("test")
            await controller.terminate()
            result = await controller.await_termination()

            assert len(result) == 1
            assert result[0] == []

        asyncio.run(_test())

    def test_collector_streaming_error_emits_error_dict(self):
        """Test that Collector emits an error dict when a generator raises mid-stream.

        When a streaming generator raises an exception, the Collector should emit
        an event with body={"error": "ExceptionType: message"} matching the
        non-streaming error format from ParallelExecutionRunnable._run().
        """

        def error_stream(x):
            yield f"{x}_chunk_0"
            raise ValueError("Generator error mid-stream")

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(error_stream),
                Collector(),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        try:
            controller.emit("test")
        finally:
            controller.terminate()
            result = controller.await_termination()

        assert len(result) == 1
        assert isinstance(result[0], dict)
        assert "error" in result[0]
        assert "ValueError" in result[0]["error"]
        assert "Generator error mid-stream" in result[0]["error"]

    def test_async_collector_streaming_error(self):
        """Async version: Test streaming error propagation through Collector."""

        async def _test():

            def error_stream(x):
                yield f"{x}_chunk_0"
                raise RuntimeError("async stream failure")

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(error_stream),
                    Collector(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            try:
                await controller.emit("test")
            finally:
                await controller.terminate()
                result = await controller.await_termination()

            assert len(result) == 1
            assert isinstance(result[0], dict)
            assert "error" in result[0]
            assert "RuntimeError" in result[0]["error"]
            assert "async stream failure" in result[0]["error"]

        asyncio.run(_test())

    def test_collector_streaming_error_sets_stream_collected(self):
        """Test that Collector sets stream_collected=True even on error."""

        async def _test():
            collected_events = []

            class EventCapture(Map):
                def __init__(self, **kwargs):
                    super().__init__(fn=lambda x: x, **kwargs)

                async def _do(self, event):
                    if hasattr(event, "stream_collected"):
                        collected_events.append(event)
                    return await super()._do(event)

            def error_stream(x):
                yield f"{x}_chunk_0"
                raise ValueError("test error")

            source = AsyncEmitSource()
            streaming_map = Map(error_stream)
            collector = Collector()
            capture = EventCapture()
            reducer = Reduce([], lambda acc, x: acc + [x])

            source.to(streaming_map).to(collector).to(capture).to(reducer)

            controller = source.run()

            try:
                await controller.emit("test")
            finally:
                await controller.terminate()
                await controller.await_termination()

            # Verify we captured an event with stream_collected=True and error body
            assert len(collected_events) == 1
            event = collected_events[0]
            assert event.stream_collected is True
            assert isinstance(event.body, dict)
            assert "error" in event.body
            assert "ValueError" in event.body["error"]
            assert "test error" in event.body["error"]

        asyncio.run(_test())

    def test_collector_streaming_error_cleans_up(self):
        """Verify that Collector cleans up _collected_streams after error (no memory leak)."""

        async def _test():

            def error_stream(x):
                yield f"{x}_chunk_0"
                raise ValueError("cleanup test error")

            source = AsyncEmitSource()
            streaming_map = Map(error_stream)
            collector = Collector()
            reducer = Reduce([], lambda acc, x: acc + [x])

            source.to(streaming_map).to(collector).to(reducer)

            controller = source.run()

            try:
                await controller.emit("test")
            finally:
                await controller.terminate()
                await controller.await_termination()

            # Verify collector cleaned up (no memory leak)
            assert len(collector._collected_streams) == 0

        asyncio.run(_test())


class TestCompleteStreaming:
    """Tests for Complete step streaming support."""

    def test_complete_streaming_response(self):
        """Test that Complete pushes StreamChunks for streaming events."""

        num_chunks = 3

        def stream_chunks(x):
            for i in range(num_chunks):
                yield f"{x}_{i}"

        def append_suffix(x):
            return f"{x}_suffix"

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(stream_chunks),
                Map(append_suffix),
                Complete(),
            ]
        ).run()

        try:
            results = []
            for request_idx in range(3):
                awaitable = controller.emit(f"test{request_idx}")
                result = awaitable.await_result()
                # await_result() should return a generator for streaming
                assert inspect.isgenerator(result)
                results.append(result)

            for result_idx, result in enumerate(results):
                assert list(result) == [f"test{result_idx}_{chunk}_suffix" for chunk in range(num_chunks)]
        finally:
            controller.terminate()
            controller.await_termination()

    def test_async_complete_streaming_response(self):
        """Async version: Test that Complete pushes StreamChunks for streaming events."""

        async def _test():
            num_chunks = 3

            def stream_chunks(x):
                for i in range(num_chunks):
                    yield f"{x}_{i}"

            def append_suffix(x):
                return f"{x}_suffix"

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(stream_chunks),
                    Map(append_suffix),
                    Complete(),
                ]
            ).run()

            try:
                results = []
                for request_idx in range(3):
                    # AsyncFlowController.emit() awaits the result internally
                    result = await controller.emit(f"test{request_idx}")
                    # For async, await_result() returns an async generator
                    assert inspect.isasyncgen(result)
                    results.append(result)

                for result_idx, result in enumerate(results):
                    chunks = [chunk async for chunk in result]
                    assert chunks == [f"test{result_idx}_{chunk}_suffix" for chunk in range(num_chunks)]
            finally:
                await controller.terminate()
                await controller.await_termination()

        asyncio.run(_test())

    def test_complete_streaming_empty(self):
        """Test streaming with zero chunks."""

        def empty_stream(x):
            return
            yield  # Make it a generator

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(empty_stream),
                Complete(),
            ]
        ).run()

        awaitable = controller.emit("test")
        controller.terminate()
        controller.await_termination()

        # Empty generator should still work
        result = awaitable.await_result()
        assert list(result) == []

    def test_async_complete_streaming_empty(self):
        """Async version: Test streaming with zero chunks."""

        async def _test():
            def empty_stream(x):
                return
                yield  # Make it a generator

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(empty_stream),
                    Complete(),
                ]
            ).run()

            result = await controller.emit("test")
            await controller.terminate()
            await controller.await_termination()

            # Empty async generator should still work
            chunks = [chunk async for chunk in result]
            assert chunks == []

        asyncio.run(_test())


class TestStreamingErrors:
    """Tests for streaming error conditions."""

    def test_streaming_on_streaming_error(self):
        """Test that streaming on top of streaming raises an error."""

        def stream1(x):
            yield x

        def stream2(x):
            yield x

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(stream1),
                Map(stream2),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()

        with pytest.raises(StreamingError, match="Streaming on top of streaming is not allowed"):
            controller.await_termination()

    def test_streaming_after_collector_ok(self):
        """Test that streaming after a Collector is allowed."""

        def stream1(x):
            for i in range(2):
                yield f"{x}_{i}"

        def stream2(x):
            # x is now a list from collector
            for item in x:
                yield f"re_{item}"

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(stream1),
                Collector(),
                Map(stream2),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        # First stream yields ["test_0", "test_1"], collected
        # Second stream yields "re_test_0", "re_test_1"
        assert result == ["re_test_0", "re_test_1"]

    def test_async_streaming_on_streaming_error(self):
        """Async version: Test that streaming on top of streaming raises an error."""

        async def _test():
            def stream1(x):
                yield x

            def stream2(x):
                yield x

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(stream1),
                    Map(stream2),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("test")
            await controller.terminate()

            with pytest.raises(StreamingError, match="Streaming on top of streaming is not allowed"):
                await controller.await_termination()

        asyncio.run(_test())

    def test_async_streaming_after_collector_ok(self):
        """Async version: Test that streaming after a Collector is allowed."""

        async def _test():
            def stream1(x):
                for i in range(2):
                    yield f"{x}_{i}"

            def stream2(x):
                for item in x:
                    yield f"re_{item}"

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(stream1),
                    Collector(),
                    Map(stream2),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("test")
            await controller.terminate()
            result = await controller.await_termination()

            assert result == ["re_test_0", "re_test_1"]

        asyncio.run(_test())

    def test_streaming_generator_raises_error(self):
        """Test that error in generator mid-stream is delivered to consumer without killing the flow."""

        def error_stream(x):
            yield f"{x}_chunk_0"
            raise ValueError("Generator error mid-stream")

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(error_stream),
                Complete(),
            ]
        ).run()

        try:
            awaitable = controller.emit("test")
            result = awaitable.await_result()

            assert inspect.isgenerator(result)

            # Consumer gets StreamingError wrapping the error message
            chunks = []
            with pytest.raises(StreamingError, match="Generator error mid-stream"):
                for chunk in result:
                    chunks.append(chunk)

            # Verify first chunk was received before error
            assert chunks == ["test_chunk_0"]
        finally:
            controller.terminate()
            controller.await_termination()

    def test_async_streaming_generator_raises_error(self):
        """Async version: Test that error in generator mid-stream is delivered to consumer without killing the flow."""

        async def _test():
            def error_stream(x):
                yield f"{x}_chunk_0"
                raise ValueError("Generator error mid-stream")

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(error_stream),
                    Complete(),
                ]
            ).run()

            try:
                result = await controller.emit("test")

                assert inspect.isasyncgen(result)

                # Consumer gets StreamingError wrapping the error message
                chunks = []
                with pytest.raises(StreamingError, match="Generator error mid-stream"):
                    async for chunk in result:
                        chunks.append(chunk)

                # Verify first chunk was received before error
                assert chunks == ["test_chunk_0"]
            finally:
                await controller.terminate()
                await controller.await_termination()

        asyncio.run(_test())

    def test_streaming_error_in_intermediate_step(self):
        """Test that error in non-streaming step processing chunks propagates correctly."""

        def stream_chunks(x):
            for i in range(3):
                yield i

        def failing_transform(x):
            if x == 1:
                raise RuntimeError("Failed on chunk 1")
            return x * 10

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(stream_chunks),
                Map(failing_transform),
                Complete(),
            ]
        ).run()

        try:
            awaitable = controller.emit("test")
            result = awaitable.await_result()

            assert inspect.isgenerator(result)

            # Collect chunks until error
            chunks = []
            with pytest.raises(RuntimeError, match="Failed on chunk 1"):
                for chunk in result:
                    chunks.append(chunk)

            # Verify first chunk (0 * 10 = 0) was received before error
            assert chunks == [0]
        finally:
            controller.terminate()
            # Error is also propagated through termination
            with pytest.raises(RuntimeError, match="Failed on chunk 1"):
                controller.await_termination()

    def test_async_streaming_error_in_intermediate_step(self):
        """Async version: Test that error in non-streaming step processing chunks propagates."""

        async def _test():
            def stream_chunks(x):
                for i in range(3):
                    yield i

            def failing_transform(x):
                if x == 1:
                    raise RuntimeError("Failed on chunk 1")
                return x * 10

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(stream_chunks),
                    Map(failing_transform),
                    Complete(),
                ]
            ).run()

            try:
                result = await controller.emit("test")

                assert inspect.isasyncgen(result)

                # Collect chunks until error
                chunks = []
                with pytest.raises(RuntimeError, match="Failed on chunk 1"):
                    async for chunk in result:
                        chunks.append(chunk)

                # Verify first chunk (0 * 10 = 0) was received before error
                assert chunks == [0]
            finally:
                await controller.terminate()
                # Error is also propagated through termination
                with pytest.raises(RuntimeError, match="Failed on chunk 1"):
                    await controller.await_termination()

        asyncio.run(_test())

    def test_streaming_in_cycle_fails(self):
        """Test that streaming step inside a cycle fails on second iteration.

        When a streaming step is inside a cycle, the first iteration streams chunks.
        When those chunks loop back to the streaming step, they already have
        streaming_step set, so the step should fail with StreamingError.
        """

        class AlwaysLoop(Map):
            """A Map step that always routes back to the loop target."""

            def __init__(self, loop_target, **kwargs):
                super().__init__(**kwargs)
                self._loop_target = loop_target

            def select_outlets(self, event_body):
                # Always loop back - the streaming error should stop us
                return [self._loop_target]

        def stream_chunks(x):
            yield f"{x}_chunk_0"
            yield f"{x}_chunk_1"

        source = SyncEmitSource()
        # The streaming map is the entry point of the loop
        streaming_map = Map(stream_chunks, name="streamer", max_iterations=5)
        loop_controller = AlwaysLoop(fn=lambda x: x, name="loop_ctrl", loop_target="streamer", max_iterations=5)
        end = Reduce([], lambda acc, x: acc + [x], name="end")

        source.to(streaming_map)
        streaming_map.to(loop_controller)
        loop_controller.to(end)
        loop_controller.to(streaming_map)  # Create cycle

        controller = source.run()

        controller.emit("test")
        controller.terminate()

        # Should fail because chunks looping back already have streaming_step set
        with pytest.raises(StreamingError, match="Streaming on top of streaming is not allowed"):
            controller.await_termination()

    def test_async_streaming_in_cycle_fails(self):
        """Async version: Test that streaming step inside a cycle fails on second iteration."""

        async def _test():
            class AlwaysLoop(Map):
                def __init__(self, loop_target, **kwargs):
                    super().__init__(**kwargs)
                    self._loop_target = loop_target

                def select_outlets(self, event_body):
                    return [self._loop_target]

            def stream_chunks(x):
                yield f"{x}_chunk_0"
                yield f"{x}_chunk_1"

            source = AsyncEmitSource()
            streaming_map = Map(stream_chunks, name="streamer", max_iterations=5)
            loop_controller = AlwaysLoop(fn=lambda x: x, name="loop_ctrl", loop_target="streamer", max_iterations=5)
            end = Reduce([], lambda acc, x: acc + [x], name="end")

            source.to(streaming_map)
            streaming_map.to(loop_controller)
            loop_controller.to(end)
            loop_controller.to(streaming_map)  # Create cycle

            controller = source.run()

            await controller.emit("test")
            await controller.terminate()

            with pytest.raises(StreamingError, match="Streaming on top of streaming is not allowed"):
                await controller.await_termination()

        asyncio.run(_test())

    def test_streaming_with_multiple_runnables_raises_error(self):
        """Test that streaming raises an error when multiple runnables are selected."""
        streaming = StreamingRunnable(name="streamer")
        non_streaming = NonStreamingRunnable(name="non_streamer")

        controller = build_flow(
            [
                SyncEmitSource(),
                ParallelExecution(
                    runnables=[streaming, non_streaming],
                    execution_mechanism_by_runnable_name={
                        "streamer": ParallelExecutionMechanisms.naive,
                        "non_streamer": ParallelExecutionMechanisms.naive,
                    },
                ),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        try:
            controller.emit("test")
        finally:
            controller.terminate()
            with pytest.raises(StreamingError, match="Streaming is not supported when multiple runnables are selected"):
                controller.await_termination()


class TestStreamingWithIntermediateSteps:
    """Tests for streaming through intermediate non-streaming steps."""

    def test_streaming_through_map(self):
        """Test that streaming chunks flow through non-streaming Map."""

        def stream_chunks(x):
            for i in range(2):
                yield i

        def double(x):
            return x * 2

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(stream_chunks),
                Map(double),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("ignored")
        controller.terminate()
        result = controller.await_termination()

        assert result == [0, 2]

    def test_async_streaming_through_map(self):
        """Async version: Test that streaming chunks flow through non-streaming Map."""

        async def _test():
            def stream_chunks(x):
                for i in range(2):
                    yield i

            def double(x):
                return x * 2

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(stream_chunks),
                    Map(double),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("ignored")
            await controller.terminate()
            result = await controller.await_termination()

            assert result == [0, 2]

        asyncio.run(_test())


class TestParallelExecutionStreaming:
    """Tests for ParallelExecution streaming support."""

    def test_parallel_execution_async_runnable_streaming(self):
        """Test streaming with an async runnable."""
        runnable = AsyncStreamingRunnable(name="async_streamer")
        controller = build_flow(
            [
                SyncEmitSource(),
                ParallelExecution(
                    runnables=[runnable],
                    execution_mechanism_by_runnable_name={"async_streamer": ParallelExecutionMechanisms.asyncio},
                ),
                Complete(),
            ]
        ).run()

        try:
            awaitable = controller.emit("test")
            result = awaitable.await_result()
            assert inspect.isgenerator(result)
            assert list(result) == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]
        finally:
            controller.terminate()
            controller.await_termination()

    def test_async_parallel_execution_single_runnable_streaming(self):
        """Async version: Test streaming with a single runnable."""

        async def _test():
            runnable = StreamingRunnable(name="streamer")
            controller = build_flow(
                [
                    AsyncEmitSource(),
                    ParallelExecution(
                        runnables=[runnable],
                        execution_mechanism_by_runnable_name={"streamer": ParallelExecutionMechanisms.naive},
                    ),
                    Complete(),
                ]
            ).run()

            try:
                result = await controller.emit("test")
                assert inspect.isasyncgen(result)
                chunks = [chunk async for chunk in result]
                assert chunks == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]
            finally:
                await controller.terminate()
                await controller.await_termination()

        asyncio.run(_test())

    def test_async_parallel_execution_async_runnable_streaming(self):
        """Async version: Test streaming with an async runnable."""

        async def _test():
            runnable = AsyncStreamingRunnable(name="async_streamer")
            controller = build_flow(
                [
                    AsyncEmitSource(),
                    ParallelExecution(
                        runnables=[runnable],
                        execution_mechanism_by_runnable_name={"async_streamer": ParallelExecutionMechanisms.asyncio},
                    ),
                    Complete(),
                ]
            ).run()

            try:
                result = await controller.emit("test")
                assert inspect.isasyncgen(result)
                chunks = [chunk async for chunk in result]
                assert chunks == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]
            finally:
                await controller.terminate()
                await controller.await_termination()

        asyncio.run(_test())

    @pytest.mark.parametrize(
        "execution_mechanism",
        [
            ParallelExecutionMechanisms.naive,
            ParallelExecutionMechanisms.thread_pool,
            ParallelExecutionMechanisms.process_pool,
            ParallelExecutionMechanisms.dedicated_process,
        ],
    )
    def test_parallel_execution_streaming_with_executor(self, execution_mechanism):
        """Test streaming works with various execution mechanisms."""
        runnable = StreamingRunnable(name="streamer")
        controller = build_flow(
            [
                SyncEmitSource(),
                ParallelExecution(
                    runnables=[runnable],
                    execution_mechanism_by_runnable_name={"streamer": execution_mechanism},
                ),
                Complete(),
            ]
        ).run()

        try:
            awaitable = controller.emit("test")
            result = awaitable.await_result()
            assert inspect.isgenerator(result)
            assert list(result) == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]
        finally:
            controller.terminate()
            controller.await_termination()

    @pytest.mark.parametrize(
        "execution_mechanism",
        [
            ParallelExecutionMechanisms.thread_pool,
            ParallelExecutionMechanisms.process_pool,
            ParallelExecutionMechanisms.dedicated_process,
        ],
    )
    def test_parallel_execution_streaming_with_shared_executor(self, execution_mechanism):
        """Test streaming works with shared_executor using different underlying mechanisms."""
        shared_executor = RunnableExecutor()
        shared_runnable = StreamingRunnable(name="shared_streamer")
        shared_executor.add_runnable(shared_runnable, execution_mechanism)

        proxy_runnable = StreamingRunnable(name="proxy", shared_runnable_name="shared_streamer")

        class ContextWithExecutor:
            def __init__(self, executor):
                self.executor = executor

        context = ContextWithExecutor(shared_executor)

        controller = build_flow(
            [
                SyncEmitSource(),
                ParallelExecution(
                    runnables=[proxy_runnable],
                    execution_mechanism_by_runnable_name={"proxy": ParallelExecutionMechanisms.shared_executor},
                    context=context,
                ),
                Complete(),
            ]
        ).run()

        try:
            awaitable = controller.emit("test")
            result = awaitable.await_result()
            assert inspect.isgenerator(result)
            assert list(result) == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]
        finally:
            controller.terminate()
            controller.await_termination()

    @pytest.mark.parametrize(
        "execution_mechanism",
        [
            ParallelExecutionMechanisms.naive,
            ParallelExecutionMechanisms.thread_pool,
            ParallelExecutionMechanisms.process_pool,
            ParallelExecutionMechanisms.dedicated_process,
        ],
    )
    def test_parallel_execution_streaming_error_propagation(self, execution_mechanism):
        """Test that streaming errors are delivered to consumer without killing the flow."""
        runnable = ErrorStreamingRunnable(name="error_streamer")
        controller = build_flow(
            [
                SyncEmitSource(),
                ParallelExecution(
                    runnables=[runnable],
                    execution_mechanism_by_runnable_name={"error_streamer": execution_mechanism},
                ),
                Complete(),
            ]
        ).run()

        try:
            awaitable = controller.emit("test")
            result = awaitable.await_result()
            assert inspect.isgenerator(result)
            # Consumer gets StreamingError wrapping the error message
            chunks = []
            with pytest.raises(StreamingError, match="Simulated streaming error"):
                for chunk in result:
                    chunks.append(chunk)
            # Verify we got the first chunk before the error
            assert chunks == ["test_chunk_0"]
        finally:
            controller.terminate()
            controller.await_termination()

    def test_parallel_execution_streaming_single_runnable_sets_metadata(self):
        """Test that streaming ParallelExecution with single runnable sets timing metadata.

        This mirrors the non-streaming behavior where _metadata includes 'when' and 'microsec'.
        After Collector aggregates chunks, the collected event should have timing metadata.
        The 'microsec' field should contain the total streaming duration calculated by Collector.
        """
        runnable = StreamingRunnable(name="streamer")
        controller = build_flow(
            [
                SyncEmitSource(),
                ParallelExecution(
                    runnables=[runnable],
                    execution_mechanism_by_runnable_name={"streamer": ParallelExecutionMechanisms.naive},
                ),
                Collector(),
                Reduce([], lambda acc, x: acc + [x], full_event=True),
            ]
        ).run()

        try:
            controller.emit("test")
        finally:
            controller.terminate()
            result = controller.await_termination()

        assert len(result) == 1
        event = result[0]
        assert hasattr(event, "_metadata"), "Expected event to have _metadata attribute"
        metadata = event._metadata
        assert "when" in metadata, "Expected _metadata to include 'when' field"
        assert "microsec" in metadata, "Expected _metadata to include 'microsec' field"
        # Verify 'when' is a valid ISO timestamp string
        assert isinstance(metadata["when"], str), "Expected 'when' to be a string"
        # Verify 'microsec' is a positive integer (total streaming duration calculated by Collector)
        assert isinstance(metadata["microsec"], int), "Expected 'microsec' to be an integer"
        assert metadata["microsec"] >= 0, "Expected 'microsec' to be non-negative"

    @pytest.mark.parametrize(
        "execution_mechanism",
        [
            ParallelExecutionMechanisms.process_pool,
            ParallelExecutionMechanisms.dedicated_process,
        ],
    )
    def test_multiple_streaming_runnables_with_process_raises_streaming_error(self, execution_mechanism):
        """Regression test for ML-12205: selecting multiple streaming runnables
        with process_pool or dedicated_process must raise StreamingError, not AttributeError.

        Before the fix, process-based streaming returned a raw async generator instead of
        a _StreamingResult. The multi-runnable streaming guard (isinstance check for
        _StreamingResult) didn't match, so code fell through to the non-streaming path
        which tried to access .runnable_name on the async generator, producing:
            AttributeError: 'async_generator' object has no attribute 'runnable_name'
        """
        streamer1 = StreamingRunnable(name="streamer1")
        streamer2 = StreamingRunnable(name="streamer2")

        controller = build_flow(
            [
                SyncEmitSource(),
                ParallelExecution(
                    runnables=[streamer1, streamer2],
                    execution_mechanism_by_runnable_name={
                        "streamer1": execution_mechanism,
                        "streamer2": execution_mechanism,
                    },
                ),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        try:
            controller.emit("test")
        finally:
            controller.terminate()
            with pytest.raises(StreamingError, match="Streaming is not supported when multiple runnables are selected"):
                controller.await_termination()


class TestStreamingGraphSplits:
    """Tests for streaming through branching graph topologies."""

    def test_streaming_graph_split_collector_expected_completions_2(self):
        """Test streaming through a split with Collector(expected_completions=2)."""

        def stream_chunks(x):
            for i in range(2):
                yield f"{x}_chunk_{i}"

        source = SyncEmitSource()
        streaming_map = Map(stream_chunks)
        branch_a = Map(lambda x: f"a_{x}", name="branch_a")
        branch_b = Map(lambda x: f"b_{x}", name="branch_b")
        collector = Collector(expected_completions=2)
        reducer = Reduce([], lambda acc, x: acc + [x])

        source.to(streaming_map)
        streaming_map.to(branch_a).to(collector)
        streaming_map.to(branch_b).to(collector)
        collector.to(reducer)

        controller = source.run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        # Collector should receive chunks from both branches and emit collected list
        # Each branch processes each chunk, so we get 4 items total (2 chunks x 2 branches)
        assert set(result[0]) == {"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}
        assert len(result) == 1

    def test_async_streaming_graph_split_collector_expected_completions_2(self):
        """Async version: Test streaming through a split with Collector(expected_completions=2)."""

        async def _test():
            def stream_chunks(x):
                for i in range(2):
                    yield f"{x}_chunk_{i}"

            source = AsyncEmitSource()
            streaming_map = Map(stream_chunks)
            branch_a = Map(lambda x: f"a_{x}", name="branch_a")
            branch_b = Map(lambda x: f"b_{x}", name="branch_b")
            collector = Collector(expected_completions=2)
            reducer = Reduce([], lambda acc, x: acc + [x])

            source.to(streaming_map)
            streaming_map.to(branch_a).to(collector)
            streaming_map.to(branch_b).to(collector)
            collector.to(reducer)

            controller = source.run()

            await controller.emit("test")
            await controller.terminate()
            result = await controller.await_termination()

            assert set(result[0]) == {"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}
            assert len(result) == 1

        asyncio.run(_test())

    def test_streaming_graph_split_complete_expected_results_2(self):
        """Test streaming through a split with Complete and expected_number_of_results=2."""

        def stream_chunks(x):
            for i in range(2):
                yield f"{x}_chunk_{i}"

        source = SyncEmitSource()
        streaming_map = Map(stream_chunks)
        branch_a = Map(lambda x: f"a_{x}", name="branch_a")
        branch_b = Map(lambda x: f"b_{x}", name="branch_b")
        complete_a = Complete(name="complete_a")
        complete_b = Complete(name="complete_b")

        source.to(streaming_map)
        streaming_map.to(branch_a).to(complete_a)
        streaming_map.to(branch_b).to(complete_b)

        controller = source.run()

        try:
            # Use expected_number_of_results=2 since event goes through 2 Complete steps
            awaitable = controller.emit("test", expected_number_of_results=2)
            result = awaitable.await_result()

            # Should be a generator yielding chunks from both branches
            assert inspect.isgenerator(result)
            chunks = list(result)

            # Should have 4 chunks total (2 chunks x 2 branches)
            assert set(chunks) == {"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}
        finally:
            controller.terminate()
            controller.await_termination()

    def test_async_streaming_graph_split_complete_expected_results_2(self):
        """Async version: Test streaming through a split with Complete and expected_number_of_results=2."""

        async def _test():
            def stream_chunks(x):
                for i in range(2):
                    yield f"{x}_chunk_{i}"

            source = AsyncEmitSource()
            streaming_map = Map(stream_chunks)
            branch_a = Map(lambda x: f"a_{x}", name="branch_a")
            branch_b = Map(lambda x: f"b_{x}", name="branch_b")
            complete_a = Complete(name="complete_a")
            complete_b = Complete(name="complete_b")

            source.to(streaming_map)
            streaming_map.to(branch_a).to(complete_a)
            streaming_map.to(branch_b).to(complete_b)

            controller = source.run()

            try:
                result = await controller.emit("test", expected_number_of_results=2)

                assert inspect.isasyncgen(result)
                chunks = [chunk async for chunk in result]

                assert set(chunks) == {"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}
            finally:
                await controller.terminate()
                await controller.await_termination()

        asyncio.run(_test())

    def test_streaming_diamond_graph_single_complete(self):
        """Test diamond graph: streaming splits, branches merge into single Complete."""

        def stream_chunks(x):
            for i in range(2):
                yield f"{x}_chunk_{i}"

        source = SyncEmitSource()
        streaming_map = Map(stream_chunks)
        branch_a = Map(lambda x: f"a_{x}", name="branch_a")
        branch_b = Map(lambda x: f"b_{x}", name="branch_b")
        complete = Complete(name="complete")

        # Diamond: streaming_map splits to both branches, both merge into single complete
        source.to(streaming_map)
        streaming_map.to(branch_a).to(complete)
        streaming_map.to(branch_b).to(complete)

        controller = source.run()

        try:
            # expected_number_of_results=2 because event passes through Complete twice (once per branch)
            awaitable = controller.emit("test", expected_number_of_results=2)
            result = awaitable.await_result()

            assert inspect.isgenerator(result)
            chunks = list(result)

            # Should have 4 chunks total (2 chunks x 2 branches)
            assert set(chunks) == {"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}
        finally:
            controller.terminate()
            controller.await_termination()

    def test_async_streaming_diamond_graph_single_complete(self):
        """Async version: Test diamond graph with single Complete at merge point."""

        async def _test():
            def stream_chunks(x):
                for i in range(2):
                    yield f"{x}_chunk_{i}"

            source = AsyncEmitSource()
            streaming_map = Map(stream_chunks)
            branch_a = Map(lambda x: f"a_{x}", name="branch_a")
            branch_b = Map(lambda x: f"b_{x}", name="branch_b")
            complete = Complete(name="complete")

            source.to(streaming_map)
            streaming_map.to(branch_a).to(complete)
            streaming_map.to(branch_b).to(complete)

            controller = source.run()

            try:
                result = await controller.emit("test", expected_number_of_results=2)

                assert inspect.isasyncgen(result)
                chunks = [chunk async for chunk in result]

                assert set(chunks) == {"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}
            finally:
                await controller.terminate()
                await controller.await_termination()

        asyncio.run(_test())

    def test_streaming_select_outlets_single_branch(self):
        """Test streaming with selective routing to single branch."""

        def stream_chunks(x):
            for i in range(2):
                yield f"{x}_chunk_{i}"

        class RouteChoice(Choice):
            def select_outlets(self, event):
                # Route based on chunk content
                if "high" in str(event):
                    return ["branch_high"]
                else:
                    return ["branch_low"]

        source = SyncEmitSource()
        streaming_map = Map(stream_chunks)
        route = RouteChoice()
        branch_high = Map(lambda x: f"HIGH_{x}", name="branch_high")
        branch_low = Map(lambda x: f"LOW_{x}", name="branch_low")
        collector = Collector(expected_completions=1)
        reducer = Reduce([], lambda acc, x: acc + [x])

        source.to(streaming_map).to(route)
        route.to(branch_high).to(collector)
        route.to(branch_low).to(collector)
        collector.to(reducer)

        controller = source.run()

        controller.emit("low_value")
        controller.emit("high_value")
        controller.terminate()
        result = controller.await_termination()

        # 4 results: chunks go to one branch, but StreamCompletion goes to all branches
        # (like _termination_obj) to avoid hangs in cyclic graphs.
        # Branches that don't receive chunks emit empty lists.
        assert len(result) == 4
        low_result = [r for r in result if r and any("LOW_" in str(item) for item in r)]
        high_result = [r for r in result if r and any("HIGH_" in str(item) for item in r)]
        empty_results = [r for r in result if r == []]
        assert len(low_result) == 1
        assert len(high_result) == 1
        assert len(empty_results) == 2
        assert "LOW_low_value_chunk_0" in low_result[0]
        assert "HIGH_high_value_chunk_0" in high_result[0]

    def test_async_streaming_select_outlets_single_branch(self):
        """Async version: Test streaming with selective routing to single branch."""

        async def _test():
            def stream_chunks(x):
                for i in range(2):
                    yield f"{x}_chunk_{i}"

            class RouteChoice(Choice):
                def select_outlets(self, event):
                    if "high" in str(event):
                        return ["branch_high"]
                    else:
                        return ["branch_low"]

            source = AsyncEmitSource()
            streaming_map = Map(stream_chunks)
            route = RouteChoice()
            branch_high = Map(lambda x: f"HIGH_{x}", name="branch_high")
            branch_low = Map(lambda x: f"LOW_{x}", name="branch_low")
            collector = Collector(expected_completions=1)
            reducer = Reduce([], lambda acc, x: acc + [x])

            source.to(streaming_map).to(route)
            route.to(branch_high).to(collector)
            route.to(branch_low).to(collector)
            collector.to(reducer)

            controller = source.run()

            await controller.emit("low_value")
            await controller.emit("high_value")
            await controller.terminate()
            result = await controller.await_termination()

            # 4 results: chunks go to one branch, but StreamCompletion goes to all branches
            assert len(result) == 4
            low_result = [r for r in result if r and any("LOW_" in str(item) for item in r)]
            high_result = [r for r in result if r and any("HIGH_" in str(item) for item in r)]
            empty_results = [r for r in result if r == []]
            assert len(low_result) == 1
            assert len(high_result) == 1
            assert len(empty_results) == 2
            assert "LOW_low_value_chunk_0" in low_result[0]
            assert "HIGH_high_value_chunk_0" in high_result[0]

        asyncio.run(_test())


class TestVerboseLoggingWithStreamCompletion:
    """Tests for verbose logging with StreamCompletion events."""

    def test_event_string_with_stream_completion(self):
        """Test that _event_string handles StreamCompletion objects correctly.

        The _event_string method is called during verbose logging in _do_downstream.
        It must handle StreamCompletion objects which have a body property that
        delegates to original_event.body.
        """

        event = Event(body="test_body", id="event_123", key="test_key")
        completion = StreamCompletion("streaming_step", event)

        # _event_string should handle StreamCompletion without error
        result = Flow._event_string(completion)

        # The result should contain the event id from original_event
        assert "event_123" in result
        assert isinstance(result, str)

    def test_verbose_logging_with_streaming_flow(self):
        """Test verbose logging when StreamCompletion passes through a flow.

        This integration test verifies that when verbose=True, the flow logs
        debug messages for StreamCompletion events without errors.
        """

        def stream_chunks(x):
            for i in range(2):
                yield f"{x}_chunk_{i}"

        logger = MockLogger()
        context = MockContext(logger, verbose=True)

        controller = build_flow(
            [
                SyncEmitSource(context=context),
                Map(stream_chunks, name="StreamingMap", context=context),
                Collector(name="Collector", context=context),
                Reduce([], lambda acc, x: acc + [x], name="Reducer", context=context),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        # Verify the flow completed successfully
        assert len(result) == 1
        assert result[0] == ["test_chunk_0", "test_chunk_1"]

        # Verify debug logs were recorded (verbose logging was active)
        debug_logs = [log for log in logger.logs if log[0] == "debug"]
        assert len(debug_logs) > 0

        # Verify that StreamCompletion was logged - it should appear in at least one log entry
        # since StreamCompletion goes through _do_downstream when verbose is True
        all_log_messages = " ".join(str(log[1]) for log in logger.logs)
        # The logs should contain references to the step names showing flow progression
        assert "StreamingMap" in all_log_messages or "Collector" in all_log_messages


class TestConcurrentExecutionStreaming:
    """Tests for ConcurrentExecution streaming support (ML-12178).

    ConcurrentExecution should handle async generator process_event functions
    the same way Map handles generator functions -- by emitting streaming chunks
    and StreamCompletion, so that a downstream Collector can aggregate them.
    """

    @pytest.mark.parametrize("use_async_generator", [False, True], ids=["sync_gen", "async_gen"])
    def test_concurrent_execution_generator_with_collector(self, use_async_generator):
        """Reproducer for ML-12178: ConcurrentExecution with generator -> Collector -> downstream.

        When process_event is a generator (sync or async), ConcurrentExecution should emit
        streaming chunks so the Collector can aggregate them into a list. Without
        the fix, the Collector receives a raw generator object instead.
        """

        if use_async_generator:

            async def stream_chunks(event):
                for i in range(3):
                    yield f"{event}_chunk_{i}"

        else:

            def stream_chunks(event):
                for i in range(3):
                    yield f"{event}_chunk_{i}"

        controller = build_flow(
            [
                SyncEmitSource(),
                ConcurrentExecution(stream_chunks),
                Collector(),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        try:
            controller.emit("test")
        finally:
            controller.terminate()
            result = controller.await_termination()

        assert len(result) == 1
        collected = result[0]
        assert isinstance(
            collected, list
        ), f"Expected collected chunks as a list, got {type(collected).__name__}: {collected}"
        assert collected == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

    def test_concurrent_execution_generator_then_streaming_step(self):
        """Full ML-12178 scenario: ConcurrentExecution -> Collector -> second streaming step.

        The second streaming step should receive the collected list, not a generator object.
        """

        async def first_stream(event):
            for i in range(2):
                yield f"{event}_{i}"

        def second_stream(collected):
            for item in collected:
                yield f"re_{item}"

        controller = build_flow(
            [
                SyncEmitSource(),
                ConcurrentExecution(first_stream),
                Collector(),
                Map(second_stream),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        try:
            controller.emit("test")
        finally:
            controller.terminate()
            result = controller.await_termination()

        assert result == ["re_test_0", "re_test_1"]

    # -- ML-12378 concurrency tests ----------------------------------------
    # Verify that streaming generators run concurrently (not serially) when
    # max_in_flight > 1, across execution mechanisms.

    def _assert_streaming_results(self, result, n_events, n_chunks):
        """Check all events were collected with the correct chunks (order-independent)."""
        assert len(result) == n_events, f"Expected {n_events} collected events, got {len(result)}"
        expected = {tuple(f"event_{i}_chunk_{j}" for j in range(n_chunks)) for i in range(n_events)}
        actual = {tuple(collected) for collected in result}
        assert actual == expected, f"Unexpected results: {actual} != {expected}"

    def test_concurrent_streaming_asyncio_async_gen(self):
        """Default (asyncio) mechanism + async generator.

        Tracks max simultaneously-active generators.  ``await asyncio.sleep(0)``
        between yields simulates realistic I/O and gives the event loop a
        chance to schedule other generator tasks.
        """

        n_chunks = 3
        n_events = 4

        async def _run():
            active = 0
            max_active = 0

            async def stream_chunks(event):
                nonlocal active, max_active
                active += 1
                if active > max_active:
                    max_active = active
                try:
                    for i in range(n_chunks):
                        await asyncio.sleep(0)
                        yield f"{event}_chunk_{i}"
                finally:
                    active -= 1

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    ConcurrentExecution(stream_chunks, max_in_flight=n_events),
                    Collector(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            for i in range(n_events):
                await controller.emit(f"event_{i}")

            await controller.terminate()
            result = await controller.await_termination()

            self._assert_streaming_results(result, n_events, n_chunks)
            assert max_active > 1, (
                f"Generators not concurrent: max active was {max_active}, "
                f"expected > 1 with max_in_flight={n_events}"
            )

        asyncio.run(_run())

    @pytest.mark.parametrize(
        "mechanism, expect_concurrent",
        [
            ("thread_pool", True),
            ("process_pool", True),
            ("dedicated_process", False),  # single worker — generators serialize
            # shared_executor omitted: requires an executor= parameter not yet
            # supported by ConcurrentExecution.
            ("naive", False),
        ],
    )
    def test_concurrent_streaming_sync_gen(self, mechanism, expect_concurrent):
        """Sync generator across execution mechanisms.

        Uses a module-level function with ``time.sleep`` per event to
        simulate blocking work.  For mechanisms that support concurrency the
        total elapsed time must be well below the serial estimate.  For naive
        and dedicated_process (single worker) only correctness is checked.
        """

        n_events = 4
        chunk_delay = _SYNC_STREAMING_DELAY

        async def _run():
            controller = build_flow(
                [
                    AsyncEmitSource(),
                    ConcurrentExecution(
                        _sync_streaming_fn,
                        concurrency_mechanism=mechanism,
                        max_in_flight=n_events,
                    ),
                    Collector(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            start = time.monotonic()
            for i in range(n_events):
                await controller.emit(f"event_{i}")

            await controller.terminate()
            result = await controller.await_termination()
            elapsed = time.monotonic() - start

            self._assert_streaming_results(result, n_events, 3)

            if expect_concurrent:
                serial_duration = chunk_delay * n_events
                assert elapsed < serial_duration * 0.75, (
                    f"{mechanism} streaming serialized: {elapsed:.2f}s " f"vs serial estimate {serial_duration:.2f}s"
                )

        asyncio.run(_run())

    # -- Error handling tests --------------------------------------------------
    # Verify that generator errors in ConcurrentExecution are propagated
    # correctly through _iterate_generator → _GeneratorDone → Collector,
    # producing an error dict rather than killing the flow.

    def test_concurrent_streaming_error_asyncio_async_gen(self):
        """Async generator raising mid-stream via asyncio mechanism.

        Chunks emitted before the error should be collected, and the
        Collector should emit an error dict for the failed stream.
        """

        async def error_stream(event):
            yield f"{event}_chunk_0"
            raise ValueError("async generator error mid-stream")

        async def _run():
            controller = build_flow(
                [
                    AsyncEmitSource(),
                    ConcurrentExecution(error_stream),
                    Collector(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            try:
                await controller.emit("test")
            finally:
                await controller.terminate()
                result = await controller.await_termination()

            assert len(result) == 1
            assert isinstance(result[0], dict)
            assert "error" in result[0]
            assert "ValueError" in result[0]["error"]
            assert "async generator error mid-stream" in result[0]["error"]

        asyncio.run(_run())

    @pytest.mark.parametrize(
        "mechanism",
        [
            "thread_pool",
            "process_pool",
            "dedicated_process",
            "naive",
        ],
    )
    def test_concurrent_streaming_error_sync_gen(self, mechanism):
        """Sync generator raising mid-stream across executor mechanisms.

        Uses the module-level _sync_error_streaming_fn so it is picklable
        for process-based mechanisms.
        """

        async def _run():
            controller = build_flow(
                [
                    AsyncEmitSource(),
                    ConcurrentExecution(
                        _sync_error_streaming_fn,
                        concurrency_mechanism=mechanism,
                    ),
                    Collector(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            try:
                await controller.emit("test")
            finally:
                await controller.terminate()
                result = await controller.await_termination()

            assert len(result) == 1
            assert isinstance(result[0], dict)
            assert "error" in result[0]
            error_str = result[0]["error"]
            assert "ValueError" in error_str
            assert "sync generator error mid-stream" in error_str

        asyncio.run(_run())

    def test_concurrent_streaming_error_mixed_with_healthy(self):
        """One event's generator fails while others succeed.

        With max_in_flight > 1, a single failing generator must not
        prevent healthy events from completing successfully.
        """

        async def maybe_error_stream(event):
            yield f"{event}_chunk_0"
            await asyncio.sleep(0)
            if event == "event_bad":
                raise ValueError("bad event error")
            yield f"{event}_chunk_1"

        async def _run():
            controller = build_flow(
                [
                    AsyncEmitSource(),
                    ConcurrentExecution(maybe_error_stream, max_in_flight=4),
                    Collector(),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("event_ok_1")
            await controller.emit("event_bad")
            await controller.emit("event_ok_2")

            await controller.terminate()
            result = await controller.await_termination()

            assert len(result) == 3

            error_results = [r for r in result if isinstance(r, dict) and "error" in r]
            assert len(error_results) == 1
            assert "ValueError" in error_results[0]["error"]
            assert "bad event error" in error_results[0]["error"]

            ok_results = [r for r in result if isinstance(r, list)]
            assert len(ok_results) == 2
            ok_chunks = {tuple(r) for r in ok_results}
            assert ok_chunks == {
                ("event_ok_1_chunk_0", "event_ok_1_chunk_1"),
                ("event_ok_2_chunk_0", "event_ok_2_chunk_1"),
            }

        asyncio.run(_run())


class TestSyncGeneratorEventLoopBlocking:
    def test_sync_generator_does_not_block_event_loop(self):
        """A sync generator with time.sleep() must not starve the event loop.

        Reproduces ML-12203: when em=naive or em=thread_pool, a blocking sync
        generator in _emit_streaming_chunks prevents the event loop from flushing
        HTTP chunks, causing them to be concatenated.
        """

        async def _test():
            sleep_duration = 0.15
            num_chunks = 3
            concurrent_ticks = []
            streaming_done = asyncio.Event()

            def slow_generator(x):
                for i in range(num_chunks):
                    time.sleep(sleep_duration)
                    yield f"{x}_chunk_{i}"

            async def concurrent_task():
                tick_interval = sleep_duration / 4
                while not streaming_done.is_set():
                    concurrent_ticks.append(time.monotonic())
                    await asyncio.sleep(tick_interval)

            source = AsyncEmitSource()
            source.to(Map(slow_generator)).to(Reduce([], lambda acc, x: acc + [x]))
            controller = source.run()

            concurrent = asyncio.create_task(concurrent_task())
            try:
                await controller.emit("test")
            finally:
                await controller.terminate()
                result = await controller.await_termination()

            streaming_done.set()
            await concurrent

            assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]
            assert len(concurrent_ticks) >= num_chunks * 2, (
                f"Event loop was blocked: only {len(concurrent_ticks)} ticks "
                f"during {sleep_duration * num_chunks:.2f}s of generator sleeps"
            )

        asyncio.run(_test())
