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

import pytest

from storey import (
    AsyncEmitSource,
    Collector,
    Complete,
    Map,
    MapClass,
    ParallelExecution,
    ParallelExecutionMechanisms,
    ParallelExecutionRunnable,
    Reduce,
    StreamingError,
    SyncEmitSource,
    build_flow,
)
from storey.dtypes import Event, StreamChunk, StreamCompletion
from storey.flow import _StreamingStepMixin


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


class TestStreamingStepMixin:
    """Tests for the _StreamingStepMixin utility methods."""

    def test_is_generator_sync(self):
        def gen():
            yield 1
            yield 2

        assert _StreamingStepMixin._is_generator(gen())

    def test_is_generator_async(self):
        async def async_gen():
            yield 1
            yield 2

        assert _StreamingStepMixin._is_generator(async_gen())

    def test_is_generator_non_generator(self):
        assert not _StreamingStepMixin._is_generator([1, 2, 3])
        assert not _StreamingStepMixin._is_generator("string")
        assert not _StreamingStepMixin._is_generator(42)

    def test_is_generator_coroutine(self):
        async def coro():
            return 1

        # Coroutine is not a generator
        c = coro()
        try:
            assert not _StreamingStepMixin._is_generator(c)
        finally:
            c.close()


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

    def test_collector_single_chunk_unwrap(self):
        """Test that a single chunk is unwrapped by Collector."""

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
                awaitable = (controller.emit(f"test{request_idx}"))
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

    def test_parallel_execution_single_runnable_streaming(self):
        """Test streaming with a single runnable."""

        class StreamingRunnable(ParallelExecutionRunnable):
            def run(self, body, path, origin_name=None):
                for i in range(3):
                    yield f"{body}_chunk_{i}"

        runnable = StreamingRunnable(name="streamer")
        controller = build_flow(
            [
                SyncEmitSource(),
                ParallelExecution(
                    runnables=[runnable],
                    execution_mechanism_by_runnable_name={"streamer": ParallelExecutionMechanisms.naive},
                ),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

    def test_parallel_execution_async_runnable_streaming(self):
        """Test streaming with an async runnable."""

        class AsyncStreamingRunnable(ParallelExecutionRunnable):
            async def run_async(self, body, path, origin_name=None):
                for i in range(3):
                    yield f"{body}_chunk_{i}"

        runnable = AsyncStreamingRunnable(name="async_streamer")
        controller = build_flow(
            [
                SyncEmitSource(),
                ParallelExecution(
                    runnables=[runnable],
                    execution_mechanism_by_runnable_name={"async_streamer": ParallelExecutionMechanisms.asyncio},
                ),
                Reduce([], lambda acc, x: acc + [x]),
            ]
        ).run()

        controller.emit("test")
        controller.terminate()
        result = controller.await_termination()

        assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

    def test_async_parallel_execution_single_runnable_streaming(self):
        """Async version: Test streaming with a single runnable."""

        async def _test():
            class StreamingRunnable(ParallelExecutionRunnable):
                def run(self, body, path, origin_name=None):
                    for i in range(3):
                        yield f"{body}_chunk_{i}"

            runnable = StreamingRunnable(name="streamer")
            controller = build_flow(
                [
                    AsyncEmitSource(),
                    ParallelExecution(
                        runnables=[runnable],
                        execution_mechanism_by_runnable_name={"streamer": ParallelExecutionMechanisms.naive},
                    ),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("test")
            await controller.terminate()
            result = await controller.await_termination()

            assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

        asyncio.run(_test())

    def test_async_parallel_execution_async_runnable_streaming(self):
        """Async version: Test streaming with an async runnable."""

        async def _test():
            class AsyncStreamingRunnable(ParallelExecutionRunnable):
                async def run_async(self, body, path, origin_name=None):
                    for i in range(3):
                        yield f"{body}_chunk_{i}"

            runnable = AsyncStreamingRunnable(name="async_streamer")
            controller = build_flow(
                [
                    AsyncEmitSource(),
                    ParallelExecution(
                        runnables=[runnable],
                        execution_mechanism_by_runnable_name={"async_streamer": ParallelExecutionMechanisms.asyncio},
                    ),
                    Reduce([], lambda acc, x: acc + [x]),
                ]
            ).run()

            await controller.emit("test")
            await controller.terminate()
            result = await controller.await_termination()

            assert result == ["test_chunk_0", "test_chunk_1", "test_chunk_2"]

        asyncio.run(_test())


class TestAwaitableResultStreaming:
    """Tests for AwaitableResult streaming support."""

    def test_awaitable_result_stream_generator(self):
        """Test that await_result() returns a generator for streaming."""

        def stream(x):
            for i in range(3):
                yield f"chunk_{i}"

        controller = build_flow(
            [
                SyncEmitSource(),
                Map(stream),
                Complete(),
            ]
        ).run()

        awaitable = controller.emit("test")
        controller.terminate()
        controller.await_termination()

        result = awaitable.await_result()
        assert inspect.isgenerator(result)
        assert list(result) == ["chunk_0", "chunk_1", "chunk_2"]

    def test_awaitable_result_stream_empty(self):
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

    def test_async_awaitable_result_stream_generator(self):
        """Async version: Test that await_result() returns an async generator for streaming."""

        async def _test():
            def stream(x):
                for i in range(3):
                    yield f"chunk_{i}"

            controller = build_flow(
                [
                    AsyncEmitSource(),
                    Map(stream),
                    Complete(),
                ]
            ).run()

            # AsyncFlowController.emit() returns result directly
            result = await controller.emit("test")
            await controller.terminate()
            await controller.await_termination()

            assert inspect.isasyncgen(result)
            chunks = [chunk async for chunk in result]
            assert chunks == ["chunk_0", "chunk_1", "chunk_2"]

        asyncio.run(_test())

    def test_async_awaitable_result_stream_empty(self):
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
