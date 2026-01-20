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
    Choice,
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
        assert [set(*result)] == [{"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}]

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

            assert [set(*result)] == [{"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}]

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
            assert [set(chunks)] == [{"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}]
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

                assert [set(chunks)] == [{"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}]
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
            assert [set(chunks)] == [{"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}]
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

                assert [set(chunks)] == [{"a_test_chunk_0", "a_test_chunk_1", "b_test_chunk_0", "b_test_chunk_1"}]
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


class TestStreamingErrorHandling:
    """Tests for error handling in streaming scenarios."""

    def test_streaming_generator_raises_error(self):
        """Test that error in generator mid-stream propagates without hanging."""

        def error_stream(x):
            yield f"{x}_chunk_0"
            raise ValueError("Generator error mid-stream")
            yield f"{x}_chunk_1"  # noqa: unreachable

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

            # Should be a generator
            assert inspect.isgenerator(result)

            # First chunk should work
            first_chunk = next(result)
            assert first_chunk == "test_chunk_0"

            # Second iteration should raise the error
            with pytest.raises(ValueError, match="Generator error mid-stream"):
                next(result)
        finally:
            controller.terminate()
            # Error is also propagated through termination
            with pytest.raises(ValueError, match="Generator error mid-stream"):
                controller.await_termination()

    def test_async_streaming_generator_raises_error(self):
        """Async version: Test that error in generator mid-stream propagates without hanging."""

        async def _test():
            def error_stream(x):
                yield f"{x}_chunk_0"
                raise ValueError("Generator error mid-stream")
                yield f"{x}_chunk_1"  # noqa: unreachable

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

                # First chunk should work
                first_chunk = await result.__anext__()
                assert first_chunk == "test_chunk_0"

                # Second iteration should raise the error
                with pytest.raises(ValueError, match="Generator error mid-stream"):
                    await result.__anext__()
            finally:
                await controller.terminate()
                # Error is also propagated through termination
                with pytest.raises(ValueError, match="Generator error mid-stream"):
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

            # First chunk (0) should work
            first_chunk = next(result)
            assert first_chunk == 0

            # Second chunk (1) should raise error
            with pytest.raises(RuntimeError, match="Failed on chunk 1"):
                next(result)
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

                # First chunk (0) should work
                first_chunk = await result.__anext__()
                assert first_chunk == 0

                # Second chunk (1) should raise error
                with pytest.raises(RuntimeError, match="Failed on chunk 1"):
                    await result.__anext__()
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
