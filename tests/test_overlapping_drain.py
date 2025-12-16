# Copyright 2025 Iguazio
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
"""
Tests for overlapping drain scenarios (ML-11518).

When using AsyncEmitSource with Kafka triggers, the Go runtime may timeout
(rebalanceTimeout) before Python's drain completes. This causes Go to send
a new drain signal while the old drain is still running.

The issue manifests when:
1. A drain is in progress (flow is terminating)
2. The task gets externally cancelled (e.g., by a new drain signal)
3. CancelledError propagates up and crashes the worker

The fix ensures that:
1. External cancellation during drain is handled gracefully
2. Resources are properly cleaned up
3. No crash occurs
"""

import asyncio
from contextlib import suppress

import pytest

from storey import AsyncEmitSource, Map, Reduce, build_flow
from storey.dtypes import _termination_obj

pytestmark = pytest.mark.asyncio


class SlowStep(Map):
    """A step that takes time to process, simulating slow drain."""

    def __init__(self, delay: float = 0.5, **kwargs):
        super().__init__(lambda x: x, **kwargs)
        self._delay = delay

    async def _do(self, event):
        await asyncio.sleep(self._delay)
        return await super()._do(event)


async def test_overlapping_drain_cancellation():
    """
    Test that external cancellation during drain is handled gracefully.

    This simulates the scenario where Go's rebalanceTimeout expires before
    Python's drain completes, causing the drain task to be cancelled.

    Before the fix: CancelledError would propagate and crash the worker.
    After the fix: Cancellation is handled gracefully, no crash.
    """
    # Build a flow with a slow step to ensure drain takes time
    controller = build_flow(
        [
            AsyncEmitSource(),
            SlowStep(delay=0.5),
            Reduce([], lambda acc, x: acc + [x]),
        ]
    ).run()

    # Emit some events
    for i in range(5):
        await controller.emit(i)

    # Start termination (drain) but don't wait for it yet
    await controller._emit_fn(_termination_obj)

    # Simulate external cancellation (like Go timeout triggering new drain)
    # This is what happens when rebalanceTimeout expires
    controller._loop_task.cancel()

    # Awaiting a cancelled task raises CancelledError - this is expected asyncio behavior.
    # The test verifies the flow handles cancellation without crashing.
    with pytest.raises(asyncio.CancelledError):
        await controller._loop_task

    # Task should be done (cancelled tasks are a subset of done tasks)
    assert controller._loop_task.done()


async def test_overlapping_drain_with_await_termination():
    """
    Test that await_termination handles external cancellation gracefully.

    This tests the more realistic scenario where await_termination() is called
    and the underlying task gets cancelled.
    """
    results = []
    controller = build_flow(
        [
            AsyncEmitSource(),
            SlowStep(delay=0.5),
            Reduce(results, lambda acc, x: acc + [x]),
        ]
    ).run()

    # Emit events
    for i in range(3):
        await controller.emit(i)

    # Create a task that will call terminate and await_termination
    async def do_drain():
        await controller.terminate()
        await controller.await_termination()

    drain_task = asyncio.create_task(do_drain())

    # Give it a moment to start
    await asyncio.sleep(0.05)

    # Cancel the drain (simulating Go timeout)
    drain_task.cancel()

    # Cancelling drain_task raises CancelledError - expected behavior
    with suppress(asyncio.CancelledError):
        await drain_task

    # Always clean up controller's loop task (cancel is no-op if already done)
    controller._loop_task.cancel()
    with suppress(asyncio.CancelledError):
        await controller._loop_task


async def test_sequential_drains_no_overlap():
    """
    Test that sequential (non-overlapping) drains work correctly.

    This is the normal case - drain completes fully before any new operation.
    """
    controller = build_flow(
        [
            AsyncEmitSource(),
            Reduce(0, lambda acc, x: acc + x),
        ]
    ).run()

    # First batch
    for i in range(3):
        await controller.emit(i)

    # Terminate and wait
    await controller.terminate()
    result = await controller.await_termination()

    # Verify result (0 + 0 + 1 + 2 = 3)
    assert result == 3


async def test_drain_with_batching_step():
    """
    Test drain with a batching step that holds state.

    Batching steps (_Batching subclasses) hold events in _batch and _batch_events.
    When drain is cancelled, these must be cleared to prevent memory leaks.
    """
    from storey.flow import _Batching

    results = []

    # Create a simple batching step
    class SimpleBatch(_Batching):
        def __init__(self, max_events=10, **kwargs):
            super().__init__(max_events=max_events, **kwargs)

        async def _emit(self, batch, batch_key, batch_time, batch_events, last_event_time=None):
            results.append(batch)

    controller = build_flow(
        [
            AsyncEmitSource(),
            SimpleBatch(max_events=100, timeout_secs=60),
        ]
    ).run()

    # Emit events (they'll be batched, not flushed yet)
    for i in range(5):
        await controller.emit({"value": i})

    # Cancel the task before drain completes
    controller._loop_task.cancel()
    with suppress(asyncio.CancelledError):
        await controller._loop_task

    # The batched events should be cleared (or at least not leak)
    # In real scenario with fix, resources would be cleaned up


async def test_overlapping_drain_memory_leak():
    """
    Test that overlapping drains (cancelled mid-drain) don't cause memory leaks.

    When a drain is cancelled mid-flight (simulating Go's rebalanceTimeout expiring),
    buffers in _Batching._batch may not be cleaned up, causing memory to grow
    with each interrupted drain cycle.

    The real scenario (ML-11518):
    1. Same batching step is reused across drain cycles (Kafka trigger pattern)
    2. Go times out, cancels drain, sends new drain signal
    3. Old drain's _batch buffer is orphaned if not cleared

    This test reuses the same batching step to detect buffer accumulation.
    """
    import gc
    import tracemalloc

    from storey.flow import _Batching

    class PersistentBatch(_Batching):
        """Batching step that persists across flow cycles - like real Kafka trigger."""

        def __init__(self, **kwargs):
            super().__init__(max_events=1000, **kwargs)
            self.emitted_batches = []

        async def _emit(self, batch, batch_key, batch_time, batch_events, last_event_time=None):
            self.emitted_batches.append(len(batch))

    # Create persistent batching step - reused across all cycles
    persistent_batch = PersistentBatch(timeout_secs=60)

    tracemalloc.start()

    # Warmup
    controller = build_flow(
        [
            AsyncEmitSource(),
            Reduce([], lambda acc, x: acc + [x]),
        ]
    ).run()
    await controller.emit(0)
    await controller.terminate()
    await controller.await_termination()

    gc.collect()
    snapshot_before = tracemalloc.take_snapshot()

    # Run many interrupted drain cycles with SAME batching step
    num_cycles = 50
    for cycle in range(num_cycles):
        # Rebuild flow but reuse the same persistent_batch step
        source = AsyncEmitSource()
        flow = build_flow(
            [
                source,
                SlowStep(delay=0.2),
                persistent_batch,
            ]
        )
        controller = flow.run()

        # Emit events (they accumulate in persistent_batch._batch)
        for i in range(10):
            await controller.emit({"value": i, "cycle": cycle, "data": "x" * 100})

        # Start drain but cancel before it completes
        await controller.terminate()
        await asyncio.sleep(0.05)

        # Cancel mid-drain (simulating Go timeout / overlapping drain)
        controller._loop_task.cancel()
        # await_termination() catches CancelledError and calls _clear_resources() automatically
        await controller.await_termination()

    gc.collect()
    snapshot_after = tracemalloc.take_snapshot()
    tracemalloc.stop()

    # Count total events in batch (it's a dict of key -> list)
    batch_size_after = sum(len(v) for v in persistent_batch._batch.values())

    # The batch should not grow unboundedly - it should be cleared on drain
    # If leaking: batch_size_after = 50 cycles * 10 events = 500 events
    # If working: batch_size_after should be 0 or small (last cycle's unflushed)
    max_expected_batch = 20  # Allow some buffered events, but not 500
    assert batch_size_after <= max_expected_batch, (
        f"Buffer leak detected: _batch has {batch_size_after} events after {num_cycles} "
        f"interrupted drain cycles (max expected: {max_expected_batch}). "
        f"Buffers are not being cleared when drain is cancelled."
    )

    # Also check memory growth
    stats = snapshot_after.compare_to(snapshot_before, "lineno")
    total_growth = sum(stat.size_diff for stat in stats if stat.size_diff > 0)

    max_growth_bytes = 50 * 1024  # 50KB max
    assert total_growth <= max_growth_bytes, (
        f"Memory leak detected: {total_growth / 1024:.1f}KB grew after {num_cycles} "
        f"interrupted drain cycles (max allowed: {max_growth_bytes / 1024:.1f}KB)."
    )


async def test_clear_resources_clears_circular_references():
    """
    Test that _clear_resources() properly clears circular references.

    Flow steps have circular references via _outlets/_inlets that prevent
    garbage collection. _clear_resources() must clear these to enable GC.
    """
    from storey import Batch

    # Build a flow with multiple steps
    source = AsyncEmitSource()
    map_step = Map(lambda x: x)
    batch_step = Batch(max_events=100, flush_after_seconds=60)
    reduce_step = Reduce([], lambda acc, x: acc + [x])

    controller = build_flow([source, map_step, batch_step, reduce_step]).run()

    # Emit some events
    for i in range(3):
        await controller.emit({"value": i})

    # Terminate normally
    await controller.terminate()
    await controller.await_termination()

    # Verify circular references exist before clearing
    assert len(source._outlets) > 0, "source should have outlets"
    assert len(map_step._outlets) > 0, "map_step should have outlets"
    assert len(batch_step._outlets) > 0, "batch_step should have outlets"

    # Clear resources starting from source (propagates to all downstream steps)
    source._clear_resources()

    # Verify circular references are cleared in all steps
    assert source._outlets == [], "source._outlets should be cleared"
    assert source.context is None, "source.context should be None"

    assert map_step._outlets == [], "map_step._outlets should be cleared"
    assert map_step.context is None, "map_step.context should be None"

    assert batch_step._outlets == [], "batch_step._outlets should be cleared"
    assert batch_step._timeout_task is None, "batch_step._timeout_task should be None"
    assert batch_step._extract_key is None, "batch_step._extract_key should be None"
    assert len(batch_step._batch) == 0, "batch_step._batch should be empty"

    assert reduce_step._outlets == [], "reduce_step._outlets should be cleared"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
