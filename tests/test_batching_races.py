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
"""Tests verifying fixes for race conditions in storey _Batching.

Races happen at `await` points where the event loop switches between the
timer task (_sleep_and_emit) and the run loop processing events.  We use
an asyncio.Event gate inside _emit to deterministically yield control at
the exact moment needed to verify each fix.

Tests use build_flow with AsyncEmitSource to exercise the full graph,
matching production configuration.
"""

import asyncio
import time as _time
from datetime import datetime as _dt

from storey import AsyncEmitSource, Event, build_flow
from storey.flow import _Batching

# ---------------------------------------------------------------------------
# Helpers — real _Batching subclasses matching production config
# ---------------------------------------------------------------------------


class GatedTarget(_Batching):
    """_Batching subclass whose _emit blocks on an asyncio.Event gate.

    Matches production: _do_downstream_per_event=True (same as ParquetTarget
    and TimescaleDBTarget which inherit from _Batching without overriding it).
    """

    # Same as production — ParquetTarget/TimescaleDBTarget don't override this
    _do_downstream_per_event = True

    def __init__(self, gate: asyncio.Event = None, **kwargs):
        super().__init__(**kwargs)
        self.gate = gate
        self.emitted_batches: list[list] = []
        self.emitted_batch_events: list[list] = []
        self.emit_count = 0

    async def _emit(self, batch, batch_key, batch_time, batch_events, last_event_time=None):
        self.emit_count += 1
        if self.gate is not None:
            # Simulates a slow S3 write (ParquetTarget) or TSDB write (TimescaleDBTarget)
            # Both are async operations that yield to the event loop
            await self.gate.wait()
        self.emitted_batches.append(list(batch))
        self.emitted_batch_events.append(list(batch_events))


class RecordingTarget(_Batching):
    """_Batching subclass that captures state during _terminate."""

    _do_downstream_per_event = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.emitted_batches: list[list] = []
        self.terminate_called = False
        self.timeout_task_alive_during_terminate = None

    async def _emit(self, batch, batch_key, batch_time, batch_events, last_event_time=None):
        self.emitted_batches.append(list(batch))

    async def _terminate(self):
        # Capture _timeout_task state at the moment _terminate is called.
        # In production, this is where ParquetTarget closes file handles
        # and TimescaleDBTarget closes the connection pool.
        task = self._timeout_task
        if task is not None:
            self.timeout_task_alive_during_terminate = not task.done()
        else:
            self.timeout_task_alive_during_terminate = False
        self.terminate_called = True


def _ev(value, key=None):
    """Create an Event with a dict body."""
    return Event({"v": value}, key=key)


# ---------------------------------------------------------------------------
# Tests — each verifies a race condition fix
# ---------------------------------------------------------------------------


class TestBatchingRaceConditions:
    """Verify race condition fixes in _Batching._emit_batch.

    Production trigger: event source rebalance/drain →
    drain_callback calls controller.terminate(wait=True) →
    AsyncEmitSource emits _termination_obj → propagates to _Batching._do →
    _emit_all → _emit_batch → _emit (the slow target write).

    Meanwhile, _sleep_and_emit (timer task) runs independently.
    """

    def test_race1_concurrent_emit_batch_keyerror(self):
        """Timer and max_events both call _emit_batch for the same key.

        The timer flushes a batch while _emit yields (slow write).  New
        events arrive and hit max_events, triggering a second _emit_batch
        for the same key.  With the fix, _batch_events is popped before
        await so both calls operate on separate data.
        """

        async def _test():
            gate = asyncio.Event()
            target = GatedTarget(gate=gate, max_events=2, flush_after_seconds=0.01)
            controller = build_flow([AsyncEmitSource(), target]).run()

            # Event 1 starts the timer
            await controller.emit(_ev(1))

            # Timer fires -> _emit_batch(None) -> blocks on gate inside _emit
            await asyncio.sleep(0.05)
            assert target.emit_count == 1, "Timer should have started _emit"

            # Events 2,3 arrive while _emit is blocked.
            # Event 3 hits max_events=2 -> _do calls _emit_batch(None).
            await controller.emit(_ev(2))
            await controller.emit(_ev(3))
            # Yield to let the max_events _emit_batch start and reach the gate
            await asyncio.sleep(0)

            # Release gate — both _emit_batch calls complete without KeyError
            gate.set()
            await asyncio.sleep(0)

            await controller.terminate()
            await controller.await_termination()

        asyncio.run(_test())

    def test_race2_events_deleted_by_concurrent_finally(self):
        """Events arriving during a slow _emit are isolated from the
        in-flight batch.

        With the fix, _batch_events is popped before await, so new events
        go into a fresh list and their references survive the completion
        of the in-flight _emit.
        """

        async def _test():
            gate = asyncio.Event()
            target = GatedTarget(gate=gate, flush_after_seconds=0.05)
            controller = build_flow([AsyncEmitSource(), target]).run()

            # Emit first event
            await controller.emit(_ev(1))

            # Timer fires -> _emit_batch -> blocks on gate
            await asyncio.sleep(0.1)
            assert target.emit_count == 1

            # Event 2 arrives while _emit is blocked.  The run loop picks
            # it up from the queue and calls _do(), which appends to a
            # fresh _batch_events[None] (the timer popped the old one).
            await controller.emit(_ev(2))
            # Yield so the run loop processes the queued event
            await asyncio.sleep(0)

            # Release gate -> timer's _emit completes and cleans up its
            # own (already-popped) batch_events.  Event 2's list survives.
            gate.set()
            await asyncio.sleep(0)

            # Event 2's reference should survive — it's in a separate list
            has_pending_data = None in target._batch and len(target._batch[None]) > 0
            batch_events_count = len(target._batch_events.get(None, []))

            assert has_pending_data, "Event 2's data should be in _batch"
            assert batch_events_count == 1, (
                f"Event 2's reference should be preserved in _batch_events[None], " f"got {batch_events_count}"
            )

            await controller.terminate()
            await controller.await_termination()

        asyncio.run(_test())

    def test_race3_timeout_task_not_cancelled_during_terminate(self):
        """_timeout_task must be stopped before _terminate runs.

        Without the fix, _timeout_task is still alive when _terminate
        closes connection pools / file handles, and could wake up and
        try to write to a closed target.
        """

        async def _test():
            # flush_after_seconds=60 -> timer sleeps a long time
            target = RecordingTarget(flush_after_seconds=60.0)
            controller = build_flow([AsyncEmitSource(), target]).run()

            # Emit event, starts timer
            await controller.emit(_ev(1))

            # Terminate — matches drain_callback path
            await controller.terminate()
            await controller.await_termination()

            assert target.terminate_called
            assert not target.timeout_task_alive_during_terminate, (
                "_timeout_task was alive when _terminate ran — "
                "TimescaleDBTarget._terminate closes the connection pool, "
                "but the timer could wake up and try to use it"
            )

        asyncio.run(_test())

    def test_race4_emit_all_misses_new_keys(self):
        """_emit_all must flush keys added during a yielding _emit.

        With the snapshot-based iteration (list(keys)), keys added while
        _emit yields are missed.  The while-loop fix picks them up.

        We inject a new key directly into _batch while _emit is blocked
        to simulate a new partition key arriving during drain.
        """

        async def _test():
            gate = asyncio.Event()
            # key_field="$key" simulates ParquetTarget's partition-based keying
            target = GatedTarget(gate=gate, key_field="$key")
            controller = build_flow([AsyncEmitSource(), target]).run()

            # Event with key "A"
            await controller.emit(_ev(1, key="endpoint_A"))

            # Start termination — _emit_all begins, blocks on gate for "endpoint_A"
            term_task = asyncio.ensure_future(controller.terminate())
            await asyncio.sleep(0.05)
            assert target.emit_count == 1

            # While _emit("endpoint_A") is blocked, inject a new partition key.
            # In production, this happens when the event loop processes a queued
            # event for a different endpoint during the S3 write yield.
            target._batch["endpoint_B"].append({"v": 2})
            target._batch_events["endpoint_B"].append(_ev(2, key="endpoint_B"))
            target._batch_first_event_time["endpoint_B"] = _dt.now()
            target._batch_last_event_time["endpoint_B"] = _dt.now()
            target._batch_start_time["endpoint_B"] = _time.monotonic()

            # Release gate
            gate.set()
            await term_task
            await controller.await_termination()

            emitted_values = [item["v"] for batch in target.emitted_batches for item in batch if isinstance(item, dict)]

            assert 2 in emitted_values, (
                f"endpoint_B was never flushed — _emit_all's snapshot missed it. " f"Emitted: {emitted_values}"
            )

        asyncio.run(_test())
