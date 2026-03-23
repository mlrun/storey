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

        Time  | Run loop (_do)                     | Timer (_sleep_and_emit)
        ------+------------------------------------+------------------------------------
          t0  | emit(ev1) -> append, start timer   | sleeping...
          t1  | idle at _q.get()                   | wakes -> _emit_batch(None)
          t2  | idle                               |   _batch.pop(None) -> batch1
          t3  | idle                               |   await _emit(batch1) YIELDS
        ------+------------------------------------+-- -- -- -- -- -- -- -- -- -- -- --
          t4  | emit(ev2) -> append                |   (suspended in _emit)
          t5  | emit(ev3) -> max_events hit        |   (suspended in _emit)
          t6  |   _emit_batch(None)                |   (suspended in _emit)
          t7  |     _batch.pop(None) -> batch2     |   (suspended in _emit)
          t8  |     await _emit(batch2) YIELDS     |   (suspended in _emit)
        ------+-- -- -- -- -- -- -- -- -- -- -- -- +-- -- -- -- -- -- -- -- -- -- -- --
          t9  |   (suspended)                      |   _emit returns
          t10 |   (suspended)                      |   pop _batch_events[None] (ok)
        ------+-- -- -- -- -- -- -- -- -- -- -- -- +------------------------------------
          t11 |   _emit returns                    |
          t12 |   pop _batch_events[None]          |   FIX: separate list, no KeyError
        """

        async def _test():
            gate = asyncio.Event()
            target = GatedTarget(gate=gate, max_events=2, flush_after_seconds=0.01)
            controller = build_flow([AsyncEmitSource(), target]).run()

            # t0: Event 1 starts the timer
            await controller.emit(_ev(1))

            # t1-t3: Wait for timer to fire (flush_after_seconds=0.01) and block on gate
            await asyncio.sleep(0.05)
            assert target.emit_count == 1, "Timer should have started _emit"

            # t4-t8: Events 2,3 arrive while _emit is blocked.
            # Event 3 hits max_events=2 -> _do calls _emit_batch(None).
            await controller.emit(_ev(2))
            await controller.emit(_ev(3))
            await asyncio.sleep(0)  # yield to let max_events _emit_batch reach gate

            # t9-t12: Release gate — both _emit_batch calls complete without KeyError
            gate.set()
            await asyncio.sleep(0)  # yield to let both _emit calls finish

            await controller.terminate()
            await controller.await_termination()

        asyncio.run(_test())

    def test_race2_events_deleted_by_concurrent_finally(self):
        """Events arriving during a slow _emit go into a separate list.

        Time  | Run loop (_do)                     | Timer (_sleep_and_emit)
        ------+------------------------------------+------------------------------------
          t0  | emit(ev1) -> append, start timer   | sleeping...
          t1  | idle at _q.get()                   | sleeping...
          t2  | idle                               | wakes -> _emit_batch(None)
          t3  | idle                               |   _batch.pop, _batch_events.pop
          t4  | idle                               |   await _emit(batch1) YIELDS
        ------+------------------------------------+-- -- -- -- -- -- -- -- -- -- -- --
          t5  | emit(ev2) -> _Batching._do         |   (suspended in _emit)
          t6  |   _batch[None].append(data2)       |   (suspended in _emit)
          t7  |   _batch_events[None].append(ev2)  |   FIX: fresh list, not timer's
          t8  | idle at _q.get()                   |   (suspended in _emit)
        ------+------------------------------------+-- -- -- -- -- -- -- -- -- -- -- --
          t9  | idle                               |   _emit returns
          t10 | idle                               |   (timer's batch_events is local)
        ------+------------------------------------+------------------------------------
              |                                    |   ev2 ref SURVIVES in new list
        """

        async def _test():
            gate = asyncio.Event()
            target = GatedTarget(gate=gate, flush_after_seconds=0.05)
            controller = build_flow([AsyncEmitSource(), target]).run()

            # t0: emit first event
            await controller.emit(_ev(1))

            # t2-t4: Wait for timer to fire (flush_after_seconds=0.05) and block on gate
            await asyncio.sleep(0.1)
            assert target.emit_count == 1

            # t5-t7: Event 2 arrives while _emit is blocked.  The run loop
            # picks it up and appends to a fresh _batch_events[None].
            await controller.emit(_ev(2))
            await asyncio.sleep(0)  # yield so run loop processes the queued event

            # t9-t10: Release gate -> timer's _emit completes
            gate.set()
            await asyncio.sleep(0)  # yield to let timer finish

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

        Time  | _do(_termination_obj)              | Timer (_sleep_and_emit)
        ------+------------------------------------+------------------------------------
          t0  | emit(ev1) -> append, start timer   | sleeping for 60s...
          t1  | terminate()                        | sleeping...
          t2  |   _terminating = True              | sleeping...
          t3  |   await _timeout_task              | checks _terminating -> exits loop
          t4  |   _emit_all() -> _emit_batch(None) |
          t5  |     _emit(batch) -> completes      |
          t6  |   _terminate()                     |   FIX: task is done, not alive
              |     closes DB pool / file handles  |
          t7  |   _do_downstream(_termination_obj) |
        """

        async def _test():
            # flush_after_seconds=60 -> timer sleeps a long time
            target = RecordingTarget(flush_after_seconds=60.0)
            controller = build_flow([AsyncEmitSource(), target]).run()

            # t0: emit event, starts timer
            await controller.emit(_ev(1))

            # t1-t7: Terminate — matches drain_callback path
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

        Time  | _do(_termination_obj)              | _batch keys
        ------+------------------------------------+-----------------------------
          t0  | emit(ev1, key="A") -> append       | {"A": [ev1]}
          t1  | terminate()                        | {"A": [ev1]}
          t2  |   _emit_all()                      |
          t3  |     while _batch: key="A"          | {"A": [ev1]}
          t4  |     _emit_batch("A")               |
          t5  |       _batch.pop("A") -> batch_A   | {}
          t6  |       await _emit(batch_A) YIELDS  | {}
        ------+-- -- -- -- -- -- -- -- -- -- -- -- +-----------------------------
          t7  |   (suspended in _emit)             | {"B": [ev2]}  (injected)
        ------+-- -- -- -- -- -- -- -- -- -- -- -- +-----------------------------
          t8  |       _emit returns                | {"B": [ev2]}
          t9  |     while _batch: key="B"          |   FIX: loop continues
          t10 |     _emit_batch("B") -> completes  | {}
          t11 |   _terminate()                     | {}
        """

        async def _test():
            gate = asyncio.Event()
            # key_field="$key" simulates ParquetTarget's partition-based keying
            target = GatedTarget(gate=gate, key_field="$key")
            controller = build_flow([AsyncEmitSource(), target]).run()

            # t0: event with key "A"
            await controller.emit(_ev(1, key="endpoint_A"))

            # t1-t6: Start termination — _emit_all begins, blocks on gate
            term_task = asyncio.ensure_future(controller.terminate())
            await asyncio.sleep(0.05)  # wait for termination to propagate to _emit
            assert target.emit_count == 1

            # t7: While _emit("endpoint_A") is blocked, inject a new partition key.
            # In production, this happens when the event loop processes a queued
            # event for a different endpoint during the S3 write yield.
            target._batch["endpoint_B"].append({"v": 2})
            target._batch_events["endpoint_B"].append(_ev(2, key="endpoint_B"))
            target._batch_first_event_time["endpoint_B"] = _dt.now()
            target._batch_last_event_time["endpoint_B"] = _dt.now()
            target._batch_start_time["endpoint_B"] = _time.monotonic()

            # t8-t11: Release gate
            gate.set()
            await term_task
            await controller.await_termination()

            emitted_values = [item["v"] for batch in target.emitted_batches for item in batch if isinstance(item, dict)]

            assert 2 in emitted_values, (
                f"endpoint_B was never flushed — _emit_all's snapshot missed it. " f"Emitted: {emitted_values}"
            )

        asyncio.run(_test())
