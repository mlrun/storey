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
"""Tests demonstrating race conditions in storey _Batching.

All code runs on a single asyncio event loop.  Races happen at `await` points
where the event loop switches to another ready coroutine.  We use an
asyncio.Event gate inside _emit to deterministically yield control at the
exact moment needed to trigger each race.

Tests operate directly on _Batching subclasses, calling _do() to inject events.
This bypasses the AsyncEmitSource run loop (which processes events one at a time),
but the interleaving is realistic because:

  1. The timer (_sleep_and_emit) is an independent asyncio.Task — it runs regardless
     of whether the run loop is processing an event or idle at _q.get().
  2. When the timer's _emit yields (slow S3/TSDB write), the run loop CAN be at
     await _q.get(), receive a new event, and call _do_downstream → _Batching._do.
  3. So _do() being called while the timer's _emit is blocked is exactly what happens
     in production when the run loop is between events.

The one thing that CANNOT happen in production: _do() called while a PREVIOUS _do()
is still in _emit_batch (via max_events).  The run loop serializes that.  But _do()
CAN run while the TIMER's _emit_batch is in progress — that's the realistic race.

Production configuration:

  - AsyncEmitSource (explicit_ack=True) → ... → ParquetTarget / TimescaleDBTarget
  - ParquetTarget:     max_events=10, flush_after_seconds=30, key_field=<callable> (partition path)
  - TimescaleDBTarget: max_events=1000, flush_after_seconds=30, key_field=None (all events → key=None)
  - _do_downstream_per_event=True for both (inherited from _Batching)
  - Drain via: controller.terminate(wait=True) from Nuclio drain_callback (SIGUSR2)
"""

import asyncio
import time as _time
from datetime import datetime as _dt

from storey import Event
from storey.flow import _Batching, _termination_obj

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


class FailOnceTarget(_Batching):
    """_Batching subclass whose _emit fails on the Nth call.

    Simulates S3 ReadTimeoutError (ParquetTarget) or TSDB connection
    failure (TimescaleDBTarget) after exhausting retries.
    """

    _do_downstream_per_event = True

    def __init__(self, fail_on=1, **kwargs):
        super().__init__(**kwargs)
        self._fail_on = fail_on
        self.emit_count = 0
        self.emitted_batches: list[list] = []

    async def _emit(self, batch, batch_key, batch_time, batch_events, last_event_time=None):
        self.emit_count += 1
        if self.emit_count == self._fail_on:
            raise ConnectionError("Simulated target failure (e.g. S3 ReadTimeoutError)")
        self.emitted_batches.append(list(batch))


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
# Tests — each demonstrates one race condition
# ---------------------------------------------------------------------------


class TestBatchingRaceConditions:
    """Demonstrate race conditions in _Batching._emit_batch.

    Production trigger: Nuclio sends SIGUSR2 on Kafka rebalance →
    MLRun drain_callback calls controller.terminate(wait=True) →
    AsyncEmitSource emits _termination_obj → propagates to _Batching._do →
    _emit_all → _emit_batch → _emit (the slow target write).

    Meanwhile, _sleep_and_emit (timer task) runs independently.
    """

    def test_race1_concurrent_emit_batch_keyerror(self):
        """Reproduces the KeyError: None crash seen in IG4-1713 pod logs.

        Production scenario: TimescaleDBTarget with max_events=1000,
        flush_after_seconds=30.  Timer flushes a small batch, _emit is slow
        (TSDB connection issue).  New events arrive.  When they hit max_events,
        _do calls _emit_batch for the same key=None.  Both _emit_batch calls
        share the same _batch_events[None] list.  Both finally blocks try to
        del it — second one KeyErrors.

        Time  | Run loop (_do)                     | Timer (_sleep_and_emit)             | _batch_events[None]
        ------+------------------------------------+-------------------------------------+--------------------
          t0  | _do(ev1) -> append, start timer    | sleeping...                         | [ev1]
          t1  | idle at _q.get()                   | wakes -> _emit_batch(None)          | [ev1]
          t2  | idle                               |   _batch.pop(None) -> batch1        | [ev1]
          t3  | idle                               |   await _emit(batch1, list_A) YIELD | [ev1]  <- list_A
        ------+------------------------------------+-- -- -- -- -- -- -- -- -- -- -- -- --+--------------------
          t4  | _do(ev2) -> append                 |   (suspended in _emit)              | [ev1, ev2]
          t5  | _do(ev3) -> append, max_events hit |   (suspended in _emit)              | [ev1, ev2, ev3]
          t6  |   _emit_batch(None)                |   (suspended in _emit)              | [ev1, ev2, ev3]
          t7  |     _batch.pop(None) -> batch2     |   (suspended in _emit)              | [ev1, ev2, ev3]
          t8  |     await _emit(batch2, list_A)    |   (suspended in _emit)              |  SAME list_A!
              |       YIELDS                       |                                     |
        ------+-- -- -- -- -- -- -- -- -- -- -- -- +-- -- -- -- -- -- -- -- -- -- -- -- --+--------------------
          t9  |   (suspended)                      |   _emit returns                     | [ev1, ev2, ev3]
          t10 |   (suspended)                      |   finally: del _batch_events[None]  | DELETED ok
        ------+-- -- -- -- -- -- -- -- -- -- -- -- +-------------------------------------+--------------------
          t11 |   _emit returns                    |                                     |
          t12 |   finally: del _batch_events[None] |                                     | KeyError!
        """

        async def _test():
            gate = asyncio.Event()
            # Match TimescaleDBTarget: key_field=None (all events under key None)
            target = GatedTarget(gate=gate, max_events=2, flush_after_seconds=0.01)
            target._init()

            # t0: Event 1 starts the timer
            await target._do(_ev(1))

            # t1-t3: Timer fires -> _emit_batch(None) -> blocks on gate inside _emit
            await asyncio.sleep(0.05)
            assert target.emit_count == 1, "Timer should have started _emit"

            # t4-t5: Events 2,3 arrive while _emit is blocked.
            # Event 2 appends to existing _batch_events[None].
            await target._do(_ev(2))
            # t6-t8: Event 3 hits max_events=2 -> _do calls _emit_batch(None).
            # This also blocks on gate.  Use ensure_future to avoid deadlock.
            do_task = asyncio.ensure_future(target._do(_ev(3)))
            await asyncio.sleep(0.01)

            # t9-t12: Release both -- both finally blocks run, second del KeyErrors.
            gate.set()
            await do_task

        asyncio.run(_test())

    def test_race2_events_deleted_by_concurrent_finally(self):
        """Events arriving during a slow _emit are appended to the live
        _batch_events list.  finally: del _batch_events[key] deletes them too.

        Production scenario: ParquetTarget flush_after_seconds=30, slow S3
        write.  While S3 write is in progress, new events accumulate.
        When write completes, finally deletes _batch_events[None] including
        the new events.  Their Kafka offset weakrefs are dropped, making
        the offsets look committable before the events are actually written.

        Time  | Run loop (_do)                     | Timer (_sleep_and_emit)             | _batch_events[None]
        ------+------------------------------------+-------------------------------------+--------------------
          t0  | _do(ev1) -> append, start timer    | sleeping...                         | [ev1]
          t1  | idle at _q.get()                   | sleeping...                         | [ev1]
          t2  | idle                               | wakes -> _emit_batch(None)          | [ev1]
          t3  | idle                               |   _batch.pop(None) -> batch1        | [ev1]
          t4  | idle                               |   await _emit(batch1, list_A) YIELD | [ev1]  <- list_A
        ------+------------------------------------+-- -- -- -- -- -- -- -- -- -- -- -- --+--------------------
          t5  | _do(ev2) -> _Batching._do          |   (suspended in _emit)              | [ev1, ev2]
          t6  |   _batch[None].append(data2)       |   (suspended in _emit)              |   SAME list_A!
          t7  |   _batch_events[None].append(ev2)  |   (suspended in _emit)              | [ev1, ev2]
          t8  | idle at _q.get()                   |   (suspended in _emit)              | [ev1, ev2]
        ------+------------------------------------+-- -- -- -- -- -- -- -- -- -- -- -- --+--------------------
          t9  | idle                               |   _emit returns                     | [ev1, ev2]
          t10 | idle                               |   finally: del _batch_events[None]  | DELETED
        ------+------------------------------------+-------------------------------------+--------------------
              |                                    |                                     | ev2 ref gone
              |                                    |                                     | offset committable
              |                                    |                                     | ev2 NEVER WRITTEN
        """

        async def _test():
            gate = asyncio.Event()
            # Match production: flush_after_seconds triggers timer-based flush
            target = GatedTarget(gate=gate, flush_after_seconds=0.05)
            target._init()

            # t0: emit first event
            await target._do(_ev(1))

            # t2-t4: Timer fires -> _emit_batch -> blocks on gate
            await asyncio.sleep(0.1)
            assert target.emit_count == 1

            # t5-t7: Event 2 arrives while _emit is blocked
            await target._do(_ev(2))

            # With the fix: events should be in SEPARATE lists.
            # The timer popped _batch_events[None] before await, so event 2
            # went into a fresh defaultdict list, not the timer's list.
            assert (
                len(target._batch_events[None]) == 1
            ), "Event 2 should be in its own _batch_events[None], not shared with event 1"

            # t9-t10: Release gate -> timer's _emit completes
            gate.set()
            await asyncio.sleep(0.01)

            # Event 2's reference should survive — it's in a separate list
            has_pending_data = None in target._batch and len(target._batch[None]) > 0
            batch_events_count = len(target._batch_events.get(None, []))

            assert has_pending_data, "Event 2's data should be in _batch"
            assert batch_events_count == 1, (
                f"Event 2's reference should be preserved in _batch_events[None], " f"got {batch_events_count}"
            )

        asyncio.run(_test())

    def test_race3_timeout_task_not_cancelled_during_terminate(self):
        """_do(_termination_obj) calls _emit_all() then _terminate() but never
        cancels _timeout_task.

        Production scenario: Nuclio drain_callback -> controller.terminate(wait=True)
        -> _termination_obj propagates to _Batching._do -> _emit_all -> _terminate.
        TimescaleDBTarget._terminate closes the connection pool.
        ParquetTarget has no _terminate but its file system handles go stale.

        If _timeout_task is sleeping (flush_after_seconds=30), it's still alive
        when _terminate runs.  If it wakes and finds new events, it writes to
        a closed target.

        Time  | _do(_termination_obj)              | Timer (_sleep_and_emit)             | _timeout_task
        ------+------------------------------------+-------------------------------------+--------------------
          t0  | _do(ev1) -> append, start timer    | sleeping for 60s...                 | alive, sleeping
          t1  | _do(_termination_obj)              | sleeping...                         | alive, sleeping
          t2  |   _emit_all() -> _emit_batch(None) | sleeping...                         | alive, sleeping
          t3  |     _emit(batch) -> completes      | sleeping...                         | alive, sleeping
          t4  |     finally: del _batch_events     | sleeping...                         | alive, sleeping
          t5  |   _terminate()                     | sleeping...                         | alive, sleeping
              |     closes DB pool / file handles  |                                     |
          t6  |   _do_downstream(_termination_obj) | sleeping...                         | alive, sleeping
        ------+------------------------------------+-------------------------------------+--------------------
              | DONE                               | STILL SLEEPING                      | NOT CANCELLED
              |                                    | will wake at t0+60s                 |
              |                                    | may find new events                 |
              |                                    | writes to CLOSED target             |
        """

        async def _test():
            # flush_after_seconds=60 -> timer sleeps a long time
            target = RecordingTarget(flush_after_seconds=60.0)
            target._init()

            # t0: emit event, starts timer
            await target._do(_ev(1))
            assert target._timeout_task is not None

            # t1-t6: Terminate -- matches drain_callback path
            await target._do(_termination_obj)

            assert target.terminate_called
            assert not target.timeout_task_alive_during_terminate, (
                "_timeout_task was alive when _terminate ran — "
                "TimescaleDBTarget._terminate closes the connection pool, "
                "but the timer could wake up and try to use it"
            )

        asyncio.run(_test())

    def test_race4_emit_all_misses_new_keys(self):
        """_emit_all snapshots keys via list(self._batch.keys()), then iterates.
        If _emit yields and new events with a NEW key arrive, those keys are not
        in the snapshot and are never flushed.

        Production scenario: ParquetTarget uses key_field=<callable> that extracts
        the partition path (e.g. "endpoint_id=abc/2026/03/16/09").  Different
        endpoints produce different keys.  During drain, _emit_all snapshots
        existing partition keys.  If a new partition key arrives while _emit is
        writing to S3 for an existing key, the new partition is never flushed.

        Time  | _do(_termination_obj)              | Run loop (_do)                      | _batch keys
        ------+------------------------------------+-------------------------------------+--------------------
          t0  | _do(ev1, key="A") -> append        |                                     | {"A": [ev1]}
          t1  | _do(_termination_obj)              |                                     | {"A": [ev1]}
          t2  |   _emit_all()                      |                                     |
          t3  |     snapshot = list(keys) -> ["A"]  |                                     | {"A": [ev1]}
          t4  |     _emit_batch("A")               |                                     |
          t5  |       _batch.pop("A") -> batch_A   |                                     | {}
          t6  |       await _emit(batch_A) YIELDS  |                                     | {}
        ------+-- -- -- -- -- -- -- -- -- -- -- -- -+-------------------------------------+--------------------
          t7  |   (suspended in _emit)             | _do(ev2, key="B") -> append         | {"B": [ev2]}
        ------+-- -- -- -- -- -- -- -- -- -- -- -- -+-------------------------------------+--------------------
          t8  |       _emit returns                |                                     | {"B": [ev2]}
          t9  |     snapshot exhausted, loop ends  |                                     | {"B": [ev2]}
          t10 |   _terminate()                     |                                     | {"B": [ev2]}
        ------+------------------------------------+-------------------------------------+--------------------
              | DONE                               |                                     | "B" NEVER FLUSHED
        """

        async def _test():
            gate = asyncio.Event()
            # key_field="$key" simulates ParquetTarget's partition-based keying
            target = GatedTarget(gate=gate, key_field="$key")
            target._init()

            # t0: event with key "A"
            await target._do(_ev(1, key="endpoint_A"))

            # t1-t6: Start termination -- _emit_all snapshots keys=["endpoint_A"]
            term_task = asyncio.ensure_future(target._do(_termination_obj))
            await asyncio.sleep(0.05)
            assert target.emit_count == 1

            # t7: While _emit("endpoint_A") is blocked, a new partition key arrives.
            # In production, this happens when the event loop processes a queued
            # event for a different endpoint during the S3 write yield.
            # We inject directly because _do would block on the termination.
            target._batch["endpoint_B"].append({"v": 2})
            target._batch_events["endpoint_B"].append(_ev(2, key="endpoint_B"))
            target._batch_first_event_time["endpoint_B"] = _dt.now()
            target._batch_last_event_time["endpoint_B"] = _dt.now()
            target._batch_start_time["endpoint_B"] = _time.monotonic()

            # t8-t10: release gate
            gate.set()
            await term_task

            emitted_values = []
            for batch in target.emitted_batches:
                for item in batch:
                    if isinstance(item, dict):
                        emitted_values.append(item["v"])

            assert 2 in emitted_values, (
                f"endpoint_B was never flushed — _emit_all's snapshot missed it. " f"Emitted: {emitted_values}"
            )

        asyncio.run(_test())

    def test_timer_error_silently_loses_batch(self):
        """When _emit raises during _sleep_and_emit, except catches it.
        But the batch was already popped and events deleted in finally.
        Data is permanently lost.

        Production scenario: TimescaleDBTarget._emit raises after exhausting
        3 retries (connection error).  ValueError propagates to _sleep_and_emit
        which catches it.  The batch was popped at line 1528 and events deleted
        at line 1538.  Those predictions are gone from TSDB forever.
        This is the ML-12286 silent data loss vector.

        flush_after_seconds=30 in production (using 0.01 here to trigger quickly).

        Time  | Run loop (_do)                     | Timer (_sleep_and_emit)             | _batch[None]
        ------+------------------------------------+-------------------------------------+--------------------
          t0  | _do(ev1) -> append, start timer    | sleeping...                         | [ev1]
          t1  | _do(ev2) -> append                 | sleeping...                         | [ev1, ev2]
          t2  | idle at _q.get()                   | sleeping...                         | [ev1, ev2]
          t3  | idle                               | wakes -> _emit_batch(None)          | [ev1, ev2]
          t4  | idle                               |   _batch.pop(None) -> batch         | {} (popped)
          t5  | idle                               |   await _emit(batch) -> RAISES      | {} (popped)
          t6  | idle                               |   finally: del _batch_events[None]  | {} events gone
          t7  | idle                               |   except: logs "Failed to flush"    |
          t8  | idle                               |   _timeout_task = None              |
        ------+------------------------------------+-------------------------------------+--------------------
              |                                    |                                     | ev1, ev2 LOST
              |                                    |                                     | no retry
              |                                    |                                     | no re-queue
        """

        async def _test():
            target = FailOnceTarget(fail_on=1, flush_after_seconds=0.01)
            target._init()

            # t0-t1: emit events
            await target._do(_ev(1))
            await target._do(_ev(2))

            # t3-t8: Timer fires, _emit fails, error swallowed, batch gone
            await asyncio.sleep(0.1)

            assert target.emit_count == 1
            assert len(target.emitted_batches) == 0

            # Terminate — flush whatever survived
            await target._do(_termination_obj)

            all_values = []
            for batch in target.emitted_batches:
                for item in batch:
                    if isinstance(item, dict):
                        all_values.append(item["v"])

            assert sorted(all_values) == [1, 2], (
                f"Expected [1, 2] but got {sorted(all_values)} — " f"events from the failed batch were permanently lost"
            )

        asyncio.run(_test())

    def test_drain_and_timer_should_handle_errors_the_same_way(self):
        """Asymmetric error handling: _sleep_and_emit catches exceptions,
        _emit_all does not.  Same failure is silent in timer, fatal in drain.

        Both paths should handle errors gracefully — either both propagate
        the error, or both catch it and preserve the batch for retry.
        Currently neither path preserves the batch, and only the drain path
        propagates the error (crashing the flow).

        Production consequence: S3 ReadTimeoutError during normal operation
        is silently swallowed (data lost, no crash).  Same error during
        Nuclio drain (rebalance) crashes the Python wrapper, triggering a
        pod restart and another rebalance — cascade.

        DRAIN PATH:
        Time  | _do(_termination_obj)              | _emit_batch                         | outcome
        ------+------------------------------------+-------------------------------------+--------------------
          t0  | _do(ev1) -> append                 |                                     |
          t1  | _do(_termination_obj)              |                                     |
          t2  |   _emit_all() -> _emit_batch(None) |   _batch.pop -> batch               |
          t3  |                                    |   await _emit(batch) -> RAISES      |
          t4  |                                    |   finally: del _batch_events         |
          t5  |   exception propagates up          |                                     | CRASH
        ------+------------------------------------+-------------------------------------+--------------------

        TIMER PATH (same error):
        Time  | Run loop (_do)                     | Timer (_sleep_and_emit)             | outcome
        ------+------------------------------------+-------------------------------------+--------------------
          t0  | _do(ev1) -> append, start timer    | sleeping...                         |
          t1  | idle                               | wakes -> _emit_batch(None)          |
          t2  | idle                               |   _batch.pop -> batch               |
          t3  | idle                               |   await _emit(batch) -> RAISES      |
          t4  | idle                               |   finally: del _batch_events         |
          t5  | idle                               |   except: logs error, continues     | SILENT
          t6  | idle                               |   _timeout_task = None              | data lost
        ------+------------------------------------+-------------------------------------+--------------------
        """

        async def _test():
            # DRAIN PATH: error should not crash the flow — it should be
            # handled gracefully (e.g. logged, batch preserved for redelivery).
            target1 = FailOnceTarget(fail_on=1, flush_after_seconds=999)
            target1._init()
            await target1._do(_ev(1))

            drain_crashed = False
            try:
                await target1._do(_termination_obj)
            except ConnectionError:
                drain_crashed = True

            assert not drain_crashed, (
                "Drain path crashes on _emit error, but timer path swallows it — "
                "both should handle errors the same way"
            )

            # TIMER PATH: error should not silently lose data — the batch
            # should be preserved for retry or redelivery.
            target2 = FailOnceTarget(fail_on=1, flush_after_seconds=0.01)
            target2._init()
            await target2._do(_ev(1))
            await asyncio.sleep(0.1)

            # Batch should still be available for retry
            has_data = None in target2._batch and len(target2._batch[None]) > 0
            has_events = None in target2._batch_events and len(target2._batch_events[None]) > 0
            assert has_data and has_events, (
                "Timer path swallowed _emit error and lost the batch — "
                "data should be preserved for retry or Kafka redelivery"
            )

        asyncio.run(_test())
