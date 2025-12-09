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
"""
Tests for Table cleanup on termination.
"""

import gc
import tracemalloc
from datetime import datetime, timedelta

import pytest

from storey import (
    AggregateByKey,
    AsyncEmitSource,
    FieldAggregator,
    NoopDriver,
    Reduce,
    Table,
    build_flow,
)
from storey.dtypes import SlidingWindows

test_base_time = datetime.fromisoformat("2020-07-21T21:40:00+00:00")


def append_return(lst, x):
    lst.append(x)
    return lst


@pytest.mark.asyncio
async def test_table_cleanup_on_terminate():
    """
    Verify Table properly clears internal state on terminate().

    When a Table is reused across multiple flow cycles (e.g., Kafka rebalancing),
    internal caches like _attrs_cache, _aggregates, and _schema must be cleared
    to prevent memory leaks. This test runs 100 cycles and verifies memory
    stays constant (no linear growth).
    """
    tracemalloc.start()

    # Single table reused across cycles (like production model monitoring)
    table = Table("test", NoopDriver())

    # Warmup cycle to stabilize memory
    controller = build_flow(
        [
            AsyncEmitSource(),
            AggregateByKey(
                [FieldAggregator("col1", "col1", ["sum", "avg"], SlidingWindows(["1h"], "10m"))],
                table,
                time_field="time",
            ),
            Reduce([], append_return),
        ]
    ).run()
    await controller.emit({"col1": 0, "time": test_base_time}, key="warmup")
    await controller.terminate(wait=True)

    gc.collect()
    snapshot_before = tracemalloc.take_snapshot()

    # Run 100 drain cycles - reusing same table (critical for leak detection)
    for cycle in range(100):
        # Reset table for reuse (simulates what happens between Kafka rebalances)
        table._terminated = False

        controller = build_flow(
            [
                AsyncEmitSource(),
                AggregateByKey(
                    [FieldAggregator("col1", "col1", ["sum", "avg"], SlidingWindows(["1h"], "10m"))],
                    table,
                    time_field="time",
                ),
                Reduce([], append_return),
            ]
        ).run()

        # Emit events with unique keys per cycle (simulates different model endpoints)
        for i in range(10):
            event_time = test_base_time + timedelta(minutes=cycle * 10 + i)
            await controller.emit({"col1": i, "time": event_time}, key=f"endpoint_{cycle}_{i}")

        await controller.terminate(wait=True)

    # Force garbage collection to measure true leaks (not just pending GC)
    gc.collect()
    snapshot_after = tracemalloc.take_snapshot()
    tracemalloc.stop()

    # Compare memory growth
    stats = snapshot_after.compare_to(snapshot_before, "lineno")
    total_growth = sum(stat.size_diff for stat in stats if stat.size_diff > 0)

    max_growth_bytes = 50 * 1024
    assert total_growth <= max_growth_bytes, (
        f"Memory leak detected: {total_growth / 1024:.1f}KB grew after 100 cycles "
        f"(max allowed: {max_growth_bytes / 1024:.1f}KB). "
        f"This indicates resources are not being properly cleaned up during termination."
    )
