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
import math
from datetime import timedelta

import pandas as pd
import pytest

from storey import (AggregateByKey, Context, DataframeSource, FieldAggregator,
                    Map, MapWithState, NoSqlTarget, QueryByKey, Reduce,
                    SyncEmitSource, Table, build_flow)
from storey.dtypes import (EmitAfterMaxEvent, FixedWindows, FixedWindowType,
                           SlidingWindows)
from storey.utils import _split_path

from .integration_test_utils import (append_return, setup_teardown_test,
                                     test_base_time)


@pytest.mark.parametrize(
    "fixed_window_type",
    [FixedWindowType.CurrentOpenWindow, FixedWindowType.LastClosedWindow],
)
@pytest.mark.parametrize("partitioned_by_key", [True, False])
@pytest.mark.parametrize("flush_interval", [None, 1])
def test_aggregate_with_fixed_windows_and_query_past_and_future_times(
    setup_teardown_test, partitioned_by_key, flush_interval, fixed_window_type
):
    table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        partitioned_by_key=partitioned_by_key,
        flush_interval_secs=flush_interval,
    )

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["count"],
                        FixedWindows(["1h", "2h", "24h"]),
                    )
                ],
                table,
                key="col1",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": items_in_ingest_batch}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_count_1h": 1.0,
            "number_of_stuff_count_2h": 1.0,
            "number_of_stuff_count_24h": 1.0,
            "col1": 10,
        },
        {
            "number_of_stuff_count_1h": 1.0,
            "number_of_stuff_count_2h": 1.0,
            "number_of_stuff_count_24h": 2.0,
            "col1": 10,
        },
        {
            "number_of_stuff_count_1h": 2.0,
            "number_of_stuff_count_2h": 2.0,
            "number_of_stuff_count_24h": 3.0,
            "col1": 10,
        },
        {
            "number_of_stuff_count_1h": 3.0,
            "number_of_stuff_count_2h": 3.0,
            "number_of_stuff_count_24h": 4.0,
            "col1": 10,
        },
        {
            "number_of_stuff_count_1h": 1.0,
            "number_of_stuff_count_2h": 4.0,
            "number_of_stuff_count_24h": 5.0,
            "col1": 10,
        },
        {
            "number_of_stuff_count_1h": 2.0,
            "number_of_stuff_count_2h": 5.0,
            "number_of_stuff_count_24h": 6.0,
            "col1": 10,
        },
        {
            "number_of_stuff_count_1h": 1.0,
            "number_of_stuff_count_2h": 1.0,
            "number_of_stuff_count_24h": 1.0,
            "col1": 10,
        },
        {
            "number_of_stuff_count_1h": 2.0,
            "number_of_stuff_count_2h": 2.0,
            "number_of_stuff_count_24h": 2.0,
            "col1": 10,
        },
        {
            "number_of_stuff_count_1h": 1.0,
            "number_of_stuff_count_2h": 3.0,
            "number_of_stuff_count_24h": 3.0,
            "col1": 10,
        },
        {
            "number_of_stuff_count_1h": 2.0,
            "number_of_stuff_count_2h": 4.0,
            "number_of_stuff_count_24h": 4.0,
            "col1": 10,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    if fixed_window_type == FixedWindowType.CurrentOpenWindow:
        expected_results = [
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 1.0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 1.0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 3.0,
                "number_of_stuff_count_2h": 5.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 3.0,
                "number_of_stuff_count_2h": 5.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 3.0,
                "number_of_stuff_count_2h": 5.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 5.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 5.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 4.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 4.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 4.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 4.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 4.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 4.0,
                "col1": 10,
            },
        ]
    else:
        expected_results = [
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 1.0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 1.0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 1.0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 3.0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 3.0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_24h": 0.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 5.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 5.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 5.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 5.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 5.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 4.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 2.0,
                "number_of_stuff_count_2h": 4.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 4.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 4.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 4.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
            {
                "number_of_stuff_count_1h": 0.0,
                "number_of_stuff_count_2h": 0.0,
                "number_of_stuff_count_24h": 6.0,
                "col1": 10,
            },
        ]
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                [
                    "number_of_stuff_count_1h",
                    "number_of_stuff_count_2h",
                    "number_of_stuff_count_24h",
                ],
                table,
                key="col1",
                fixed_window_type=fixed_window_type,
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    for i in range(-items_in_ingest_batch, 2 * items_in_ingest_batch):
        data = {"col1": items_in_ingest_batch}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("partitioned_by_key", [True, False])
@pytest.mark.parametrize("flush_interval", [None, 1])
def test_aggregate_and_query_with_different_sliding_windows(
    setup_teardown_test, partitioned_by_key, flush_interval
):
    table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        partitioned_by_key=partitioned_by_key,
        flush_interval_secs=flush_interval,
    )

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["sum", "avg", "min", "max", "sqr"],
                        SlidingWindows(["1h", "2h", "24h"], "10m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_1h": 0,
            "number_of_stuff_sum_2h": 0,
            "number_of_stuff_sum_24h": 0,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 0,
            "number_of_stuff_max_2h": 0,
            "number_of_stuff_max_24h": 0,
            "number_of_stuff_sqr_1h": 0,
            "number_of_stuff_sqr_2h": 0,
            "number_of_stuff_sqr_24h": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_avg_24h": 0.0,
            "col1": 0,
        },
        {
            "number_of_stuff_sum_1h": 1,
            "number_of_stuff_sum_2h": 1,
            "number_of_stuff_sum_24h": 1,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 1,
            "number_of_stuff_max_2h": 1,
            "number_of_stuff_max_24h": 1,
            "number_of_stuff_sqr_1h": 1,
            "number_of_stuff_sqr_2h": 1,
            "number_of_stuff_sqr_24h": 1,
            "number_of_stuff_avg_1h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_avg_24h": 0.5,
            "col1": 1,
        },
        {
            "number_of_stuff_sum_1h": 3,
            "number_of_stuff_sum_2h": 3,
            "number_of_stuff_sum_24h": 3,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 2,
            "number_of_stuff_max_2h": 2,
            "number_of_stuff_max_24h": 2,
            "number_of_stuff_sqr_1h": 5,
            "number_of_stuff_sqr_2h": 5,
            "number_of_stuff_sqr_24h": 5,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_avg_24h": 1.0,
            "col1": 2,
        },
        {
            "number_of_stuff_sum_1h": 6,
            "number_of_stuff_sum_2h": 6,
            "number_of_stuff_sum_24h": 6,
            "number_of_stuff_min_1h": 1,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 3,
            "number_of_stuff_max_2h": 3,
            "number_of_stuff_max_24h": 3,
            "number_of_stuff_sqr_1h": 14,
            "number_of_stuff_sqr_2h": 14,
            "number_of_stuff_sqr_24h": 14,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_avg_24h": 1.5,
            "col1": 3,
        },
        {
            "number_of_stuff_sum_1h": 9,
            "number_of_stuff_sum_2h": 10,
            "number_of_stuff_sum_24h": 10,
            "number_of_stuff_min_1h": 2,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 4,
            "number_of_stuff_max_2h": 4,
            "number_of_stuff_max_24h": 4,
            "number_of_stuff_sqr_1h": 29,
            "number_of_stuff_sqr_2h": 30,
            "number_of_stuff_sqr_24h": 30,
            "number_of_stuff_avg_1h": 3.0,
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_avg_24h": 2.0,
            "col1": 4,
        },
        {
            "number_of_stuff_sum_1h": 12,
            "number_of_stuff_sum_2h": 15,
            "number_of_stuff_sum_24h": 15,
            "number_of_stuff_min_1h": 3,
            "number_of_stuff_min_2h": 1,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 5,
            "number_of_stuff_max_2h": 5,
            "number_of_stuff_max_24h": 5,
            "number_of_stuff_sqr_1h": 50,
            "number_of_stuff_sqr_2h": 55,
            "number_of_stuff_sqr_24h": 55,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_avg_24h": 2.5,
            "col1": 5,
        },
        {
            "number_of_stuff_sum_1h": 15,
            "number_of_stuff_sum_2h": 20,
            "number_of_stuff_sum_24h": 21,
            "number_of_stuff_min_1h": 4,
            "number_of_stuff_min_2h": 2,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 6,
            "number_of_stuff_max_2h": 6,
            "number_of_stuff_max_24h": 6,
            "number_of_stuff_sqr_1h": 77,
            "number_of_stuff_sqr_2h": 90,
            "number_of_stuff_sqr_24h": 91,
            "number_of_stuff_avg_1h": 5.0,
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_avg_24h": 3.0,
            "col1": 6,
        },
        {
            "number_of_stuff_sum_1h": 18,
            "number_of_stuff_sum_2h": 25,
            "number_of_stuff_sum_24h": 28,
            "number_of_stuff_min_1h": 5,
            "number_of_stuff_min_2h": 3,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 7,
            "number_of_stuff_max_2h": 7,
            "number_of_stuff_max_24h": 7,
            "number_of_stuff_sqr_1h": 110,
            "number_of_stuff_sqr_2h": 135,
            "number_of_stuff_sqr_24h": 140,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_avg_24h": 3.5,
            "col1": 7,
        },
        {
            "number_of_stuff_sum_1h": 21,
            "number_of_stuff_sum_2h": 30,
            "number_of_stuff_sum_24h": 36,
            "number_of_stuff_min_1h": 6,
            "number_of_stuff_min_2h": 4,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 8,
            "number_of_stuff_max_2h": 8,
            "number_of_stuff_max_24h": 8,
            "number_of_stuff_sqr_1h": 149,
            "number_of_stuff_sqr_2h": 190,
            "number_of_stuff_sqr_24h": 204,
            "number_of_stuff_avg_1h": 7.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_avg_24h": 4.0,
            "col1": 8,
        },
        {
            "number_of_stuff_sum_1h": 24,
            "number_of_stuff_sum_2h": 35,
            "number_of_stuff_sum_24h": 45,
            "number_of_stuff_min_1h": 7,
            "number_of_stuff_min_2h": 5,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 9,
            "number_of_stuff_max_2h": 9,
            "number_of_stuff_max_24h": 9,
            "number_of_stuff_sqr_1h": 194,
            "number_of_stuff_sqr_2h": 255,
            "number_of_stuff_sqr_24h": 285,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_avg_24h": 4.5,
            "col1": 9,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    tables = [
        table,
        Table(setup_teardown_test.table_name, setup_teardown_test.driver()),
    ]  # test on previous table and on new table
    expected_results = [
        {
            "col1": 10,
            "number_of_stuff_sum_1h": 17.0,
            "number_of_stuff_min_1h": 8.0,
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_avg_1h": 8.5,
        },
        {
            "col1": 10,
            "number_of_stuff_sum_1h": 9.0,
            "number_of_stuff_min_1h": 9.0,
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_avg_1h": 9.0,
        },
    ]
    for table in tables:
        controller = build_flow(
            [
                SyncEmitSource(),
                QueryByKey(
                    [
                        "number_of_stuff_sum_1h",
                        "number_of_stuff_avg_1h",
                        "number_of_stuff_min_1h",
                        "number_of_stuff_max_1h",
                    ],
                    table,
                ),
                Reduce([], lambda acc, x: append_return(acc, x)),
            ]
        ).run()

        base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
        data = {"col1": items_in_ingest_batch}
        controller.emit(data, "tal", base_time)
        controller.emit(data, "tal", base_time + timedelta(minutes=25))

        controller.terminate()
        actual = controller.await_termination()

        assert (
            actual == expected_results
        ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("partitioned_by_key", [True, False])
@pytest.mark.parametrize("flush_interval", [None, 1])
def test_aggregate_and_query_with_different_fixed_windows(
    setup_teardown_test, partitioned_by_key, flush_interval
):
    table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        partitioned_by_key=partitioned_by_key,
        flush_interval_secs=flush_interval,
    )

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["sum", "avg", "min", "max", "sqr"],
                        FixedWindows(["1h", "2h", "24h"]),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_max_1h": 0.0,
            "number_of_stuff_max_2h": 0.0,
            "number_of_stuff_max_24h": 0.0,
            "number_of_stuff_sqr_1h": 0.0,
            "number_of_stuff_sqr_2h": 0.0,
            "number_of_stuff_sqr_24h": 0.0,
            "number_of_stuff_sum_1h": 0.0,
            "number_of_stuff_sum_2h": 0.0,
            "number_of_stuff_sum_24h": 0.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_avg_24h": 0.0,
            "col1": 0,
        },
        {
            "number_of_stuff_max_1h": 1.0,
            "number_of_stuff_sqr_1h": 1.0,
            "number_of_stuff_sum_1h": 1.0,
            "number_of_stuff_min_1h": 1.0,
            "number_of_stuff_max_2h": 1.0,
            "number_of_stuff_sqr_2h": 1.0,
            "number_of_stuff_sum_2h": 1.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_max_24h": 1.0,
            "number_of_stuff_sqr_24h": 1.0,
            "number_of_stuff_sum_24h": 1.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_avg_24h": 0.5,
            "col1": 1,
        },
        {
            "number_of_stuff_max_1h": 2.0,
            "number_of_stuff_max_2h": 2.0,
            "number_of_stuff_max_24h": 2.0,
            "number_of_stuff_sqr_1h": 5.0,
            "number_of_stuff_sqr_2h": 5.0,
            "number_of_stuff_sqr_24h": 5.0,
            "number_of_stuff_sum_1h": 3.0,
            "number_of_stuff_sum_2h": 3.0,
            "number_of_stuff_sum_24h": 3.0,
            "number_of_stuff_min_1h": 1.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_avg_1h": 1.5,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_avg_24h": 1.0,
            "col1": 2,
        },
        {
            "number_of_stuff_max_1h": 3.0,
            "number_of_stuff_max_2h": 3.0,
            "number_of_stuff_max_24h": 3.0,
            "number_of_stuff_sqr_1h": 14.0,
            "number_of_stuff_sqr_2h": 14.0,
            "number_of_stuff_sqr_24h": 14.0,
            "number_of_stuff_sum_1h": 6.0,
            "number_of_stuff_sum_2h": 6.0,
            "number_of_stuff_sum_24h": 6.0,
            "number_of_stuff_min_1h": 1.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_avg_24h": 1.5,
            "col1": 3,
        },
        {
            "number_of_stuff_max_1h": 4.0,
            "number_of_stuff_sqr_1h": 16.0,
            "number_of_stuff_sum_1h": 4.0,
            "number_of_stuff_min_1h": 4.0,
            "number_of_stuff_max_2h": 4.0,
            "number_of_stuff_sqr_2h": 30.0,
            "number_of_stuff_sum_2h": 10.0,
            "number_of_stuff_min_2h": 1.0,
            "number_of_stuff_max_24h": 4.0,
            "number_of_stuff_sqr_24h": 30.0,
            "number_of_stuff_sum_24h": 10.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_2h": 2.5,
            "number_of_stuff_avg_24h": 2.0,
            "col1": 4,
        },
        {
            "number_of_stuff_max_1h": 5.0,
            "number_of_stuff_max_2h": 5.0,
            "number_of_stuff_max_24h": 5.0,
            "number_of_stuff_sqr_1h": 41.0,
            "number_of_stuff_sqr_2h": 55.0,
            "number_of_stuff_sqr_24h": 55.0,
            "number_of_stuff_sum_1h": 9.0,
            "number_of_stuff_sum_2h": 15.0,
            "number_of_stuff_sum_24h": 15.0,
            "number_of_stuff_min_1h": 4.0,
            "number_of_stuff_min_2h": 1.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_avg_1h": 4.5,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_avg_24h": 2.5,
            "col1": 5,
        },
        {
            "number_of_stuff_max_1h": 6.0,
            "number_of_stuff_sqr_1h": 36.0,
            "number_of_stuff_sum_1h": 6.0,
            "number_of_stuff_min_1h": 6.0,
            "number_of_stuff_max_2h": 6.0,
            "number_of_stuff_sqr_2h": 36.0,
            "number_of_stuff_sum_2h": 6.0,
            "number_of_stuff_min_2h": 6.0,
            "number_of_stuff_max_24h": 6.0,
            "number_of_stuff_sqr_24h": 36.0,
            "number_of_stuff_sum_24h": 6.0,
            "number_of_stuff_min_24h": 6.0,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_avg_24h": 6.0,
            "col1": 6,
        },
        {
            "number_of_stuff_max_1h": 7.0,
            "number_of_stuff_max_2h": 7.0,
            "number_of_stuff_max_24h": 7.0,
            "number_of_stuff_sqr_1h": 85.0,
            "number_of_stuff_sqr_2h": 85.0,
            "number_of_stuff_sqr_24h": 85.0,
            "number_of_stuff_sum_1h": 13.0,
            "number_of_stuff_sum_2h": 13.0,
            "number_of_stuff_sum_24h": 13.0,
            "number_of_stuff_min_1h": 6.0,
            "number_of_stuff_min_2h": 6.0,
            "number_of_stuff_min_24h": 6.0,
            "number_of_stuff_avg_1h": 6.5,
            "number_of_stuff_avg_2h": 6.5,
            "number_of_stuff_avg_24h": 6.5,
            "col1": 7,
        },
        {
            "number_of_stuff_max_1h": 8.0,
            "number_of_stuff_sqr_1h": 64.0,
            "number_of_stuff_sum_1h": 8.0,
            "number_of_stuff_min_1h": 8.0,
            "number_of_stuff_max_2h": 8.0,
            "number_of_stuff_sqr_2h": 149.0,
            "number_of_stuff_sum_2h": 21.0,
            "number_of_stuff_min_2h": 6.0,
            "number_of_stuff_max_24h": 8.0,
            "number_of_stuff_sqr_24h": 149.0,
            "number_of_stuff_sum_24h": 21.0,
            "number_of_stuff_min_24h": 6.0,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_avg_24h": 7.0,
            "col1": 8,
        },
        {
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_max_2h": 9.0,
            "number_of_stuff_max_24h": 9.0,
            "number_of_stuff_sqr_1h": 145.0,
            "number_of_stuff_sqr_2h": 230.0,
            "number_of_stuff_sqr_24h": 230.0,
            "number_of_stuff_sum_1h": 17.0,
            "number_of_stuff_sum_2h": 30.0,
            "number_of_stuff_sum_24h": 30.0,
            "number_of_stuff_min_1h": 8.0,
            "number_of_stuff_min_2h": 6.0,
            "number_of_stuff_min_24h": 6.0,
            "number_of_stuff_avg_1h": 8.5,
            "number_of_stuff_avg_2h": 7.5,
            "number_of_stuff_avg_24h": 7.5,
            "col1": 9,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    table_name = setup_teardown_test.table_name
    tables = [
        table_name,
        table,
        Table(setup_teardown_test.table_name, setup_teardown_test.driver()),
    ]  # test on previous table and on new table
    expected_results = [
        {
            "col1": 10,
            "number_of_stuff_sum_1h": 17.0,
            "number_of_stuff_min_1h": 8.0,
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_avg_1h": 8.5,
        },
        {
            "col1": 10,
            "number_of_stuff_sum_1h": 0.0,
            "number_of_stuff_min_1h": math.inf,
            "number_of_stuff_max_1h": -math.inf,
            "number_of_stuff_avg_1h": math.nan,
        },
    ]
    for table in tables:
        if isinstance(table, str):
            tmp_table = Table(table_name, setup_teardown_test.driver())
            context = Context(initial_tables={table_name: tmp_table})
        else:
            context = None

        controller = build_flow(
            [
                SyncEmitSource(),
                QueryByKey(
                    [
                        "number_of_stuff_sum_1h",
                        "number_of_stuff_avg_1h",
                        "number_of_stuff_min_1h",
                        "number_of_stuff_max_1h",
                    ],
                    table,
                    context=context,
                ),
                Reduce([], lambda acc, x: append_return(acc, x)),
            ]
        ).run()

        base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
        data = {"col1": items_in_ingest_batch}
        controller.emit(data, "tal", base_time)
        controller.emit(data, "tal", base_time + timedelta(minutes=25))

        controller.terminate()
        actual = controller.await_termination()
        assert (
            actual == expected_results
        ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("use_parallel_operations", [True, False])
def test_query_virtual_aggregations_flow(setup_teardown_test, use_parallel_operations):
    table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(use_parallel_operations=use_parallel_operations),
    )
    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["avg", "stddev", "stdvar"],
                        SlidingWindows(["24h"], "10m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "dina", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_avg_24h": 0.0,
            "number_of_stuff_stddev_24h": math.nan,
            "number_of_stuff_stdvar_24h": math.nan,
        },
        {
            "col1": 1,
            "number_of_stuff_avg_24h": 0.5,
            "number_of_stuff_stddev_24h": math.sqrt(0.5),
            "number_of_stuff_stdvar_24h": 0.5,
        },
        {
            "col1": 2,
            "number_of_stuff_avg_24h": 1.0,
            "number_of_stuff_stddev_24h": 1.0,
            "number_of_stuff_stdvar_24h": 1.0,
        },
        {
            "col1": 3,
            "number_of_stuff_avg_24h": 1.5,
            "number_of_stuff_stddev_24h": math.sqrt(1.6666666666666667),
            "number_of_stuff_stdvar_24h": 1.6666666666666667,
        },
        {
            "col1": 4,
            "number_of_stuff_avg_24h": 2.0,
            "number_of_stuff_stddev_24h": math.sqrt(2.5),
            "number_of_stuff_stdvar_24h": 2.5,
        },
        {
            "col1": 5,
            "number_of_stuff_avg_24h": 2.5,
            "number_of_stuff_stddev_24h": math.sqrt(3.5),
            "number_of_stuff_stdvar_24h": 3.5,
        },
        {
            "col1": 6,
            "number_of_stuff_avg_24h": 3.0,
            "number_of_stuff_stddev_24h": math.sqrt(4.666666666666667),
            "number_of_stuff_stdvar_24h": 4.666666666666667,
        },
        {
            "col1": 7,
            "number_of_stuff_avg_24h": 3.5,
            "number_of_stuff_stddev_24h": math.sqrt(6.0),
            "number_of_stuff_stdvar_24h": 6.0,
        },
        {
            "col1": 8,
            "number_of_stuff_avg_24h": 4.0,
            "number_of_stuff_stddev_24h": math.sqrt(7.5),
            "number_of_stuff_stdvar_24h": 7.5,
        },
        {
            "col1": 9,
            "number_of_stuff_avg_24h": 4.5,
            "number_of_stuff_stddev_24h": math.sqrt(9.166666666666666),
            "number_of_stuff_stdvar_24h": 9.166666666666666,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(use_parallel_operations=use_parallel_operations),
    )
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                [
                    "number_of_stuff_avg_1h",
                    "number_of_stuff_stdvar_2h",
                    "number_of_stuff_stddev_3h",
                ],
                other_table,
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch}
    controller.emit(data, "dina", base_time)
    controller.emit(data, "dina", base_time + timedelta(minutes=25))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 10,
            "number_of_stuff_avg_1h": 8.5,
            "number_of_stuff_stdvar_2h": 1.6666666666666667,
            "number_of_stuff_stddev_3h": 1.8708286933869707,
        },
        {
            "col1": 10,
            "number_of_stuff_avg_1h": 9.0,
            "number_of_stuff_stdvar_2h": 1.0,
            "number_of_stuff_stddev_3h": 1.8708286933869707,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("partitioned_by_key", [True, False])
@pytest.mark.parametrize("flush_interval", [None, 1])
def test_query_aggregate_by_key(
    setup_teardown_test, partitioned_by_key, flush_interval
):
    table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        partitioned_by_key=partitioned_by_key,
        flush_interval_secs=flush_interval,
    )

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["sum", "avg", "min", "max", "sqr"],
                        SlidingWindows(["1h", "2h", "24h"], "10m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_1h": 0,
            "number_of_stuff_sum_2h": 0,
            "number_of_stuff_sum_24h": 0,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 0,
            "number_of_stuff_max_2h": 0,
            "number_of_stuff_max_24h": 0,
            "number_of_stuff_sqr_1h": 0,
            "number_of_stuff_sqr_2h": 0,
            "number_of_stuff_sqr_24h": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_avg_24h": 0.0,
            "col1": 0,
        },
        {
            "number_of_stuff_sum_1h": 1,
            "number_of_stuff_sum_2h": 1,
            "number_of_stuff_sum_24h": 1,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 1,
            "number_of_stuff_max_2h": 1,
            "number_of_stuff_max_24h": 1,
            "number_of_stuff_sqr_1h": 1,
            "number_of_stuff_sqr_2h": 1,
            "number_of_stuff_sqr_24h": 1,
            "number_of_stuff_avg_1h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_avg_24h": 0.5,
            "col1": 1,
        },
        {
            "number_of_stuff_sum_1h": 3,
            "number_of_stuff_sum_2h": 3,
            "number_of_stuff_sum_24h": 3,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 2,
            "number_of_stuff_max_2h": 2,
            "number_of_stuff_max_24h": 2,
            "number_of_stuff_sqr_1h": 5,
            "number_of_stuff_sqr_2h": 5,
            "number_of_stuff_sqr_24h": 5,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_avg_24h": 1.0,
            "col1": 2,
        },
        {
            "number_of_stuff_sum_1h": 6,
            "number_of_stuff_sum_2h": 6,
            "number_of_stuff_sum_24h": 6,
            "number_of_stuff_min_1h": 1,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 3,
            "number_of_stuff_max_2h": 3,
            "number_of_stuff_max_24h": 3,
            "number_of_stuff_sqr_1h": 14,
            "number_of_stuff_sqr_2h": 14,
            "number_of_stuff_sqr_24h": 14,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_avg_24h": 1.5,
            "col1": 3,
        },
        {
            "number_of_stuff_sum_1h": 9,
            "number_of_stuff_sum_2h": 10,
            "number_of_stuff_sum_24h": 10,
            "number_of_stuff_min_1h": 2,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 4,
            "number_of_stuff_max_2h": 4,
            "number_of_stuff_max_24h": 4,
            "number_of_stuff_sqr_1h": 29,
            "number_of_stuff_sqr_2h": 30,
            "number_of_stuff_sqr_24h": 30,
            "number_of_stuff_avg_1h": 3.0,
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_avg_24h": 2.0,
            "col1": 4,
        },
        {
            "number_of_stuff_sum_1h": 12,
            "number_of_stuff_sum_2h": 15,
            "number_of_stuff_sum_24h": 15,
            "number_of_stuff_min_1h": 3,
            "number_of_stuff_min_2h": 1,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 5,
            "number_of_stuff_max_2h": 5,
            "number_of_stuff_max_24h": 5,
            "number_of_stuff_sqr_1h": 50,
            "number_of_stuff_sqr_2h": 55,
            "number_of_stuff_sqr_24h": 55,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_avg_24h": 2.5,
            "col1": 5,
        },
        {
            "number_of_stuff_sum_1h": 15,
            "number_of_stuff_sum_2h": 20,
            "number_of_stuff_sum_24h": 21,
            "number_of_stuff_min_1h": 4,
            "number_of_stuff_min_2h": 2,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 6,
            "number_of_stuff_max_2h": 6,
            "number_of_stuff_max_24h": 6,
            "number_of_stuff_sqr_1h": 77,
            "number_of_stuff_sqr_2h": 90,
            "number_of_stuff_sqr_24h": 91,
            "number_of_stuff_avg_1h": 5.0,
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_avg_24h": 3.0,
            "col1": 6,
        },
        {
            "number_of_stuff_sum_1h": 18,
            "number_of_stuff_sum_2h": 25,
            "number_of_stuff_sum_24h": 28,
            "number_of_stuff_min_1h": 5,
            "number_of_stuff_min_2h": 3,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 7,
            "number_of_stuff_max_2h": 7,
            "number_of_stuff_max_24h": 7,
            "number_of_stuff_sqr_1h": 110,
            "number_of_stuff_sqr_2h": 135,
            "number_of_stuff_sqr_24h": 140,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_avg_24h": 3.5,
            "col1": 7,
        },
        {
            "number_of_stuff_sum_1h": 21,
            "number_of_stuff_sum_2h": 30,
            "number_of_stuff_sum_24h": 36,
            "number_of_stuff_min_1h": 6,
            "number_of_stuff_min_2h": 4,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 8,
            "number_of_stuff_max_2h": 8,
            "number_of_stuff_max_24h": 8,
            "number_of_stuff_sqr_1h": 149,
            "number_of_stuff_sqr_2h": 190,
            "number_of_stuff_sqr_24h": 204,
            "number_of_stuff_avg_1h": 7.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_avg_24h": 4.0,
            "col1": 8,
        },
        {
            "number_of_stuff_sum_1h": 24,
            "number_of_stuff_sum_2h": 35,
            "number_of_stuff_sum_24h": 45,
            "number_of_stuff_min_1h": 7,
            "number_of_stuff_min_2h": 5,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 9,
            "number_of_stuff_max_2h": 9,
            "number_of_stuff_max_24h": 9,
            "number_of_stuff_sqr_1h": 194,
            "number_of_stuff_sqr_2h": 255,
            "number_of_stuff_sqr_24h": 285,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_avg_24h": 4.5,
            "col1": 9,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                [
                    "number_of_stuff_sum_1h",
                    "number_of_stuff_sum_2h",
                    "number_of_stuff_sum_24h",
                    "number_of_stuff_avg_1h",
                    "number_of_stuff_avg_2h",
                    "number_of_stuff_avg_24h",
                    "number_of_stuff_min_1h",
                    "number_of_stuff_min_2h",
                    "number_of_stuff_min_24h",
                    "number_of_stuff_max_1h",
                    "number_of_stuff_max_2h",
                    "number_of_stuff_max_24h",
                ],
                other_table,
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch}
    controller.emit(data, "tal", base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 10,
            "number_of_stuff_sum_1h": 17,
            "number_of_stuff_sum_2h": 30,
            "number_of_stuff_sum_24h": 45,
            "number_of_stuff_min_1h": 8,
            "number_of_stuff_min_2h": 6,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 9,
            "number_of_stuff_max_2h": 9,
            "number_of_stuff_max_24h": 9,
            "number_of_stuff_avg_1h": 8.5,
            "number_of_stuff_avg_2h": 7.5,
            "number_of_stuff_avg_24h": 4.5,
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize(
    "query_aggregations",
    [
        ["number_of_stuff_sum_1h", "number_of_stuff_avg_2h"],
        ["number_of_stuff_avg_2h", "number_of_stuff_sum_1h"],
    ],
)
def test_aggregate_and_query_with_dependent_aggrs_different_windows(
    setup_teardown_test, query_aggregations
):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["sum", "avg"],
                        SlidingWindows(["1h", "2h"], "10m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_1h": 0,
            "number_of_stuff_sum_2h": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "col1": 0,
        },
        {
            "number_of_stuff_sum_1h": 1,
            "number_of_stuff_sum_2h": 1,
            "number_of_stuff_avg_1h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "col1": 1,
        },
        {
            "number_of_stuff_sum_1h": 3,
            "number_of_stuff_sum_2h": 3,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "col1": 2,
        },
        {
            "number_of_stuff_sum_1h": 6,
            "number_of_stuff_sum_2h": 6,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_2h": 1.5,
            "col1": 3,
        },
        {
            "number_of_stuff_sum_1h": 9,
            "number_of_stuff_sum_2h": 10,
            "number_of_stuff_avg_1h": 3.0,
            "number_of_stuff_avg_2h": 2.0,
            "col1": 4,
        },
        {
            "number_of_stuff_sum_1h": 12,
            "number_of_stuff_sum_2h": 15,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_2h": 3.0,
            "col1": 5,
        },
        {
            "number_of_stuff_sum_1h": 15,
            "number_of_stuff_sum_2h": 20,
            "number_of_stuff_avg_1h": 5.0,
            "number_of_stuff_avg_2h": 4.0,
            "col1": 6,
        },
        {
            "number_of_stuff_sum_1h": 18,
            "number_of_stuff_sum_2h": 25,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_2h": 5.0,
            "col1": 7,
        },
        {
            "number_of_stuff_sum_1h": 21,
            "number_of_stuff_sum_2h": 30,
            "number_of_stuff_avg_1h": 7.0,
            "number_of_stuff_avg_2h": 6.0,
            "col1": 8,
        },
        {
            "number_of_stuff_sum_1h": 24,
            "number_of_stuff_sum_2h": 35,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_2h": 7.0,
            "col1": 9,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(query_aggregations, other_table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch}
    controller.emit(data, "tal", base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"col1": 10, "number_of_stuff_sum_1h": 17, "number_of_stuff_avg_2h": 7.5}
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("partitioned_by_key", [True, False])
@pytest.mark.parametrize("flush_interval", [None, 1])
def test_aggregate_by_key_one_underlying_window(
    setup_teardown_test, partitioned_by_key, flush_interval
):
    expected = {
        1: [
            {"number_of_stuff_count_1h": 1, "other_stuff_sum_1h": 0.0, "col1": 0},
            {"number_of_stuff_count_1h": 2, "other_stuff_sum_1h": 1.0, "col1": 1},
            {"number_of_stuff_count_1h": 3, "other_stuff_sum_1h": 3.0, "col1": 2},
        ],
        2: [
            {"number_of_stuff_count_1h": 4, "other_stuff_sum_1h": 6.0, "col1": 3},
            {"number_of_stuff_count_1h": 5, "other_stuff_sum_1h": 10.0, "col1": 4},
            {"number_of_stuff_count_1h": 6, "other_stuff_sum_1h": 15.0, "col1": 5},
        ],
        3: [
            {"number_of_stuff_count_1h": 7, "other_stuff_sum_1h": 21.0, "col1": 6},
            {"number_of_stuff_count_1h": 8, "other_stuff_sum_1h": 28.0, "col1": 7},
            {"number_of_stuff_count_1h": 9, "other_stuff_sum_1h": 36.0, "col1": 8},
        ],
    }

    items_in_ingest_batch = 3
    current_index = 0

    for current_expected in expected.values():

        table = Table(
            setup_teardown_test.table_name,
            setup_teardown_test.driver(),
            partitioned_by_key=partitioned_by_key,
            flush_interval_secs=flush_interval,
        )
        controller = build_flow(
            [
                SyncEmitSource(),
                AggregateByKey(
                    [
                        FieldAggregator(
                            "number_of_stuff",
                            "col1",
                            ["count"],
                            SlidingWindows(["1h"], "10m"),
                        ),
                        FieldAggregator(
                            "other_stuff",
                            "col1",
                            ["sum"],
                            SlidingWindows(["1h"], "10m"),
                        ),
                    ],
                    table,
                ),
                NoSqlTarget(table),
                Reduce([], lambda acc, x: append_return(acc, x)),
            ]
        ).run()

        for i in range(items_in_ingest_batch):
            data = {"col1": current_index}
            controller.emit(
                data, "tal", test_base_time + timedelta(minutes=1 * current_index)
            )
            current_index = current_index + 1

        controller.terminate()
        actual = controller.await_termination()

        assert (
            actual == current_expected
        ), f"actual did not match expected. \n actual: {actual} \n expected: {current_expected}"


@pytest.mark.parametrize("partitioned_by_key", [True, False])
def test_aggregate_by_key_two_underlying_windows(
    setup_teardown_test, partitioned_by_key
):
    expected = {
        1: [
            {"number_of_stuff_count_24h": 1, "other_stuff_sum_24h": 0.0, "col1": 0},
            {"number_of_stuff_count_24h": 2, "other_stuff_sum_24h": 1.0, "col1": 1},
            {"number_of_stuff_count_24h": 3, "other_stuff_sum_24h": 3.0, "col1": 2},
        ],
        2: [
            {"number_of_stuff_count_24h": 4, "other_stuff_sum_24h": 6.0, "col1": 3},
            {"number_of_stuff_count_24h": 5, "other_stuff_sum_24h": 10.0, "col1": 4},
            {"number_of_stuff_count_24h": 6, "other_stuff_sum_24h": 15.0, "col1": 5},
        ],
        3: [
            {"number_of_stuff_count_24h": 7, "other_stuff_sum_24h": 21.0, "col1": 6},
            {"number_of_stuff_count_24h": 8, "other_stuff_sum_24h": 28.0, "col1": 7},
            {"number_of_stuff_count_24h": 9, "other_stuff_sum_24h": 36.0, "col1": 8},
        ],
    }

    items_in_ingest_batch = 3
    current_index = 0
    for current_expected in expected.values():

        table = Table(
            setup_teardown_test.table_name,
            setup_teardown_test.driver(),
            partitioned_by_key=partitioned_by_key,
        )
        controller = build_flow(
            [
                SyncEmitSource(),
                AggregateByKey(
                    [
                        FieldAggregator(
                            "number_of_stuff",
                            "col1",
                            ["count"],
                            SlidingWindows(["24h"], "10m"),
                        ),
                        FieldAggregator(
                            "other_stuff",
                            "col1",
                            ["sum"],
                            SlidingWindows(["24h"], "10m"),
                        ),
                    ],
                    table,
                ),
                NoSqlTarget(table),
                Reduce([], lambda acc, x: append_return(acc, x)),
            ]
        ).run()

        for i in range(items_in_ingest_batch):
            data = {"col1": current_index}
            controller.emit(
                data, "tal", test_base_time + timedelta(minutes=25 * current_index)
            )
            current_index = current_index + 1

        controller.terminate()
        actual = controller.await_termination()

        assert (
            actual == current_expected
        ), f"actual did not match expected. \n actual: {actual} \n expected: {current_expected}"


def test_aggregate_by_key_with_extra_aliases(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    table["tal"] = {"color": "blue", "age": 41, "iss": True, "sometime": test_base_time}

    def enrich(event, state):
        if "first_activity" not in state:
            state["first_activity"] = event.time

        event.body["time_since_activity"] = (
            event.time - state["first_activity"]
        ).seconds
        state["last_event"] = event.time
        event.body["total_activities"] = state["total_activities"] = (
            state.get("total_activities", 0) + 1
        )
        event.body["color"] = state["color"]
        return event, state

    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(table, enrich, group_by_key=True, full_event=True),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["sum", "avg"],
                        SlidingWindows(["2h"], "10m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_2h": 0,
            "number_of_stuff_avg_2h": 0.0,
            "col1": 0,
            "time_since_activity": 0,
            "total_activities": 1,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 1,
            "number_of_stuff_avg_2h": 0.5,
            "col1": 1,
            "time_since_activity": 1500,
            "total_activities": 2,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 3,
            "number_of_stuff_avg_2h": 1.0,
            "col1": 2,
            "time_since_activity": 3000,
            "total_activities": 3,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 6,
            "number_of_stuff_avg_2h": 1.5,
            "col1": 3,
            "time_since_activity": 4500,
            "total_activities": 4,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 10,
            "number_of_stuff_avg_2h": 2.0,
            "col1": 4,
            "time_since_activity": 6000,
            "total_activities": 5,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 15,
            "number_of_stuff_avg_2h": 3.0,
            "col1": 5,
            "time_since_activity": 7500,
            "total_activities": 6,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 20,
            "number_of_stuff_avg_2h": 4.0,
            "col1": 6,
            "time_since_activity": 9000,
            "total_activities": 7,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 25,
            "number_of_stuff_avg_2h": 5.0,
            "col1": 7,
            "time_since_activity": 10500,
            "total_activities": 8,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 30,
            "number_of_stuff_avg_2h": 6.0,
            "col1": 8,
            "time_since_activity": 12000,
            "total_activities": 9,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 35,
            "number_of_stuff_avg_2h": 7.0,
            "col1": 9,
            "time_since_activity": 13500,
            "total_activities": 10,
            "color": "blue",
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                [
                    "number_of_stuff_sum_2h",
                    "number_of_stuff_avg_2h",
                    "color",
                    "age",
                    "iss",
                    "sometime",
                ],
                other_table,
                aliases={
                    "color": "external.color",
                    "iss": "external.iss",
                    "number_of_stuff_avg_2h": "my_avg",
                },
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch}
    controller.emit(data, "tal", base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_2h": 30,
            "my_avg": 7.5,
            "col1": 10,
            "external.color": "blue",
            "age": 41,
            "external.iss": True,
            "sometime": test_base_time,
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("flush_interval", [None, 1])
def test_write_cache_with_aggregations(setup_teardown_test, flush_interval):
    table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        flush_interval_secs=flush_interval,
    )

    table["tal"] = {"color": "blue", "age": 41, "iss": True, "sometime": test_base_time}

    def enrich(event, state):
        if "first_activity" not in state:
            state["first_activity"] = event.time

        event.body["time_since_activity"] = (
            event.time - state["first_activity"]
        ).seconds
        state["last_event"] = event.time
        event.body["total_activities"] = state["total_activities"] = (
            state.get("total_activities", 0) + 1
        )
        event.body["color"] = state["color"]
        return event, state

    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(table, enrich, group_by_key=True, full_event=True),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["sum", "avg"],
                        SlidingWindows(["2h"], "10m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_2h": 0,
            "number_of_stuff_avg_2h": 0.0,
            "col1": 0,
            "time_since_activity": 0,
            "total_activities": 1,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 1,
            "number_of_stuff_avg_2h": 0.5,
            "col1": 1,
            "time_since_activity": 1500,
            "total_activities": 2,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 3,
            "number_of_stuff_avg_2h": 1.0,
            "col1": 2,
            "time_since_activity": 3000,
            "total_activities": 3,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 6,
            "number_of_stuff_avg_2h": 1.5,
            "col1": 3,
            "time_since_activity": 4500,
            "total_activities": 4,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 10,
            "number_of_stuff_avg_2h": 2.0,
            "col1": 4,
            "time_since_activity": 6000,
            "total_activities": 5,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 15,
            "number_of_stuff_avg_2h": 3.0,
            "col1": 5,
            "time_since_activity": 7500,
            "total_activities": 6,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 20,
            "number_of_stuff_avg_2h": 4.0,
            "col1": 6,
            "time_since_activity": 9000,
            "total_activities": 7,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 25,
            "number_of_stuff_avg_2h": 5.0,
            "col1": 7,
            "time_since_activity": 10500,
            "total_activities": 8,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 30,
            "number_of_stuff_avg_2h": 6.0,
            "col1": 8,
            "time_since_activity": 12000,
            "total_activities": 9,
            "color": "blue",
        },
        {
            "number_of_stuff_sum_2h": 35,
            "number_of_stuff_avg_2h": 7.0,
            "col1": 9,
            "time_since_activity": 13500,
            "total_activities": 10,
            "color": "blue",
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        flush_interval_secs=flush_interval,
    )

    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                [
                    "number_of_stuff_sum_2h",
                    "number_of_stuff_avg_2h",
                    "color",
                    "age",
                    "iss",
                    "sometime",
                ],
                other_table,
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch}
    controller.emit(data, "tal", base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_2h": 30,
            "number_of_stuff_avg_2h": 7.5,
            "col1": 10,
            "color": "blue",
            "age": 41,
            "iss": True,
            "sometime": test_base_time,
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("flush_interval", [None, 1])
def test_write_cache(setup_teardown_test, flush_interval):
    table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        flush_interval_secs=flush_interval,
    )

    table["tal"] = {"color": "blue", "age": 41, "iss": True, "sometime": test_base_time}

    def enrich(event, state):
        if "first_activity" not in state:
            state["first_activity"] = event.time

        event.body["time_since_activity"] = (
            event.time - state["first_activity"]
        ).seconds
        state["last_event"] = event.time
        event.body["total_activities"] = state["total_activities"] = (
            state.get("total_activities", 0) + 1
        )
        event.body["color"] = state["color"]
        return event, state

    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(table, enrich, group_by_key=True, full_event=True),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"col1": 0, "time_since_activity": 0, "total_activities": 1, "color": "blue"},
        {
            "col1": 1,
            "time_since_activity": 1500,
            "total_activities": 2,
            "color": "blue",
        },
        {
            "col1": 2,
            "time_since_activity": 3000,
            "total_activities": 3,
            "color": "blue",
        },
        {
            "col1": 3,
            "time_since_activity": 4500,
            "total_activities": 4,
            "color": "blue",
        },
        {
            "col1": 4,
            "time_since_activity": 6000,
            "total_activities": 5,
            "color": "blue",
        },
        {
            "col1": 5,
            "time_since_activity": 7500,
            "total_activities": 6,
            "color": "blue",
        },
        {
            "col1": 6,
            "time_since_activity": 9000,
            "total_activities": 7,
            "color": "blue",
        },
        {
            "col1": 7,
            "time_since_activity": 10500,
            "total_activities": 8,
            "color": "blue",
        },
        {
            "col1": 8,
            "time_since_activity": 12000,
            "total_activities": 9,
            "color": "blue",
        },
        {
            "col1": 9,
            "time_since_activity": 13500,
            "total_activities": 10,
            "color": "blue",
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(other_table, enrich, group_by_key=True, full_event=True),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch}
    controller.emit(data, "tal", base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 10,
            "time_since_activity": 15000,
            "total_activities": 11,
            "color": "blue",
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_aggregate_with_string_table(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    table_name = "tals-table"
    context = Context(initial_tables={table_name: table})
    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["sum", "avg", "min", "max", "sqr"],
                        SlidingWindows(["1h", "2h", "24h"], "10m"),
                    )
                ],
                table_name,
                context=context,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_1h": 0,
            "number_of_stuff_sum_2h": 0,
            "number_of_stuff_sum_24h": 0,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 0,
            "number_of_stuff_max_2h": 0,
            "number_of_stuff_max_24h": 0,
            "number_of_stuff_sqr_1h": 0,
            "number_of_stuff_sqr_2h": 0,
            "number_of_stuff_sqr_24h": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_avg_24h": 0.0,
            "col1": 0,
        },
        {
            "number_of_stuff_sum_1h": 1,
            "number_of_stuff_sum_2h": 1,
            "number_of_stuff_sum_24h": 1,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 1,
            "number_of_stuff_max_2h": 1,
            "number_of_stuff_max_24h": 1,
            "number_of_stuff_sqr_1h": 1,
            "number_of_stuff_sqr_2h": 1,
            "number_of_stuff_sqr_24h": 1,
            "number_of_stuff_avg_1h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_avg_24h": 0.5,
            "col1": 1,
        },
        {
            "number_of_stuff_sum_1h": 3,
            "number_of_stuff_sum_2h": 3,
            "number_of_stuff_sum_24h": 3,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 2,
            "number_of_stuff_max_2h": 2,
            "number_of_stuff_max_24h": 2,
            "number_of_stuff_sqr_1h": 5,
            "number_of_stuff_sqr_2h": 5,
            "number_of_stuff_sqr_24h": 5,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_avg_24h": 1.0,
            "col1": 2,
        },
        {
            "number_of_stuff_sum_1h": 6,
            "number_of_stuff_sum_2h": 6,
            "number_of_stuff_sum_24h": 6,
            "number_of_stuff_min_1h": 1,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 3,
            "number_of_stuff_max_2h": 3,
            "number_of_stuff_max_24h": 3,
            "number_of_stuff_sqr_1h": 14,
            "number_of_stuff_sqr_2h": 14,
            "number_of_stuff_sqr_24h": 14,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_avg_24h": 1.5,
            "col1": 3,
        },
        {
            "number_of_stuff_sum_1h": 9,
            "number_of_stuff_sum_2h": 10,
            "number_of_stuff_sum_24h": 10,
            "number_of_stuff_min_1h": 2,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 4,
            "number_of_stuff_max_2h": 4,
            "number_of_stuff_max_24h": 4,
            "number_of_stuff_sqr_1h": 29,
            "number_of_stuff_sqr_2h": 30,
            "number_of_stuff_sqr_24h": 30,
            "number_of_stuff_avg_1h": 3.0,
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_avg_24h": 2.0,
            "col1": 4,
        },
        {
            "number_of_stuff_sum_1h": 12,
            "number_of_stuff_sum_2h": 15,
            "number_of_stuff_sum_24h": 15,
            "number_of_stuff_min_1h": 3,
            "number_of_stuff_min_2h": 1,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 5,
            "number_of_stuff_max_2h": 5,
            "number_of_stuff_max_24h": 5,
            "number_of_stuff_sqr_1h": 50,
            "number_of_stuff_sqr_2h": 55,
            "number_of_stuff_sqr_24h": 55,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_avg_24h": 2.5,
            "col1": 5,
        },
        {
            "number_of_stuff_sum_1h": 15,
            "number_of_stuff_sum_2h": 20,
            "number_of_stuff_sum_24h": 21,
            "number_of_stuff_min_1h": 4,
            "number_of_stuff_min_2h": 2,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 6,
            "number_of_stuff_max_2h": 6,
            "number_of_stuff_max_24h": 6,
            "number_of_stuff_sqr_1h": 77,
            "number_of_stuff_sqr_2h": 90,
            "number_of_stuff_sqr_24h": 91,
            "number_of_stuff_avg_1h": 5.0,
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_avg_24h": 3.0,
            "col1": 6,
        },
        {
            "number_of_stuff_sum_1h": 18,
            "number_of_stuff_sum_2h": 25,
            "number_of_stuff_sum_24h": 28,
            "number_of_stuff_min_1h": 5,
            "number_of_stuff_min_2h": 3,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 7,
            "number_of_stuff_max_2h": 7,
            "number_of_stuff_max_24h": 7,
            "number_of_stuff_sqr_1h": 110,
            "number_of_stuff_sqr_2h": 135,
            "number_of_stuff_sqr_24h": 140,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_avg_24h": 3.5,
            "col1": 7,
        },
        {
            "number_of_stuff_sum_1h": 21,
            "number_of_stuff_sum_2h": 30,
            "number_of_stuff_sum_24h": 36,
            "number_of_stuff_min_1h": 6,
            "number_of_stuff_min_2h": 4,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 8,
            "number_of_stuff_max_2h": 8,
            "number_of_stuff_max_24h": 8,
            "number_of_stuff_sqr_1h": 149,
            "number_of_stuff_sqr_2h": 190,
            "number_of_stuff_sqr_24h": 204,
            "number_of_stuff_avg_1h": 7.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_avg_24h": 4.0,
            "col1": 8,
        },
        {
            "number_of_stuff_sum_1h": 24,
            "number_of_stuff_sum_2h": 35,
            "number_of_stuff_sum_24h": 45,
            "number_of_stuff_min_1h": 7,
            "number_of_stuff_min_2h": 5,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 9,
            "number_of_stuff_max_2h": 9,
            "number_of_stuff_max_24h": 9,
            "number_of_stuff_sqr_1h": 194,
            "number_of_stuff_sqr_2h": 255,
            "number_of_stuff_sqr_24h": 285,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_avg_24h": 4.5,
            "col1": 9,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def _assert_schema_equal(actual, expected):
    assert len(actual) == len(expected)
    for key, item in actual.items():
        current_expected = expected[key]
        assert item["period_millis"] == current_expected["period_millis"]
        assert set(item["aggregates"]) == set(current_expected["aggregates"])


async def load_schema(setup_teardown_test):
    driver = setup_teardown_test.driver()
    path = setup_teardown_test.table_name
    container, table_path = _split_path(path)
    res = await driver._load_schema(container, table_path)
    await driver.close()
    return res


def test_modify_schema(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["sum", "avg", "min", "max"],
                        SlidingWindows(["1h", "2h", "24h"], "10m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_sum_1h": 0,
            "number_of_stuff_sum_2h": 0,
            "number_of_stuff_sum_24h": 0,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 0,
            "number_of_stuff_max_2h": 0,
            "number_of_stuff_max_24h": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_avg_24h": 0.0,
        },
        {
            "col1": 1,
            "number_of_stuff_sum_1h": 1,
            "number_of_stuff_sum_2h": 1,
            "number_of_stuff_sum_24h": 1,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 1,
            "number_of_stuff_max_2h": 1,
            "number_of_stuff_max_24h": 1,
            "number_of_stuff_avg_1h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_avg_24h": 0.5,
        },
        {
            "col1": 2,
            "number_of_stuff_sum_1h": 3,
            "number_of_stuff_sum_2h": 3,
            "number_of_stuff_sum_24h": 3,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 2,
            "number_of_stuff_max_2h": 2,
            "number_of_stuff_max_24h": 2,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_avg_24h": 1.0,
        },
        {
            "col1": 3,
            "number_of_stuff_sum_1h": 6,
            "number_of_stuff_sum_2h": 6,
            "number_of_stuff_sum_24h": 6,
            "number_of_stuff_min_1h": 1,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 3,
            "number_of_stuff_max_2h": 3,
            "number_of_stuff_max_24h": 3,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_avg_24h": 1.5,
        },
        {
            "col1": 4,
            "number_of_stuff_sum_1h": 9,
            "number_of_stuff_sum_2h": 10,
            "number_of_stuff_sum_24h": 10,
            "number_of_stuff_min_1h": 2,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 4,
            "number_of_stuff_max_2h": 4,
            "number_of_stuff_max_24h": 4,
            "number_of_stuff_avg_1h": 3.0,
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_avg_24h": 2.0,
        },
        {
            "col1": 5,
            "number_of_stuff_sum_1h": 12,
            "number_of_stuff_sum_2h": 15,
            "number_of_stuff_sum_24h": 15,
            "number_of_stuff_min_1h": 3,
            "number_of_stuff_min_2h": 1,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 5,
            "number_of_stuff_max_2h": 5,
            "number_of_stuff_max_24h": 5,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_avg_24h": 2.5,
        },
        {
            "col1": 6,
            "number_of_stuff_sum_1h": 15,
            "number_of_stuff_sum_2h": 20,
            "number_of_stuff_sum_24h": 21,
            "number_of_stuff_min_1h": 4,
            "number_of_stuff_min_2h": 2,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 6,
            "number_of_stuff_max_2h": 6,
            "number_of_stuff_max_24h": 6,
            "number_of_stuff_avg_1h": 5.0,
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_avg_24h": 3.0,
        },
        {
            "col1": 7,
            "number_of_stuff_sum_1h": 18,
            "number_of_stuff_sum_2h": 25,
            "number_of_stuff_sum_24h": 28,
            "number_of_stuff_min_1h": 5,
            "number_of_stuff_min_2h": 3,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 7,
            "number_of_stuff_max_2h": 7,
            "number_of_stuff_max_24h": 7,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_avg_24h": 3.5,
        },
        {
            "col1": 8,
            "number_of_stuff_sum_1h": 21,
            "number_of_stuff_sum_2h": 30,
            "number_of_stuff_sum_24h": 36,
            "number_of_stuff_min_1h": 6,
            "number_of_stuff_min_2h": 4,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 8,
            "number_of_stuff_max_2h": 8,
            "number_of_stuff_max_24h": 8,
            "number_of_stuff_avg_1h": 7.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_avg_24h": 4.0,
        },
        {
            "col1": 9,
            "number_of_stuff_sum_1h": 24,
            "number_of_stuff_sum_2h": 35,
            "number_of_stuff_sum_24h": 45,
            "number_of_stuff_min_1h": 7,
            "number_of_stuff_min_2h": 5,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 9,
            "number_of_stuff_max_2h": 9,
            "number_of_stuff_max_24h": 9,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_avg_24h": 4.5,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    schema = asyncio.run(load_schema(setup_teardown_test))
    expected_schema = {
        "number_of_stuff": {
            "period_millis": 600000,
            "aggregates": ["max", "min", "sum", "count"],
        }
    }
    _assert_schema_equal(schema, expected_schema)

    driver = setup_teardown_test.driver()
    other_table = Table(setup_teardown_test.table_name, driver)
    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["sum", "avg", "min", "max"],
                        SlidingWindows(["1h", "2h", "24h"], "10m"),
                    ),
                    FieldAggregator(
                        "new_aggr",
                        "col1",
                        ["min", "max"],
                        SlidingWindows(["3h"], "10m"),
                    ),
                ],
                other_table,
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch}
    controller.emit(data, "tal", base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 10,
            "number_of_stuff_sum_1h": 27,
            "number_of_stuff_sum_2h": 40,
            "number_of_stuff_sum_24h": 55,
            "number_of_stuff_min_1h": 8,
            "number_of_stuff_min_2h": 6,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 10,
            "number_of_stuff_max_2h": 10,
            "number_of_stuff_max_24h": 10,
            "new_aggr_min_3h": 10,
            "new_aggr_max_3h": 10,
            "number_of_stuff_avg_1h": 9.0,
            "number_of_stuff_avg_2h": 8.0,
            "number_of_stuff_avg_24h": 5.0,
            "col1": 10,
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    schema = asyncio.run(load_schema(setup_teardown_test))
    expected_schema = {
        "number_of_stuff": {
            "period_millis": 600000,
            "aggregates": ["sum", "max", "min", "count"],
        },
        "new_aggr": {"period_millis": 600000, "aggregates": ["min", "max"]},
    }
    _assert_schema_equal(schema, expected_schema)


def test_invalid_modify_schema(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["sum", "avg", "min", "max"],
                        SlidingWindows(["1h", "2h", "24h"], "10m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_sum_1h": 0,
            "number_of_stuff_sum_2h": 0,
            "number_of_stuff_sum_24h": 0,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 0,
            "number_of_stuff_max_2h": 0,
            "number_of_stuff_max_24h": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_avg_24h": 0.0,
        },
        {
            "col1": 1,
            "number_of_stuff_sum_1h": 1,
            "number_of_stuff_sum_2h": 1,
            "number_of_stuff_sum_24h": 1,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 1,
            "number_of_stuff_max_2h": 1,
            "number_of_stuff_max_24h": 1,
            "number_of_stuff_avg_1h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_avg_24h": 0.5,
        },
        {
            "col1": 2,
            "number_of_stuff_sum_1h": 3,
            "number_of_stuff_sum_2h": 3,
            "number_of_stuff_sum_24h": 3,
            "number_of_stuff_min_1h": 0,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 2,
            "number_of_stuff_max_2h": 2,
            "number_of_stuff_max_24h": 2,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_avg_24h": 1.0,
        },
        {
            "col1": 3,
            "number_of_stuff_sum_1h": 6,
            "number_of_stuff_sum_2h": 6,
            "number_of_stuff_sum_24h": 6,
            "number_of_stuff_min_1h": 1,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 3,
            "number_of_stuff_max_2h": 3,
            "number_of_stuff_max_24h": 3,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_avg_24h": 1.5,
        },
        {
            "col1": 4,
            "number_of_stuff_sum_1h": 9,
            "number_of_stuff_sum_2h": 10,
            "number_of_stuff_sum_24h": 10,
            "number_of_stuff_min_1h": 2,
            "number_of_stuff_min_2h": 0,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 4,
            "number_of_stuff_max_2h": 4,
            "number_of_stuff_max_24h": 4,
            "number_of_stuff_avg_1h": 3.0,
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_avg_24h": 2.0,
        },
        {
            "col1": 5,
            "number_of_stuff_sum_1h": 12,
            "number_of_stuff_sum_2h": 15,
            "number_of_stuff_sum_24h": 15,
            "number_of_stuff_min_1h": 3,
            "number_of_stuff_min_2h": 1,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 5,
            "number_of_stuff_max_2h": 5,
            "number_of_stuff_max_24h": 5,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_avg_24h": 2.5,
        },
        {
            "col1": 6,
            "number_of_stuff_sum_1h": 15,
            "number_of_stuff_sum_2h": 20,
            "number_of_stuff_sum_24h": 21,
            "number_of_stuff_min_1h": 4,
            "number_of_stuff_min_2h": 2,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 6,
            "number_of_stuff_max_2h": 6,
            "number_of_stuff_max_24h": 6,
            "number_of_stuff_avg_1h": 5.0,
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_avg_24h": 3.0,
        },
        {
            "col1": 7,
            "number_of_stuff_sum_1h": 18,
            "number_of_stuff_sum_2h": 25,
            "number_of_stuff_sum_24h": 28,
            "number_of_stuff_min_1h": 5,
            "number_of_stuff_min_2h": 3,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 7,
            "number_of_stuff_max_2h": 7,
            "number_of_stuff_max_24h": 7,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_avg_24h": 3.5,
        },
        {
            "col1": 8,
            "number_of_stuff_sum_1h": 21,
            "number_of_stuff_sum_2h": 30,
            "number_of_stuff_sum_24h": 36,
            "number_of_stuff_min_1h": 6,
            "number_of_stuff_min_2h": 4,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 8,
            "number_of_stuff_max_2h": 8,
            "number_of_stuff_max_24h": 8,
            "number_of_stuff_avg_1h": 7.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_avg_24h": 4.0,
        },
        {
            "col1": 9,
            "number_of_stuff_sum_1h": 24,
            "number_of_stuff_sum_2h": 35,
            "number_of_stuff_sum_24h": 45,
            "number_of_stuff_min_1h": 7,
            "number_of_stuff_min_2h": 5,
            "number_of_stuff_min_24h": 0,
            "number_of_stuff_max_1h": 9,
            "number_of_stuff_max_2h": 9,
            "number_of_stuff_max_24h": 9,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_avg_24h": 4.5,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    schema = asyncio.run(load_schema(setup_teardown_test))
    expected_schema = {
        "number_of_stuff": {
            "period_millis": 600000,
            "aggregates": ["max", "min", "sum", "count"],
        }
    }
    _assert_schema_equal(schema, expected_schema)

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    try:
        controller = build_flow(
            [
                SyncEmitSource(),
                AggregateByKey(
                    [
                        FieldAggregator(
                            "number_of_stuff",
                            "col1",
                            ["sum", "avg", "min", "max"],
                            SlidingWindows(["1h", "24h"], "3m"),
                        )
                    ],
                    other_table,
                ),
                Reduce([], lambda acc, x: append_return(acc, x)),
            ]
        ).run()

        base_time = test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
        data = {"col1": items_in_ingest_batch}
        controller.emit(data, "tal", base_time)

        controller.terminate()
        controller.await_termination()
    except ValueError:
        pass


def test_query_aggregate_by_key_sliding_window_new_time_exceeds_stored_window(
    setup_teardown_test,
):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["count"],
                        SlidingWindows(["30m", "2h"], "1m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(hours=i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"col1": 0, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 1},
        {"col1": 1, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 2},
        {"col1": 2, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 2},
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                ["number_of_stuff_count_30m", "number_of_stuff_count_2h"], other_table
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(hours=items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch}
    controller.emit(data, "tal", base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"col1": 3, "number_of_stuff_count_30m": 0, "number_of_stuff_count_2h": 1}
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_query_aggregate_by_key_fixed_window_new_time_exceeds_stored_window(
    setup_teardown_test,
):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["count"],
                        FixedWindows(["30m", "2h"]),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=45 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"col1": 0, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 1},
        {"col1": 1, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 1},
        {"col1": 2, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 2},
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                ["number_of_stuff_count_30m", "number_of_stuff_count_2h"], other_table
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(hours=items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch}
    controller.emit(data, "tal", base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"col1": 3, "number_of_stuff_count_30m": 0, "number_of_stuff_count_2h": 0}
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_sliding_query_time_exceeds_stored_window_by_more_than_window(
    setup_teardown_test,
):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["count"],
                        SlidingWindows(["30m", "2h"], "1m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(hours=i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"col1": 0, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 1},
        {"col1": 1, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 2},
        {"col1": 2, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 2},
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                ["number_of_stuff_count_30m", "number_of_stuff_count_2h"], other_table
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(days=10)
    data = {"col1": items_in_ingest_batch}
    controller.emit(data, "tal", base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"col1": 3, "number_of_stuff_count_30m": 0, "number_of_stuff_count_2h": 0}
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_fixed_query_time_exceeds_stored_window_by_more_than_window(
    setup_teardown_test,
):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["count"],
                        FixedWindows(["30m", "2h"]),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=45 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"col1": 0, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 1},
        {"col1": 1, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 1},
        {"col1": 2, "number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 2},
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                ["number_of_stuff_count_30m", "number_of_stuff_count_2h"], other_table
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(days=10)
    data = {"col1": items_in_ingest_batch}
    controller.emit(data, "tal", base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"col1": 3, "number_of_stuff_count_30m": 0, "number_of_stuff_count_2h": 0}
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_write_to_table_reuse(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    flow = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["count"],
                        FixedWindows(["30m", "2h"]),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    )
    items_in_ingest_batch = 3

    expected_results = [
        [
            {"number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 1, "col1": 0},
            {"number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 1, "col1": 1},
            {"number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 2, "col1": 2},
        ],
        [
            {"number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 1, "col1": 0},
            {"number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 1, "col1": 1},
            {"number_of_stuff_count_30m": 1, "number_of_stuff_count_2h": 2, "col1": 2},
            {"col1": 0},
            {"number_of_stuff_count_30m": 2, "number_of_stuff_count_2h": 2, "col1": 1},
            {"number_of_stuff_count_30m": 2, "number_of_stuff_count_2h": 3, "col1": 2},
        ],
    ]

    for iteration in range(2):
        controller = flow.run()
        for i in range(items_in_ingest_batch):
            data = {"col1": i}
            controller.emit(data, "tal", test_base_time + timedelta(minutes=45 * i))

        controller.terminate()
        actual = controller.await_termination()
        assert actual == expected_results[iteration]


def test_aggregate_multiple_keys(setup_teardown_test):
    t0 = pd.Timestamp(test_base_time)
    data = pd.DataFrame(
        {
            "first_name": ["moshe", "yosi", "yosi"],
            "last_name": ["cohen", "levi", "levi"],
            "some_data": [1, 2, 3],
            "time": [
                t0 - pd.Timedelta(minutes=25),
                t0 - pd.Timedelta(minutes=30),
                t0 - pd.Timedelta(minutes=35),
            ],
        }
    )

    keys = ["first_name", "last_name"]
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            DataframeSource(data, key_field=keys, time_field="time"),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "some_data",
                        ["sum"],
                        SlidingWindows(["1h"], "10m"),
                    )
                ],
                table,
                emit_policy=EmitAfterMaxEvent(1),
            ),
            NoSqlTarget(table),
        ]
    ).run()

    actual = controller.await_termination()

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                ["number_of_stuff_sum_1h"], other_table, key=["first_name", "last_name"]
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit(
        {"first_name": "moshe", "last_name": "cohen", "some_data": 4},
        ["moshe", "cohen"],
        event_time=test_base_time,
    )
    controller.emit(
        {"first_name": "moshe", "last_name": "levi", "some_data": 5},
        ["moshe", "levi"],
        event_time=test_base_time,
    )
    controller.emit(
        {"first_name": "yosi", "last_name": "levi", "some_data": 6},
        ["yosi", "levi"],
        event_time=test_base_time,
    )

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_1h": 1.0,
            "first_name": "moshe",
            "last_name": "cohen",
            "some_data": 4,
        },
        {"first_name": "moshe", "last_name": "levi", "some_data": 5},
        {
            "number_of_stuff_sum_1h": 5.0,
            "first_name": "yosi",
            "last_name": "levi",
            "some_data": 6,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_aggregate_multiple_keys_and_aggregationless_query(setup_teardown_test):
    t0 = pd.Timestamp(test_base_time)
    data = pd.DataFrame(
        {
            "first_name": ["moshe", "yosi", "yosi"],
            "last_name": ["cohen", "levi", "levi"],
            "some_data": [1, 2, 3],
            "time": [
                t0 - pd.Timedelta(minutes=25),
                t0 - pd.Timedelta(minutes=30),
                t0 - pd.Timedelta(minutes=35),
            ],
        }
    )

    keys = ["first_name", "last_name"]
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            DataframeSource(data, key_field=keys, time_field="time"),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "some_data",
                        ["sum"],
                        SlidingWindows(["1h"], "10m"),
                    )
                ],
                table,
                emit_policy=EmitAfterMaxEvent(1),
            ),
            NoSqlTarget(table),
        ]
    ).run()

    controller.await_termination()

    other_table = Table(
        setup_teardown_test.table_name, setup_teardown_test.driver(True)
    )

    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                ["number_of_stuff_sum_1h"], other_table, key=["first_name", "last_name"]
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit(
        {"first_name": "moshe", "last_name": "cohen", "some_data": 4},
        ["moshe", "cohen"],
        event_time=test_base_time,
    )
    controller.emit(
        {"first_name": "moshe", "last_name": "levi", "some_data": 5},
        ["moshe", "levi"],
        event_time=test_base_time,
    )
    controller.emit(
        {"first_name": "yosi", "last_name": "levi", "some_data": 6},
        ["yosi", "levi"],
        event_time=test_base_time,
    )

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_1h": 1.0,
            "first_name": "moshe",
            "last_name": "cohen",
            "some_data": 4,
        },
        {"first_name": "moshe", "last_name": "levi", "some_data": 5},
        # number_of_stuff_sum_1h is 2 because the events were inserted out of order, and reading back aggregationless takes
        # the value relative to the event time of the last event that was inserted.
        {
            "number_of_stuff_sum_1h": 2.0,
            "first_name": "yosi",
            "last_name": "levi",
            "some_data": 6,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_read_non_existing_key(setup_teardown_test):
    data = pd.DataFrame(
        {
            "first_name": ["moshe", "yosi", "yosi"],
            "last_name": ["cohen", "levi", "levi"],
            "some_data": [1, 2, 3],
            "time": [
                test_base_time - pd.Timedelta(minutes=25),
                test_base_time - pd.Timedelta(minutes=30),
                test_base_time - pd.Timedelta(minutes=35),
            ],
        }
    )

    keys = "first_name"
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            DataframeSource(data, key_field=keys),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "some_data",
                        ["sum"],
                        SlidingWindows(["1h"], "10m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
        ]
    ).run()

    actual = controller.await_termination()

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["number_of_stuff_sum_1h"], other_table, keys="first_name"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"last_name": "levi", "some_data": 5}, "non_existing_key")

    controller.terminate()
    actual = controller.await_termination()

    assert "number_of_stuff_sum_1h" not in actual[0]


def test_concurrent_updates_to_kv_table(setup_teardown_test):
    table1 = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        flush_interval_secs=None,
    )
    table2 = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        flush_interval_secs=None,
    )
    controller1 = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "attr1", "attr1", ["sum"], SlidingWindows(["1h"], "10m")
                    )
                ],
                table1,
            ),
            NoSqlTarget(table1),
        ]
    ).run()
    controller2 = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "attr2", "attr2", ["sum"], SlidingWindows(["1h"], "10m")
                    )
                ],
                table2,
            ),
            NoSqlTarget(table2),
        ]
    ).run()

    try:
        for i in range(10):
            controller1.emit({"attr1": i}, key="onekey", event_time=test_base_time)
            controller2.emit({"attr2": i}, key="onekey", event_time=test_base_time)
    finally:
        controller1.terminate()
        controller2.terminate()
        controller1.await_termination()
        controller2.await_termination()

    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["attr1", "attr2"], table, key="mykey"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"mykey": "onekey"}, event_time=test_base_time)

    controller.terminate()
    result = controller.await_termination()

    assert result == [{"mykey": "onekey", "attr1": 9, "attr2": 9}]


def test_separate_aggregate_steps(setup_teardown_test):
    def map_multiply(x):
        x["some_data"] = x["some_data"] * 10
        return x

    t0 = pd.Timestamp(test_base_time)
    data = pd.DataFrame(
        {
            "first_name": ["moshe", "yosi", "katya"],
            "some_data": [1, 2, 3],
            "time": [
                t0 - pd.Timedelta(minutes=25),
                t0 - pd.Timedelta(minutes=30),
                t0 - pd.Timedelta(minutes=35),
            ],
        }
    )

    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow(
        [
            DataframeSource(data, key_field="first_name", time_field="time"),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "some_data",
                        ["avg"],
                        SlidingWindows(["1h"], "10m"),
                    )
                ],
                table,
            ),
            Map(map_multiply),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff2",
                        "some_data",
                        ["sum"],
                        SlidingWindows(["2h"], "10m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
        ]
    ).run()

    controller.await_termination()

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                ["number_of_stuff_avg_1h", "number_of_stuff2_sum_2h"],
                other_table,
                key=["first_name"],
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"first_name": "moshe"}, ["moshe"], event_time=test_base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff2_sum_2h": 11.0,
            "number_of_stuff_avg_1h": 5.5,
            "first_name": "moshe",
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_write_read_first_last(setup_teardown_test):

    table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        flush_interval_secs=None,
    )
    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "attr", "attr", ["first", "last"], SlidingWindows(["1h"], "10m")
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
        ]
    ).run()

    try:
        for i in range(1, 10):
            controller.emit(
                {"attr": i},
                key="onekey",
                event_time=test_base_time + timedelta(minutes=i),
            )
            controller.emit(
                {"attr": i * 10},
                key="onekey",
                event_time=test_base_time + timedelta(hours=1, minutes=i),
            )
    finally:
        controller.terminate()
        controller.await_termination()

    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["attr_first_1h", "attr_last_1h"], table, key="mykey"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit(
        {"mykey": "onekey"}, event_time=test_base_time + timedelta(minutes=10)
    )
    controller.emit(
        {"mykey": "onekey"}, event_time=test_base_time + timedelta(hours=1, minutes=10)
    )

    controller.terminate()
    result = controller.await_termination()

    assert result == [
        {"mykey": "onekey", "attr_first_1h": 1.0, "attr_last_1h": 9.0},
        {"mykey": "onekey", "attr_first_1h": 10.0, "attr_last_1h": 90.0},
    ]


def test_non_existing_key_query_by_key_from_v3io_key_is_list(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    df = pd.DataFrame(
        [["katya", "green", "hod hasharon"], ["dina", "blue", "ramat gan"]],
        columns=["name", "color", "city"],
    )
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
            QueryByKey(["color"], table, key=["name"]),
            QueryByKey(["city"], table, key="name"),
        ]
    ).run()

    controller.emit({"nameeeee": "katya"}, "katya")
    controller.terminate()
    controller.await_termination()


def test_multiple_keys_int(setup_teardown_test):
    t0 = pd.Timestamp(test_base_time)
    data = pd.DataFrame(
        {
            "key_column1": [10, 20],
            "key_column2": [30, 40],
            "key_column3": [5, 6],
            "key_column4": [50, 60],
            "some_data": [1, 2],
            "time": [t0 - pd.Timedelta(minutes=25), t0 - pd.Timedelta(minutes=30)],
        }
    )

    keys = ["key_column1", "key_column2", "key_column3", "key_column4"]
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            DataframeSource(data, key_field=keys, time_field="time"),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "some_data",
                        ["sum"],
                        SlidingWindows(["1h"], "10m"),
                    )
                ],
                table,
                emit_policy=EmitAfterMaxEvent(1),
            ),
            NoSqlTarget(table),
        ]
    ).run()

    actual = controller.await_termination()

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["number_of_stuff_sum_1h"], other_table, key=keys),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit(
        {"key_column1": 10, "key_column2": 30, "key_column3": 5, "key_column4": 50},
        key=[10, 30, 5, 50],
        event_time=test_base_time,
    )

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_1h": 1.0,
            "key_column1": 10,
            "key_column2": 30,
            "key_column3": 5,
            "key_column4": 50,
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_column_begin_t(setup_teardown_test):
    t0 = pd.Timestamp(test_base_time)
    data = pd.DataFrame(
        {
            "key_column": ["a", "b"],
            "some_data": [1, 2],
            "t_col": ["storey", "rules"],
            "time": [t0 - pd.Timedelta(minutes=25), t0 - pd.Timedelta(minutes=30)],
        }
    )

    keys = ["key_column"]
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            DataframeSource(data, key_field=keys, time_field="time"),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "some_data",
                        ["sum"],
                        SlidingWindows(["1h"], "10m"),
                    )
                ],
                table,
                emit_policy=EmitAfterMaxEvent(1),
            ),
            NoSqlTarget(table),
        ]
    ).run()

    controller.await_termination()

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["number_of_stuff_sum_1h", "t_col"], other_table, key=keys),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"key_column": "a"}, key=["a"], event_time=test_base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"number_of_stuff_sum_1h": 1.0, "key_column": "a", "t_col": "storey"}
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_aggregate_float_key(setup_teardown_test):
    t0 = pd.Timestamp(test_base_time)
    data = pd.DataFrame(
        {
            "key_column2": [5.6, 8.6],
            "some_data": [1, 2],
            "time": [t0 - pd.Timedelta(minutes=25), t0 - pd.Timedelta(minutes=30)],
        }
    )

    keys = ["key_column2"]
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            DataframeSource(data, key_field=keys, time_field="time"),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "some_data",
                        ["sum"],
                        SlidingWindows(["1h"], "10m"),
                    )
                ],
                table,
                emit_policy=EmitAfterMaxEvent(1),
            ),
            NoSqlTarget(table),
        ]
    ).run()

    actual = controller.await_termination()

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["number_of_stuff_sum_1h"], other_table, key=keys),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"key_column2": 8.6}, key=[8.6], event_time=test_base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{"number_of_stuff_sum_1h": 2.0, "key_column2": 8.6}]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("flush_interval", [None, 1, 300])
def test_aggregate_and_query_persist_before_advancing_window(
    setup_teardown_test, flush_interval
):
    table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        flush_interval_secs=flush_interval,
    )
    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "particles", "sample", ["count"], FixedWindows(["30m"])
                    )
                ],
                table,
                key="sample",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    for i in range(22, -1, -1):
        data = {"number": i, "sample": "U235"}
        controller.emit(data, "tal", test_base_time - timedelta(minutes=3 * i))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"particles_count_30m": 1.0, "number": 22, "sample": "U235"},
        {"particles_count_30m": 2.0, "number": 21, "sample": "U235"},
        {"particles_count_30m": 3.0, "number": 20, "sample": "U235"},
        {"particles_count_30m": 4.0, "number": 19, "sample": "U235"},
        {"particles_count_30m": 5.0, "number": 18, "sample": "U235"},
        {"particles_count_30m": 6.0, "number": 17, "sample": "U235"},
        {"particles_count_30m": 7.0, "number": 16, "sample": "U235"},
        {"particles_count_30m": 8.0, "number": 15, "sample": "U235"},
        {"particles_count_30m": 9.0, "number": 14, "sample": "U235"},
        {"particles_count_30m": 1.0, "number": 13, "sample": "U235"},
        {"particles_count_30m": 2.0, "number": 12, "sample": "U235"},
        {"particles_count_30m": 3.0, "number": 11, "sample": "U235"},
        {"particles_count_30m": 4.0, "number": 10, "sample": "U235"},
        {"particles_count_30m": 5.0, "number": 9, "sample": "U235"},
        {"particles_count_30m": 6.0, "number": 8, "sample": "U235"},
        {"particles_count_30m": 7.0, "number": 7, "sample": "U235"},
        {"particles_count_30m": 8.0, "number": 6, "sample": "U235"},
        {"particles_count_30m": 9.0, "number": 5, "sample": "U235"},
        {"particles_count_30m": 10.0, "number": 4, "sample": "U235"},
        {"particles_count_30m": 1.0, "number": 3, "sample": "U235"},
        {"particles_count_30m": 2.0, "number": 2, "sample": "U235"},
        {"particles_count_30m": 3.0, "number": 1, "sample": "U235"},
        {"particles_count_30m": 4.0, "number": 0, "sample": "U235"},
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                ["particles_count_30m"],
                table,
                key="sample",
                fixed_window_type=FixedWindowType.LastClosedWindow,
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()
    data = {"sample": "U235"}
    controller.emit(data, "tal", test_base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{"particles_count_30m": 10.0, "sample": "U235"}]
    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_aggregate_and_query_by_key_with_holes(setup_teardown_test):
    table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        partitioned_by_key=False,
        flush_interval_secs=None,
    )

    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "col1",
                        ["sum"],
                        SlidingWindows(["1h"], "10m"),
                    )
                ],
                table,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 5
    for i in range(items_in_ingest_batch):
        data = {"col1": i}
        controller.emit(data, "tal", test_base_time + timedelta(minutes=25 * i))

    controller.emit({"col1": 8}, "tal", test_base_time + timedelta(minutes=25 * 8))
    controller.emit({"col1": 9}, "tal", test_base_time + timedelta(minutes=25 * 9))

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {"number_of_stuff_sum_1h": 0, "col1": 0},
        {"number_of_stuff_sum_1h": 1, "col1": 1},
        {"number_of_stuff_sum_1h": 3, "col1": 2},
        {"number_of_stuff_sum_1h": 6, "col1": 3},
        {"number_of_stuff_sum_1h": 9, "col1": 4},
        {"number_of_stuff_sum_1h": 8, "col1": 8},
        {"number_of_stuff_sum_1h": 17, "col1": 9},
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["number_of_stuff_sum_1h"], other_table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = test_base_time + timedelta(minutes=25 * 10)
    data = {"col1": 10}
    controller.emit(data, "tal", base_time)

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [{"col1": 10, "number_of_stuff_sum_1h": 17}]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"
