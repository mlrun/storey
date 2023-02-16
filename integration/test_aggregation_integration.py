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
import datetime
import math
from datetime import timedelta

import pandas as pd
import pytest

from storey import (
    AggregateByKey,
    Context,
    DataframeSource,
    FieldAggregator,
    Map,
    MapWithState,
    NoSqlTarget,
    QueryByKey,
    Reduce,
    SyncEmitSource,
    Table,
    build_flow,
)
from storey.dtypes import (
    EmitAfterMaxEvent,
    FixedWindows,
    FixedWindowType,
    SlidingWindows,
)
from storey.flow import DropColumns
from storey.utils import _split_path

from .integration_test_utils import append_return


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
                time_field="time",
                key_field="col1",
            ),
            DropColumns("time"),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": items_in_ingest_batch, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

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
                key_field="col1",
                time_field="time",
                fixed_window_type=fixed_window_type,
            ),
            DropColumns("time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    for i in range(-items_in_ingest_batch, 2 * items_in_ingest_batch):
        data = {"col1": items_in_ingest_batch, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("partitioned_by_key", [True, False])
@pytest.mark.parametrize("flush_interval", [None, 1])
def test_aggregate_and_query_with_different_sliding_windows(setup_teardown_test, partitioned_by_key, flush_interval):
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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_24h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_max_1h": 0.0,
            "number_of_stuff_max_24h": 0.0,
            "number_of_stuff_max_2h": 0.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 0.0,
            "number_of_stuff_sqr_24h": 0.0,
            "number_of_stuff_sqr_2h": 0.0,
            "number_of_stuff_sum_1h": 0.0,
            "number_of_stuff_sum_24h": 0.0,
            "number_of_stuff_sum_2h": 0.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_avg_1h": 0.5,
            "number_of_stuff_avg_24h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_max_1h": 1.0,
            "number_of_stuff_max_24h": 1.0,
            "number_of_stuff_max_2h": 1.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 1.0,
            "number_of_stuff_sqr_24h": 1.0,
            "number_of_stuff_sqr_2h": 1.0,
            "number_of_stuff_sum_1h": 1.0,
            "number_of_stuff_sum_24h": 1.0,
            "number_of_stuff_sum_2h": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_24h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_max_1h": 2.0,
            "number_of_stuff_max_24h": 2.0,
            "number_of_stuff_max_2h": 2.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 5.0,
            "number_of_stuff_sqr_24h": 5.0,
            "number_of_stuff_sqr_2h": 5.0,
            "number_of_stuff_sum_1h": 3.0,
            "number_of_stuff_sum_24h": 3.0,
            "number_of_stuff_sum_2h": 3.0,
            "time": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 3,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_24h": 1.5,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_max_1h": 3.0,
            "number_of_stuff_max_24h": 3.0,
            "number_of_stuff_max_2h": 3.0,
            "number_of_stuff_min_1h": 1.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 14.0,
            "number_of_stuff_sqr_24h": 14.0,
            "number_of_stuff_sqr_2h": 14.0,
            "number_of_stuff_sum_1h": 6.0,
            "number_of_stuff_sum_24h": 6.0,
            "number_of_stuff_sum_2h": 6.0,
            "time": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 4,
            "number_of_stuff_avg_1h": 3.0,
            "number_of_stuff_avg_24h": 2.0,
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_max_1h": 4.0,
            "number_of_stuff_max_24h": 4.0,
            "number_of_stuff_max_2h": 4.0,
            "number_of_stuff_min_1h": 2.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 29.0,
            "number_of_stuff_sqr_24h": 30.0,
            "number_of_stuff_sqr_2h": 30.0,
            "number_of_stuff_sum_1h": 9.0,
            "number_of_stuff_sum_24h": 10.0,
            "number_of_stuff_sum_2h": 10.0,
            "time": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 5,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_24h": 2.5,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_max_1h": 5.0,
            "number_of_stuff_max_24h": 5.0,
            "number_of_stuff_max_2h": 5.0,
            "number_of_stuff_min_1h": 3.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 1.0,
            "number_of_stuff_sqr_1h": 50.0,
            "number_of_stuff_sqr_24h": 55.0,
            "number_of_stuff_sqr_2h": 55.0,
            "number_of_stuff_sum_1h": 12.0,
            "number_of_stuff_sum_24h": 15.0,
            "number_of_stuff_sum_2h": 15.0,
            "time": datetime.datetime(2020, 7, 21, 23, 45, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 6,
            "number_of_stuff_avg_1h": 5.0,
            "number_of_stuff_avg_24h": 3.0,
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_max_1h": 6.0,
            "number_of_stuff_max_24h": 6.0,
            "number_of_stuff_max_2h": 6.0,
            "number_of_stuff_min_1h": 4.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 2.0,
            "number_of_stuff_sqr_1h": 77.0,
            "number_of_stuff_sqr_24h": 91.0,
            "number_of_stuff_sqr_2h": 90.0,
            "number_of_stuff_sum_1h": 15.0,
            "number_of_stuff_sum_24h": 21.0,
            "number_of_stuff_sum_2h": 20.0,
            "time": datetime.datetime(2020, 7, 22, 0, 10, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 7,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_24h": 3.5,
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_max_1h": 7.0,
            "number_of_stuff_max_24h": 7.0,
            "number_of_stuff_max_2h": 7.0,
            "number_of_stuff_min_1h": 5.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 3.0,
            "number_of_stuff_sqr_1h": 110.0,
            "number_of_stuff_sqr_24h": 140.0,
            "number_of_stuff_sqr_2h": 135.0,
            "number_of_stuff_sum_1h": 18.0,
            "number_of_stuff_sum_24h": 28.0,
            "number_of_stuff_sum_2h": 25.0,
            "time": datetime.datetime(2020, 7, 22, 0, 35, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 8,
            "number_of_stuff_avg_1h": 7.0,
            "number_of_stuff_avg_24h": 4.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_max_1h": 8.0,
            "number_of_stuff_max_24h": 8.0,
            "number_of_stuff_max_2h": 8.0,
            "number_of_stuff_min_1h": 6.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 4.0,
            "number_of_stuff_sqr_1h": 149.0,
            "number_of_stuff_sqr_24h": 204.0,
            "number_of_stuff_sqr_2h": 190.0,
            "number_of_stuff_sum_1h": 21.0,
            "number_of_stuff_sum_24h": 36.0,
            "number_of_stuff_sum_2h": 30.0,
            "time": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 9,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_24h": 4.5,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_max_24h": 9.0,
            "number_of_stuff_max_2h": 9.0,
            "number_of_stuff_min_1h": 7.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 5.0,
            "number_of_stuff_sqr_1h": 194.0,
            "number_of_stuff_sqr_24h": 285.0,
            "number_of_stuff_sqr_2h": 255.0,
            "number_of_stuff_sum_1h": 24.0,
            "number_of_stuff_sum_24h": 45.0,
            "number_of_stuff_sum_2h": 35.0,
            "time": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
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
            "number_of_stuff_avg_1h": 8.5,
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_min_1h": 8.0,
            "number_of_stuff_sum_1h": 17.0,
            "time": datetime.datetime(2020, 7, 22, 1, 50, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 10,
            "number_of_stuff_avg_1h": 9.0,
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_min_1h": 9.0,
            "number_of_stuff_sum_1h": 9.0,
            "time": datetime.datetime(2020, 7, 22, 2, 15, tzinfo=datetime.timezone.utc),
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
                    time_field="time",
                ),
                Reduce([], lambda acc, x: append_return(acc, x)),
            ]
        ).run()

        base_time = setup_teardown_test.test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
        data = {"col1": items_in_ingest_batch, "time": base_time}
        controller.emit(data, "tal")
        data = {"col1": items_in_ingest_batch, "time": base_time + timedelta(minutes=25)}
        controller.emit(data, "tal")

        controller.terminate()
        actual = controller.await_termination()

        assert (
            actual == expected_results
        ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("partitioned_by_key", [True, False])
@pytest.mark.parametrize("flush_interval", [None, 1])
def test_aggregate_and_query_with_different_fixed_windows(setup_teardown_test, partitioned_by_key, flush_interval):
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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_24h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_max_1h": 0.0,
            "number_of_stuff_max_24h": 0.0,
            "number_of_stuff_max_2h": 0.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 0.0,
            "number_of_stuff_sqr_24h": 0.0,
            "number_of_stuff_sqr_2h": 0.0,
            "number_of_stuff_sum_1h": 0.0,
            "number_of_stuff_sum_24h": 0.0,
            "number_of_stuff_sum_2h": 0.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_24h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_max_1h": 1.0,
            "number_of_stuff_max_24h": 1.0,
            "number_of_stuff_max_2h": 1.0,
            "number_of_stuff_min_1h": 1.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 1.0,
            "number_of_stuff_sqr_24h": 1.0,
            "number_of_stuff_sqr_2h": 1.0,
            "number_of_stuff_sum_1h": 1.0,
            "number_of_stuff_sum_24h": 1.0,
            "number_of_stuff_sum_2h": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_avg_1h": 1.5,
            "number_of_stuff_avg_24h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_max_1h": 2.0,
            "number_of_stuff_max_24h": 2.0,
            "number_of_stuff_max_2h": 2.0,
            "number_of_stuff_min_1h": 1.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 5.0,
            "number_of_stuff_sqr_24h": 5.0,
            "number_of_stuff_sqr_2h": 5.0,
            "number_of_stuff_sum_1h": 3.0,
            "number_of_stuff_sum_24h": 3.0,
            "number_of_stuff_sum_2h": 3.0,
            "time": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 3,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_24h": 1.5,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_max_1h": 3.0,
            "number_of_stuff_max_24h": 3.0,
            "number_of_stuff_max_2h": 3.0,
            "number_of_stuff_min_1h": 1.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 14.0,
            "number_of_stuff_sqr_24h": 14.0,
            "number_of_stuff_sqr_2h": 14.0,
            "number_of_stuff_sum_1h": 6.0,
            "number_of_stuff_sum_24h": 6.0,
            "number_of_stuff_sum_2h": 6.0,
            "time": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 4,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_24h": 2.0,
            "number_of_stuff_avg_2h": 2.5,
            "number_of_stuff_max_1h": 4.0,
            "number_of_stuff_max_24h": 4.0,
            "number_of_stuff_max_2h": 4.0,
            "number_of_stuff_min_1h": 4.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 1.0,
            "number_of_stuff_sqr_1h": 16.0,
            "number_of_stuff_sqr_24h": 30.0,
            "number_of_stuff_sqr_2h": 30.0,
            "number_of_stuff_sum_1h": 4.0,
            "number_of_stuff_sum_24h": 10.0,
            "number_of_stuff_sum_2h": 10.0,
            "time": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 5,
            "number_of_stuff_avg_1h": 4.5,
            "number_of_stuff_avg_24h": 2.5,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_max_1h": 5.0,
            "number_of_stuff_max_24h": 5.0,
            "number_of_stuff_max_2h": 5.0,
            "number_of_stuff_min_1h": 4.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 1.0,
            "number_of_stuff_sqr_1h": 41.0,
            "number_of_stuff_sqr_24h": 55.0,
            "number_of_stuff_sqr_2h": 55.0,
            "number_of_stuff_sum_1h": 9.0,
            "number_of_stuff_sum_24h": 15.0,
            "number_of_stuff_sum_2h": 15.0,
            "time": datetime.datetime(2020, 7, 21, 23, 45, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 6,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_24h": 6.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_max_1h": 6.0,
            "number_of_stuff_max_24h": 6.0,
            "number_of_stuff_max_2h": 6.0,
            "number_of_stuff_min_1h": 6.0,
            "number_of_stuff_min_24h": 6.0,
            "number_of_stuff_min_2h": 6.0,
            "number_of_stuff_sqr_1h": 36.0,
            "number_of_stuff_sqr_24h": 36.0,
            "number_of_stuff_sqr_2h": 36.0,
            "number_of_stuff_sum_1h": 6.0,
            "number_of_stuff_sum_24h": 6.0,
            "number_of_stuff_sum_2h": 6.0,
            "time": datetime.datetime(2020, 7, 22, 0, 10, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 7,
            "number_of_stuff_avg_1h": 6.5,
            "number_of_stuff_avg_24h": 6.5,
            "number_of_stuff_avg_2h": 6.5,
            "number_of_stuff_max_1h": 7.0,
            "number_of_stuff_max_24h": 7.0,
            "number_of_stuff_max_2h": 7.0,
            "number_of_stuff_min_1h": 6.0,
            "number_of_stuff_min_24h": 6.0,
            "number_of_stuff_min_2h": 6.0,
            "number_of_stuff_sqr_1h": 85.0,
            "number_of_stuff_sqr_24h": 85.0,
            "number_of_stuff_sqr_2h": 85.0,
            "number_of_stuff_sum_1h": 13.0,
            "number_of_stuff_sum_24h": 13.0,
            "number_of_stuff_sum_2h": 13.0,
            "time": datetime.datetime(2020, 7, 22, 0, 35, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 8,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_24h": 7.0,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_max_1h": 8.0,
            "number_of_stuff_max_24h": 8.0,
            "number_of_stuff_max_2h": 8.0,
            "number_of_stuff_min_1h": 8.0,
            "number_of_stuff_min_24h": 6.0,
            "number_of_stuff_min_2h": 6.0,
            "number_of_stuff_sqr_1h": 64.0,
            "number_of_stuff_sqr_24h": 149.0,
            "number_of_stuff_sqr_2h": 149.0,
            "number_of_stuff_sum_1h": 8.0,
            "number_of_stuff_sum_24h": 21.0,
            "number_of_stuff_sum_2h": 21.0,
            "time": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 9,
            "number_of_stuff_avg_1h": 8.5,
            "number_of_stuff_avg_24h": 7.5,
            "number_of_stuff_avg_2h": 7.5,
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_max_24h": 9.0,
            "number_of_stuff_max_2h": 9.0,
            "number_of_stuff_min_1h": 8.0,
            "number_of_stuff_min_24h": 6.0,
            "number_of_stuff_min_2h": 6.0,
            "number_of_stuff_sqr_1h": 145.0,
            "number_of_stuff_sqr_24h": 230.0,
            "number_of_stuff_sqr_2h": 230.0,
            "number_of_stuff_sum_1h": 17.0,
            "number_of_stuff_sum_24h": 30.0,
            "number_of_stuff_sum_2h": 30.0,
            "time": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
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
            "number_of_stuff_avg_1h": 8.5,
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_min_1h": 8.0,
            "number_of_stuff_sum_1h": 17.0,
            "time": datetime.datetime(2020, 7, 22, 1, 50, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 10,
            "number_of_stuff_avg_1h": math.nan,
            "number_of_stuff_max_1h": -math.inf,
            "number_of_stuff_min_1h": math.inf,
            "number_of_stuff_sum_1h": 0.0,
            "time": datetime.datetime(2020, 7, 22, 2, 15, tzinfo=datetime.timezone.utc),
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
                    time_field="time",
                    context=context,
                ),
                Reduce([], lambda acc, x: append_return(acc, x)),
            ]
        ).run()

        base_time = setup_teardown_test.test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
        data = {"col1": items_in_ingest_batch, "time": base_time}
        controller.emit(data, "tal")
        data = {"col1": items_in_ingest_batch, "time": base_time + timedelta(minutes=25)}
        controller.emit(data, "tal")

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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "dina")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_avg_24h": 0.0,
            "number_of_stuff_stddev_24h": math.nan,
            "number_of_stuff_stdvar_24h": math.nan,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_avg_24h": 0.5,
            "number_of_stuff_stddev_24h": 0.7071067811865476,
            "number_of_stuff_stdvar_24h": 0.5,
            "time": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_avg_24h": 1.0,
            "number_of_stuff_stddev_24h": 1.0,
            "number_of_stuff_stdvar_24h": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 3,
            "number_of_stuff_avg_24h": 1.5,
            "number_of_stuff_stddev_24h": 1.2909944487358056,
            "number_of_stuff_stdvar_24h": 1.6666666666666667,
            "time": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 4,
            "number_of_stuff_avg_24h": 2.0,
            "number_of_stuff_stddev_24h": 1.5811388300841898,
            "number_of_stuff_stdvar_24h": 2.5,
            "time": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 5,
            "number_of_stuff_avg_24h": 2.5,
            "number_of_stuff_stddev_24h": 1.8708286933869707,
            "number_of_stuff_stdvar_24h": 3.5,
            "time": datetime.datetime(2020, 7, 21, 23, 45, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 6,
            "number_of_stuff_avg_24h": 3.0,
            "number_of_stuff_stddev_24h": 2.160246899469287,
            "number_of_stuff_stdvar_24h": 4.666666666666667,
            "time": datetime.datetime(2020, 7, 22, 0, 10, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 7,
            "number_of_stuff_avg_24h": 3.5,
            "number_of_stuff_stddev_24h": 2.449489742783178,
            "number_of_stuff_stdvar_24h": 6.0,
            "time": datetime.datetime(2020, 7, 22, 0, 35, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 8,
            "number_of_stuff_avg_24h": 4.0,
            "number_of_stuff_stddev_24h": 2.7386127875258306,
            "number_of_stuff_stdvar_24h": 7.5,
            "time": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 9,
            "number_of_stuff_avg_24h": 4.5,
            "number_of_stuff_stddev_24h": 3.0276503540974917,
            "number_of_stuff_stdvar_24h": 9.166666666666666,
            "time": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
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
                time_field="time",
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch, "time": base_time}
    controller.emit(data, "dina")
    data = {"col1": items_in_ingest_batch, "time": base_time + timedelta(minutes=25)}
    controller.emit(data, "dina")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 10,
            "number_of_stuff_avg_1h": 8.5,
            "number_of_stuff_stddev_3h": 1.8708286933869707,
            "number_of_stuff_stdvar_2h": 1.6666666666666667,
            "time": datetime.datetime(2020, 7, 22, 1, 50, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 10,
            "number_of_stuff_avg_1h": 9.0,
            "number_of_stuff_stddev_3h": 1.8708286933869707,
            "number_of_stuff_stdvar_2h": 1.0,
            "time": datetime.datetime(2020, 7, 22, 2, 15, tzinfo=datetime.timezone.utc),
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("partitioned_by_key", [True, False])
@pytest.mark.parametrize("flush_interval", [None, 1])
def test_query_aggregate_by_key(setup_teardown_test, partitioned_by_key, flush_interval):
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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_24h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_max_1h": 0.0,
            "number_of_stuff_max_24h": 0.0,
            "number_of_stuff_max_2h": 0.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 0.0,
            "number_of_stuff_sqr_24h": 0.0,
            "number_of_stuff_sqr_2h": 0.0,
            "number_of_stuff_sum_1h": 0.0,
            "number_of_stuff_sum_24h": 0.0,
            "number_of_stuff_sum_2h": 0.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_avg_1h": 0.5,
            "number_of_stuff_avg_24h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_max_1h": 1.0,
            "number_of_stuff_max_24h": 1.0,
            "number_of_stuff_max_2h": 1.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 1.0,
            "number_of_stuff_sqr_24h": 1.0,
            "number_of_stuff_sqr_2h": 1.0,
            "number_of_stuff_sum_1h": 1.0,
            "number_of_stuff_sum_24h": 1.0,
            "number_of_stuff_sum_2h": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_24h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_max_1h": 2.0,
            "number_of_stuff_max_24h": 2.0,
            "number_of_stuff_max_2h": 2.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 5.0,
            "number_of_stuff_sqr_24h": 5.0,
            "number_of_stuff_sqr_2h": 5.0,
            "number_of_stuff_sum_1h": 3.0,
            "number_of_stuff_sum_24h": 3.0,
            "number_of_stuff_sum_2h": 3.0,
            "time": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 3,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_24h": 1.5,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_max_1h": 3.0,
            "number_of_stuff_max_24h": 3.0,
            "number_of_stuff_max_2h": 3.0,
            "number_of_stuff_min_1h": 1.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 14.0,
            "number_of_stuff_sqr_24h": 14.0,
            "number_of_stuff_sqr_2h": 14.0,
            "number_of_stuff_sum_1h": 6.0,
            "number_of_stuff_sum_24h": 6.0,
            "number_of_stuff_sum_2h": 6.0,
            "time": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 4,
            "number_of_stuff_avg_1h": 3.0,
            "number_of_stuff_avg_24h": 2.0,
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_max_1h": 4.0,
            "number_of_stuff_max_24h": 4.0,
            "number_of_stuff_max_2h": 4.0,
            "number_of_stuff_min_1h": 2.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 29.0,
            "number_of_stuff_sqr_24h": 30.0,
            "number_of_stuff_sqr_2h": 30.0,
            "number_of_stuff_sum_1h": 9.0,
            "number_of_stuff_sum_24h": 10.0,
            "number_of_stuff_sum_2h": 10.0,
            "time": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 5,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_24h": 2.5,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_max_1h": 5.0,
            "number_of_stuff_max_24h": 5.0,
            "number_of_stuff_max_2h": 5.0,
            "number_of_stuff_min_1h": 3.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 1.0,
            "number_of_stuff_sqr_1h": 50.0,
            "number_of_stuff_sqr_24h": 55.0,
            "number_of_stuff_sqr_2h": 55.0,
            "number_of_stuff_sum_1h": 12.0,
            "number_of_stuff_sum_24h": 15.0,
            "number_of_stuff_sum_2h": 15.0,
            "time": datetime.datetime(2020, 7, 21, 23, 45, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 6,
            "number_of_stuff_avg_1h": 5.0,
            "number_of_stuff_avg_24h": 3.0,
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_max_1h": 6.0,
            "number_of_stuff_max_24h": 6.0,
            "number_of_stuff_max_2h": 6.0,
            "number_of_stuff_min_1h": 4.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 2.0,
            "number_of_stuff_sqr_1h": 77.0,
            "number_of_stuff_sqr_24h": 91.0,
            "number_of_stuff_sqr_2h": 90.0,
            "number_of_stuff_sum_1h": 15.0,
            "number_of_stuff_sum_24h": 21.0,
            "number_of_stuff_sum_2h": 20.0,
            "time": datetime.datetime(2020, 7, 22, 0, 10, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 7,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_24h": 3.5,
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_max_1h": 7.0,
            "number_of_stuff_max_24h": 7.0,
            "number_of_stuff_max_2h": 7.0,
            "number_of_stuff_min_1h": 5.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 3.0,
            "number_of_stuff_sqr_1h": 110.0,
            "number_of_stuff_sqr_24h": 140.0,
            "number_of_stuff_sqr_2h": 135.0,
            "number_of_stuff_sum_1h": 18.0,
            "number_of_stuff_sum_24h": 28.0,
            "number_of_stuff_sum_2h": 25.0,
            "time": datetime.datetime(2020, 7, 22, 0, 35, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 8,
            "number_of_stuff_avg_1h": 7.0,
            "number_of_stuff_avg_24h": 4.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_max_1h": 8.0,
            "number_of_stuff_max_24h": 8.0,
            "number_of_stuff_max_2h": 8.0,
            "number_of_stuff_min_1h": 6.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 4.0,
            "number_of_stuff_sqr_1h": 149.0,
            "number_of_stuff_sqr_24h": 204.0,
            "number_of_stuff_sqr_2h": 190.0,
            "number_of_stuff_sum_1h": 21.0,
            "number_of_stuff_sum_24h": 36.0,
            "number_of_stuff_sum_2h": 30.0,
            "time": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 9,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_24h": 4.5,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_max_24h": 9.0,
            "number_of_stuff_max_2h": 9.0,
            "number_of_stuff_min_1h": 7.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 5.0,
            "number_of_stuff_sqr_1h": 194.0,
            "number_of_stuff_sqr_24h": 285.0,
            "number_of_stuff_sqr_2h": 255.0,
            "number_of_stuff_sum_1h": 24.0,
            "number_of_stuff_sum_24h": 45.0,
            "number_of_stuff_sum_2h": 35.0,
            "time": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
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
                time_field="time",
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch, "time": base_time}
    controller.emit(data, "tal")

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
            "time": datetime.datetime(2020, 7, 22, 1, 50, tzinfo=datetime.timezone.utc),
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
def test_aggregate_and_query_with_dependent_aggrs_different_windows(setup_teardown_test, query_aggregations):
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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_sum_1h": 0.0,
            "number_of_stuff_sum_2h": 0.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_avg_1h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_sum_1h": 1.0,
            "number_of_stuff_sum_2h": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_sum_1h": 3.0,
            "number_of_stuff_sum_2h": 3.0,
            "time": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 3,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_sum_1h": 6.0,
            "number_of_stuff_sum_2h": 6.0,
            "time": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 4,
            "number_of_stuff_avg_1h": 3.0,
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_sum_1h": 9.0,
            "number_of_stuff_sum_2h": 10.0,
            "time": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 5,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_sum_1h": 12.0,
            "number_of_stuff_sum_2h": 15.0,
            "time": datetime.datetime(2020, 7, 21, 23, 45, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 6,
            "number_of_stuff_avg_1h": 5.0,
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_sum_1h": 15.0,
            "number_of_stuff_sum_2h": 20.0,
            "time": datetime.datetime(2020, 7, 22, 0, 10, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 7,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_sum_1h": 18.0,
            "number_of_stuff_sum_2h": 25.0,
            "time": datetime.datetime(2020, 7, 22, 0, 35, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 8,
            "number_of_stuff_avg_1h": 7.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_sum_1h": 21.0,
            "number_of_stuff_sum_2h": 30.0,
            "time": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 9,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_sum_1h": 24.0,
            "number_of_stuff_sum_2h": 35.0,
            "time": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(query_aggregations, other_table, time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch, "time": base_time}
    controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 10,
            "number_of_stuff_avg_2h": 7.5,
            "number_of_stuff_sum_1h": 17.0,
            "time": datetime.datetime(2020, 7, 22, 1, 50, tzinfo=datetime.timezone.utc),
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("partitioned_by_key", [True, False])
@pytest.mark.parametrize("flush_interval", [None, 1])
def test_aggregate_by_key_one_underlying_window(setup_teardown_test, partitioned_by_key, flush_interval):
    expected = {
        1: [
            {
                "col1": 0,
                "number_of_stuff_count_1h": 1.0,
                "other_stuff_sum_1h": 0.0,
                "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 1,
                "number_of_stuff_count_1h": 2.0,
                "other_stuff_sum_1h": 1.0,
                "time": datetime.datetime(2020, 7, 21, 21, 41, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 2,
                "number_of_stuff_count_1h": 3.0,
                "other_stuff_sum_1h": 3.0,
                "time": datetime.datetime(2020, 7, 21, 21, 42, tzinfo=datetime.timezone.utc),
            },
        ],
        2: [
            {
                "col1": 3,
                "number_of_stuff_count_1h": 4.0,
                "other_stuff_sum_1h": 6.0,
                "time": datetime.datetime(2020, 7, 21, 21, 43, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 4,
                "number_of_stuff_count_1h": 5.0,
                "other_stuff_sum_1h": 10.0,
                "time": datetime.datetime(2020, 7, 21, 21, 44, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 5,
                "number_of_stuff_count_1h": 6.0,
                "other_stuff_sum_1h": 15.0,
                "time": datetime.datetime(2020, 7, 21, 21, 45, tzinfo=datetime.timezone.utc),
            },
        ],
        3: [
            {
                "col1": 6,
                "number_of_stuff_count_1h": 7.0,
                "other_stuff_sum_1h": 21.0,
                "time": datetime.datetime(2020, 7, 21, 21, 46, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 7,
                "number_of_stuff_count_1h": 8.0,
                "other_stuff_sum_1h": 28.0,
                "time": datetime.datetime(2020, 7, 21, 21, 47, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 8,
                "number_of_stuff_count_1h": 9.0,
                "other_stuff_sum_1h": 36.0,
                "time": datetime.datetime(2020, 7, 21, 21, 48, tzinfo=datetime.timezone.utc),
            },
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
                    time_field="time",
                ),
                NoSqlTarget(table),
                Reduce([], lambda acc, x: append_return(acc, x)),
            ]
        ).run()

        for _ in range(items_in_ingest_batch):
            data = {
                "col1": current_index,
                "time": setup_teardown_test.test_base_time + timedelta(minutes=1 * current_index),
            }
            controller.emit(data, "tal")
            current_index = current_index + 1

        controller.terminate()
        actual = controller.await_termination()

        assert (
            actual == current_expected
        ), f"actual did not match expected. \n actual: {actual} \n expected: {current_expected}"


@pytest.mark.parametrize("partitioned_by_key", [True, False])
def test_aggregate_by_key_two_underlying_windows(setup_teardown_test, partitioned_by_key):
    expected = {
        1: [
            {
                "col1": 0,
                "number_of_stuff_count_24h": 1.0,
                "other_stuff_sum_24h": 0.0,
                "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 1,
                "number_of_stuff_count_24h": 2.0,
                "other_stuff_sum_24h": 1.0,
                "time": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 2,
                "number_of_stuff_count_24h": 3.0,
                "other_stuff_sum_24h": 3.0,
                "time": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
            },
        ],
        2: [
            {
                "col1": 3,
                "number_of_stuff_count_24h": 4.0,
                "other_stuff_sum_24h": 6.0,
                "time": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 4,
                "number_of_stuff_count_24h": 5.0,
                "other_stuff_sum_24h": 10.0,
                "time": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 5,
                "number_of_stuff_count_24h": 6.0,
                "other_stuff_sum_24h": 15.0,
                "time": datetime.datetime(2020, 7, 21, 23, 45, tzinfo=datetime.timezone.utc),
            },
        ],
        3: [
            {
                "col1": 6,
                "number_of_stuff_count_24h": 7.0,
                "other_stuff_sum_24h": 21.0,
                "time": datetime.datetime(2020, 7, 22, 0, 10, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 7,
                "number_of_stuff_count_24h": 8.0,
                "other_stuff_sum_24h": 28.0,
                "time": datetime.datetime(2020, 7, 22, 0, 35, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 8,
                "number_of_stuff_count_24h": 9.0,
                "other_stuff_sum_24h": 36.0,
                "time": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
            },
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
                    time_field="time",
                ),
                NoSqlTarget(table),
                Reduce([], lambda acc, x: append_return(acc, x)),
            ]
        ).run()

        for _ in range(items_in_ingest_batch):
            data = {
                "col1": current_index,
                "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * current_index),
            }
            controller.emit(data, "tal")
            current_index = current_index + 1

        controller.terminate()
        actual = controller.await_termination()

        assert (
            actual == current_expected
        ), f"actual did not match expected. \n actual: {actual} \n expected: {current_expected}"


def test_aggregate_by_key_with_extra_aliases(setup_teardown_test):
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    table["tal"] = {"color": "blue", "age": 41, "iss": True, "sometime": setup_teardown_test.test_base_time}

    def enrich(event, state):
        if "first_activity" not in state:
            state["first_activity"] = event["sometime"]

        event["time_since_activity"] = (event["sometime"] - state["first_activity"]).seconds
        state["last_event"] = event["sometime"]
        event["total_activities"] = state["total_activities"] = state.get("total_activities", 0) + 1
        event["color"] = state["color"]
        return event, state

    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(table, enrich, group_by_key=True),
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
                time_field="sometime",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "sometime": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "color": "blue",
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_sum_2h": 0.0,
            "sometime": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
            "time_since_activity": 0,
            "total_activities": 1,
        },
        {
            "col1": 1,
            "color": "blue",
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_sum_2h": 1.0,
            "sometime": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
            "time_since_activity": 1500,
            "total_activities": 2,
        },
        {
            "col1": 2,
            "color": "blue",
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_sum_2h": 3.0,
            "sometime": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
            "time_since_activity": 3000,
            "total_activities": 3,
        },
        {
            "col1": 3,
            "color": "blue",
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_sum_2h": 6.0,
            "sometime": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
            "time_since_activity": 4500,
            "total_activities": 4,
        },
        {
            "col1": 4,
            "color": "blue",
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_sum_2h": 10.0,
            "sometime": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
            "time_since_activity": 6000,
            "total_activities": 5,
        },
        {
            "col1": 5,
            "color": "blue",
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_sum_2h": 15.0,
            "sometime": datetime.datetime(2020, 7, 21, 23, 45, tzinfo=datetime.timezone.utc),
            "time_since_activity": 7500,
            "total_activities": 6,
        },
        {
            "col1": 6,
            "color": "blue",
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_sum_2h": 20.0,
            "sometime": datetime.datetime(2020, 7, 22, 0, 10, tzinfo=datetime.timezone.utc),
            "time_since_activity": 9000,
            "total_activities": 7,
        },
        {
            "col1": 7,
            "color": "blue",
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_sum_2h": 25.0,
            "sometime": datetime.datetime(2020, 7, 22, 0, 35, tzinfo=datetime.timezone.utc),
            "time_since_activity": 10500,
            "total_activities": 8,
        },
        {
            "col1": 8,
            "color": "blue",
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_sum_2h": 30.0,
            "sometime": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
            "time_since_activity": 12000,
            "total_activities": 9,
        },
        {
            "col1": 9,
            "color": "blue",
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_sum_2h": 35.0,
            "sometime": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
            "time_since_activity": 13500,
            "total_activities": 10,
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
                time_field="sometime",
                aliases={
                    "color": "external.color",
                    "iss": "external.iss",
                    "number_of_stuff_avg_2h": "my_avg",
                },
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch, "sometime": base_time}
    controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "age": 41,
            "col1": 10,
            "external.color": "blue",
            "external.iss": True,
            "my_avg": 7.5,
            "number_of_stuff_sum_2h": 30.0,
            "sometime": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
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

    table["tal"] = {"color": "blue", "age": 41, "iss": True, "sometime": setup_teardown_test.test_base_time}

    def enrich(event, state):
        if "first_activity" not in state:
            state["first_activity"] = event["sometime"]

        event["time_since_activity"] = (event["sometime"] - state["first_activity"]).seconds
        state["last_event"] = event["sometime"]
        event["total_activities"] = state["total_activities"] = state.get("total_activities", 0) + 1
        event["color"] = state["color"]
        return event, state

    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(table, enrich, group_by_key=True),
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
                time_field="sometime",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "sometime": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "color": "blue",
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_sum_2h": 0.0,
            "sometime": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
            "time_since_activity": 0,
            "total_activities": 1,
        },
        {
            "col1": 1,
            "color": "blue",
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_sum_2h": 1.0,
            "sometime": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
            "time_since_activity": 1500,
            "total_activities": 2,
        },
        {
            "col1": 2,
            "color": "blue",
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_sum_2h": 3.0,
            "sometime": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
            "time_since_activity": 3000,
            "total_activities": 3,
        },
        {
            "col1": 3,
            "color": "blue",
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_sum_2h": 6.0,
            "sometime": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
            "time_since_activity": 4500,
            "total_activities": 4,
        },
        {
            "col1": 4,
            "color": "blue",
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_sum_2h": 10.0,
            "sometime": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
            "time_since_activity": 6000,
            "total_activities": 5,
        },
        {
            "col1": 5,
            "color": "blue",
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_sum_2h": 15.0,
            "sometime": datetime.datetime(2020, 7, 21, 23, 45, tzinfo=datetime.timezone.utc),
            "time_since_activity": 7500,
            "total_activities": 6,
        },
        {
            "col1": 6,
            "color": "blue",
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_sum_2h": 20.0,
            "sometime": datetime.datetime(2020, 7, 22, 0, 10, tzinfo=datetime.timezone.utc),
            "time_since_activity": 9000,
            "total_activities": 7,
        },
        {
            "col1": 7,
            "color": "blue",
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_sum_2h": 25.0,
            "sometime": datetime.datetime(2020, 7, 22, 0, 35, tzinfo=datetime.timezone.utc),
            "time_since_activity": 10500,
            "total_activities": 8,
        },
        {
            "col1": 8,
            "color": "blue",
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_sum_2h": 30.0,
            "sometime": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
            "time_since_activity": 12000,
            "total_activities": 9,
        },
        {
            "col1": 9,
            "color": "blue",
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_sum_2h": 35.0,
            "sometime": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
            "time_since_activity": 13500,
            "total_activities": 10,
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
                time_field="sometime",
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch, "sometime": base_time}
    controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "age": 41,
            "col1": 10,
            "color": "blue",
            "iss": True,
            "number_of_stuff_avg_2h": 7.5,
            "number_of_stuff_sum_2h": 30.0,
            "sometime": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
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

    table["tal"] = {"color": "blue", "age": 41, "iss": True, "sometime": setup_teardown_test.test_base_time}

    def enrich(event, state):
        if "first_activity" not in state:
            state["first_activity"] = event["sometime"]

        event["time_since_activity"] = (event["sometime"] - state["first_activity"]).seconds
        state["last_event"] = event["sometime"]
        event["total_activities"] = state["total_activities"] = state.get("total_activities", 0) + 1
        event["color"] = state["color"]
        return event, state

    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(table, enrich, group_by_key=True),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "sometime": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "color": "blue",
            "sometime": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
            "time_since_activity": 0,
            "total_activities": 1,
        },
        {
            "col1": 1,
            "color": "blue",
            "sometime": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
            "time_since_activity": 1500,
            "total_activities": 2,
        },
        {
            "col1": 2,
            "color": "blue",
            "sometime": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
            "time_since_activity": 3000,
            "total_activities": 3,
        },
        {
            "col1": 3,
            "color": "blue",
            "sometime": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
            "time_since_activity": 4500,
            "total_activities": 4,
        },
        {
            "col1": 4,
            "color": "blue",
            "sometime": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
            "time_since_activity": 6000,
            "total_activities": 5,
        },
        {
            "col1": 5,
            "color": "blue",
            "sometime": datetime.datetime(2020, 7, 21, 23, 45, tzinfo=datetime.timezone.utc),
            "time_since_activity": 7500,
            "total_activities": 6,
        },
        {
            "col1": 6,
            "color": "blue",
            "sometime": datetime.datetime(2020, 7, 22, 0, 10, tzinfo=datetime.timezone.utc),
            "time_since_activity": 9000,
            "total_activities": 7,
        },
        {
            "col1": 7,
            "color": "blue",
            "sometime": datetime.datetime(2020, 7, 22, 0, 35, tzinfo=datetime.timezone.utc),
            "time_since_activity": 10500,
            "total_activities": 8,
        },
        {
            "col1": 8,
            "color": "blue",
            "sometime": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
            "time_since_activity": 12000,
            "total_activities": 9,
        },
        {
            "col1": 9,
            "color": "blue",
            "sometime": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
            "time_since_activity": 13500,
            "total_activities": 10,
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())

    controller = build_flow(
        [
            SyncEmitSource(),
            MapWithState(other_table, enrich, group_by_key=True),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch, "sometime": base_time}
    controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 10,
            "color": "blue",
            "sometime": datetime.datetime(2020, 7, 22, 1, 50, tzinfo=datetime.timezone.utc),
            "time_since_activity": 15000,
            "total_activities": 11,
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
                time_field="time",
                context=context,
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_24h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_max_1h": 0.0,
            "number_of_stuff_max_24h": 0.0,
            "number_of_stuff_max_2h": 0.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 0.0,
            "number_of_stuff_sqr_24h": 0.0,
            "number_of_stuff_sqr_2h": 0.0,
            "number_of_stuff_sum_1h": 0.0,
            "number_of_stuff_sum_24h": 0.0,
            "number_of_stuff_sum_2h": 0.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_avg_1h": 0.5,
            "number_of_stuff_avg_24h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_max_1h": 1.0,
            "number_of_stuff_max_24h": 1.0,
            "number_of_stuff_max_2h": 1.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 1.0,
            "number_of_stuff_sqr_24h": 1.0,
            "number_of_stuff_sqr_2h": 1.0,
            "number_of_stuff_sum_1h": 1.0,
            "number_of_stuff_sum_24h": 1.0,
            "number_of_stuff_sum_2h": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_24h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_max_1h": 2.0,
            "number_of_stuff_max_24h": 2.0,
            "number_of_stuff_max_2h": 2.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 5.0,
            "number_of_stuff_sqr_24h": 5.0,
            "number_of_stuff_sqr_2h": 5.0,
            "number_of_stuff_sum_1h": 3.0,
            "number_of_stuff_sum_24h": 3.0,
            "number_of_stuff_sum_2h": 3.0,
            "time": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 3,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_24h": 1.5,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_max_1h": 3.0,
            "number_of_stuff_max_24h": 3.0,
            "number_of_stuff_max_2h": 3.0,
            "number_of_stuff_min_1h": 1.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 14.0,
            "number_of_stuff_sqr_24h": 14.0,
            "number_of_stuff_sqr_2h": 14.0,
            "number_of_stuff_sum_1h": 6.0,
            "number_of_stuff_sum_24h": 6.0,
            "number_of_stuff_sum_2h": 6.0,
            "time": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 4,
            "number_of_stuff_avg_1h": 3.0,
            "number_of_stuff_avg_24h": 2.0,
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_max_1h": 4.0,
            "number_of_stuff_max_24h": 4.0,
            "number_of_stuff_max_2h": 4.0,
            "number_of_stuff_min_1h": 2.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sqr_1h": 29.0,
            "number_of_stuff_sqr_24h": 30.0,
            "number_of_stuff_sqr_2h": 30.0,
            "number_of_stuff_sum_1h": 9.0,
            "number_of_stuff_sum_24h": 10.0,
            "number_of_stuff_sum_2h": 10.0,
            "time": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 5,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_24h": 2.5,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_max_1h": 5.0,
            "number_of_stuff_max_24h": 5.0,
            "number_of_stuff_max_2h": 5.0,
            "number_of_stuff_min_1h": 3.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 1.0,
            "number_of_stuff_sqr_1h": 50.0,
            "number_of_stuff_sqr_24h": 55.0,
            "number_of_stuff_sqr_2h": 55.0,
            "number_of_stuff_sum_1h": 12.0,
            "number_of_stuff_sum_24h": 15.0,
            "number_of_stuff_sum_2h": 15.0,
            "time": datetime.datetime(2020, 7, 21, 23, 45, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 6,
            "number_of_stuff_avg_1h": 5.0,
            "number_of_stuff_avg_24h": 3.0,
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_max_1h": 6.0,
            "number_of_stuff_max_24h": 6.0,
            "number_of_stuff_max_2h": 6.0,
            "number_of_stuff_min_1h": 4.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 2.0,
            "number_of_stuff_sqr_1h": 77.0,
            "number_of_stuff_sqr_24h": 91.0,
            "number_of_stuff_sqr_2h": 90.0,
            "number_of_stuff_sum_1h": 15.0,
            "number_of_stuff_sum_24h": 21.0,
            "number_of_stuff_sum_2h": 20.0,
            "time": datetime.datetime(2020, 7, 22, 0, 10, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 7,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_24h": 3.5,
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_max_1h": 7.0,
            "number_of_stuff_max_24h": 7.0,
            "number_of_stuff_max_2h": 7.0,
            "number_of_stuff_min_1h": 5.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 3.0,
            "number_of_stuff_sqr_1h": 110.0,
            "number_of_stuff_sqr_24h": 140.0,
            "number_of_stuff_sqr_2h": 135.0,
            "number_of_stuff_sum_1h": 18.0,
            "number_of_stuff_sum_24h": 28.0,
            "number_of_stuff_sum_2h": 25.0,
            "time": datetime.datetime(2020, 7, 22, 0, 35, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 8,
            "number_of_stuff_avg_1h": 7.0,
            "number_of_stuff_avg_24h": 4.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_max_1h": 8.0,
            "number_of_stuff_max_24h": 8.0,
            "number_of_stuff_max_2h": 8.0,
            "number_of_stuff_min_1h": 6.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 4.0,
            "number_of_stuff_sqr_1h": 149.0,
            "number_of_stuff_sqr_24h": 204.0,
            "number_of_stuff_sqr_2h": 190.0,
            "number_of_stuff_sum_1h": 21.0,
            "number_of_stuff_sum_24h": 36.0,
            "number_of_stuff_sum_2h": 30.0,
            "time": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 9,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_24h": 4.5,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_max_24h": 9.0,
            "number_of_stuff_max_2h": 9.0,
            "number_of_stuff_min_1h": 7.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 5.0,
            "number_of_stuff_sqr_1h": 194.0,
            "number_of_stuff_sqr_24h": 285.0,
            "number_of_stuff_sqr_2h": 255.0,
            "number_of_stuff_sum_1h": 24.0,
            "number_of_stuff_sum_24h": 45.0,
            "number_of_stuff_sum_2h": 35.0,
            "time": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
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
                time_field="time",
            ),
            DropColumns("time"),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

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
                time_field="time",
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch, "time": base_time}
    controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 10,
            "new_aggr_max_3h": 10.0,
            "new_aggr_min_3h": 10.0,
            "number_of_stuff_avg_1h": 9.0,
            "number_of_stuff_avg_24h": 5.0,
            "number_of_stuff_avg_2h": 8.0,
            "number_of_stuff_max_1h": 10.0,
            "number_of_stuff_max_24h": 10.0,
            "number_of_stuff_max_2h": 10.0,
            "number_of_stuff_min_1h": 8.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 6.0,
            "number_of_stuff_sum_1h": 27.0,
            "number_of_stuff_sum_24h": 55.0,
            "number_of_stuff_sum_2h": 40.0,
            "time": datetime.datetime(2020, 7, 22, 1, 50, tzinfo=datetime.timezone.utc),
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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_avg_1h": 0.0,
            "number_of_stuff_avg_24h": 0.0,
            "number_of_stuff_avg_2h": 0.0,
            "number_of_stuff_max_1h": 0.0,
            "number_of_stuff_max_24h": 0.0,
            "number_of_stuff_max_2h": 0.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sum_1h": 0.0,
            "number_of_stuff_sum_24h": 0.0,
            "number_of_stuff_sum_2h": 0.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_avg_1h": 0.5,
            "number_of_stuff_avg_24h": 0.5,
            "number_of_stuff_avg_2h": 0.5,
            "number_of_stuff_max_1h": 1.0,
            "number_of_stuff_max_24h": 1.0,
            "number_of_stuff_max_2h": 1.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sum_1h": 1.0,
            "number_of_stuff_sum_24h": 1.0,
            "number_of_stuff_sum_2h": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_avg_1h": 1.0,
            "number_of_stuff_avg_24h": 1.0,
            "number_of_stuff_avg_2h": 1.0,
            "number_of_stuff_max_1h": 2.0,
            "number_of_stuff_max_24h": 2.0,
            "number_of_stuff_max_2h": 2.0,
            "number_of_stuff_min_1h": 0.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sum_1h": 3.0,
            "number_of_stuff_sum_24h": 3.0,
            "number_of_stuff_sum_2h": 3.0,
            "time": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 3,
            "number_of_stuff_avg_1h": 2.0,
            "number_of_stuff_avg_24h": 1.5,
            "number_of_stuff_avg_2h": 1.5,
            "number_of_stuff_max_1h": 3.0,
            "number_of_stuff_max_24h": 3.0,
            "number_of_stuff_max_2h": 3.0,
            "number_of_stuff_min_1h": 1.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sum_1h": 6.0,
            "number_of_stuff_sum_24h": 6.0,
            "number_of_stuff_sum_2h": 6.0,
            "time": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 4,
            "number_of_stuff_avg_1h": 3.0,
            "number_of_stuff_avg_24h": 2.0,
            "number_of_stuff_avg_2h": 2.0,
            "number_of_stuff_max_1h": 4.0,
            "number_of_stuff_max_24h": 4.0,
            "number_of_stuff_max_2h": 4.0,
            "number_of_stuff_min_1h": 2.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 0.0,
            "number_of_stuff_sum_1h": 9.0,
            "number_of_stuff_sum_24h": 10.0,
            "number_of_stuff_sum_2h": 10.0,
            "time": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 5,
            "number_of_stuff_avg_1h": 4.0,
            "number_of_stuff_avg_24h": 2.5,
            "number_of_stuff_avg_2h": 3.0,
            "number_of_stuff_max_1h": 5.0,
            "number_of_stuff_max_24h": 5.0,
            "number_of_stuff_max_2h": 5.0,
            "number_of_stuff_min_1h": 3.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 1.0,
            "number_of_stuff_sum_1h": 12.0,
            "number_of_stuff_sum_24h": 15.0,
            "number_of_stuff_sum_2h": 15.0,
            "time": datetime.datetime(2020, 7, 21, 23, 45, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 6,
            "number_of_stuff_avg_1h": 5.0,
            "number_of_stuff_avg_24h": 3.0,
            "number_of_stuff_avg_2h": 4.0,
            "number_of_stuff_max_1h": 6.0,
            "number_of_stuff_max_24h": 6.0,
            "number_of_stuff_max_2h": 6.0,
            "number_of_stuff_min_1h": 4.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 2.0,
            "number_of_stuff_sum_1h": 15.0,
            "number_of_stuff_sum_24h": 21.0,
            "number_of_stuff_sum_2h": 20.0,
            "time": datetime.datetime(2020, 7, 22, 0, 10, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 7,
            "number_of_stuff_avg_1h": 6.0,
            "number_of_stuff_avg_24h": 3.5,
            "number_of_stuff_avg_2h": 5.0,
            "number_of_stuff_max_1h": 7.0,
            "number_of_stuff_max_24h": 7.0,
            "number_of_stuff_max_2h": 7.0,
            "number_of_stuff_min_1h": 5.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 3.0,
            "number_of_stuff_sum_1h": 18.0,
            "number_of_stuff_sum_24h": 28.0,
            "number_of_stuff_sum_2h": 25.0,
            "time": datetime.datetime(2020, 7, 22, 0, 35, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 8,
            "number_of_stuff_avg_1h": 7.0,
            "number_of_stuff_avg_24h": 4.0,
            "number_of_stuff_avg_2h": 6.0,
            "number_of_stuff_max_1h": 8.0,
            "number_of_stuff_max_24h": 8.0,
            "number_of_stuff_max_2h": 8.0,
            "number_of_stuff_min_1h": 6.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 4.0,
            "number_of_stuff_sum_1h": 21.0,
            "number_of_stuff_sum_24h": 36.0,
            "number_of_stuff_sum_2h": 30.0,
            "time": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 9,
            "number_of_stuff_avg_1h": 8.0,
            "number_of_stuff_avg_24h": 4.5,
            "number_of_stuff_avg_2h": 7.0,
            "number_of_stuff_max_1h": 9.0,
            "number_of_stuff_max_24h": 9.0,
            "number_of_stuff_max_2h": 9.0,
            "number_of_stuff_min_1h": 7.0,
            "number_of_stuff_min_24h": 0.0,
            "number_of_stuff_min_2h": 5.0,
            "number_of_stuff_sum_1h": 24.0,
            "number_of_stuff_sum_24h": 45.0,
            "number_of_stuff_sum_2h": 35.0,
            "time": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
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
                    time_field="time",
                ),
                Reduce([], lambda acc, x: append_return(acc, x)),
            ]
        ).run()

        base_time = setup_teardown_test.test_base_time + timedelta(minutes=25 * items_in_ingest_batch)
        data = {"col1": items_in_ingest_batch, "time": base_time}
        controller.emit(data, "tal")

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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(hours=i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_count_2h": 1.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_count_2h": 2.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_count_2h": 2.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 23, 40, tzinfo=datetime.timezone.utc),
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["number_of_stuff_count_30m", "number_of_stuff_count_2h"], other_table, time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(hours=items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch, "time": base_time}
    controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 3,
            "number_of_stuff_count_2h": 1.0,
            "number_of_stuff_count_30m": 0.0,
            "time": datetime.datetime(2020, 7, 22, 0, 40, tzinfo=datetime.timezone.utc),
        }
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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=45 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_count_2h": 1.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_count_2h": 1.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 25, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_count_2h": 2.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 23, 10, tzinfo=datetime.timezone.utc),
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["number_of_stuff_count_30m", "number_of_stuff_count_2h"], other_table, time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(hours=items_in_ingest_batch)
    data = {"col1": items_in_ingest_batch, "time": base_time}
    controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 3,
            "number_of_stuff_count_2h": 0.0,
            "number_of_stuff_count_30m": 0.0,
            "time": datetime.datetime(2020, 7, 22, 0, 40, tzinfo=datetime.timezone.utc),
        }
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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(hours=i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_count_2h": 1.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_count_2h": 2.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_count_2h": 2.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 23, 40, tzinfo=datetime.timezone.utc),
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["number_of_stuff_count_30m", "number_of_stuff_count_2h"], other_table, time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(days=10)
    data = {"col1": items_in_ingest_batch, "time": base_time}
    controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 3,
            "number_of_stuff_count_2h": 0.0,
            "number_of_stuff_count_30m": 0.0,
            "time": datetime.datetime(2020, 7, 31, 21, 40, tzinfo=datetime.timezone.utc),
        }
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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 3
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=45 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_count_2h": 1.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_count_2h": 1.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 25, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_count_2h": 2.0,
            "number_of_stuff_count_30m": 1.0,
            "time": datetime.datetime(2020, 7, 21, 23, 10, tzinfo=datetime.timezone.utc),
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
                ["number_of_stuff_count_30m", "number_of_stuff_count_2h"],
                other_table,
                time_field="time",
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(days=10)
    data = {"col1": items_in_ingest_batch, "time": base_time}
    controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 3,
            "number_of_stuff_count_2h": 0.0,
            "number_of_stuff_count_30m": 0.0,
            "time": datetime.datetime(2020, 7, 31, 21, 40, tzinfo=datetime.timezone.utc),
        }
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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    )
    items_in_ingest_batch = 3

    expected_results = [
        [
            {
                "col1": 0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_30m": 1.0,
                "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 1,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_30m": 1.0,
                "time": datetime.datetime(2020, 7, 21, 22, 25, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 2,
                "number_of_stuff_count_2h": 2.0,
                "number_of_stuff_count_30m": 1.0,
                "time": datetime.datetime(2020, 7, 21, 23, 10, tzinfo=datetime.timezone.utc),
            },
        ],
        [
            {
                "col1": 0,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_30m": 1.0,
                "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 1,
                "number_of_stuff_count_2h": 1.0,
                "number_of_stuff_count_30m": 1.0,
                "time": datetime.datetime(2020, 7, 21, 22, 25, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 2,
                "number_of_stuff_count_2h": 2.0,
                "number_of_stuff_count_30m": 1.0,
                "time": datetime.datetime(2020, 7, 21, 23, 10, tzinfo=datetime.timezone.utc),
            },
            {"col1": 0, "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc)},
            {
                "col1": 1,
                "number_of_stuff_count_2h": 2.0,
                "number_of_stuff_count_30m": 2.0,
                "time": datetime.datetime(2020, 7, 21, 22, 25, tzinfo=datetime.timezone.utc),
            },
            {
                "col1": 2,
                "number_of_stuff_count_2h": 3.0,
                "number_of_stuff_count_30m": 2.0,
                "time": datetime.datetime(2020, 7, 21, 23, 10, tzinfo=datetime.timezone.utc),
            },
        ],
    ]

    for iteration in range(2):
        controller = flow.run()
        for i in range(items_in_ingest_batch):
            data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=45 * i)}
            controller.emit(data, "tal")

        controller.terminate()
        actual = controller.await_termination()
        assert actual == expected_results[iteration]


def test_aggregate_multiple_keys(setup_teardown_test):
    t0 = pd.Timestamp(setup_teardown_test.test_base_time)
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
                time_field="time",
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
                ["number_of_stuff_sum_1h"], other_table, key_field=["first_name", "last_name"], time_field="time"
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit(
        {"first_name": "moshe", "last_name": "cohen", "some_data": 4, "time": setup_teardown_test.test_base_time},
        ["moshe", "cohen"],
    )
    controller.emit(
        {"first_name": "moshe", "last_name": "levi", "some_data": 5, "time": setup_teardown_test.test_base_time},
        ["moshe", "levi"],
    )
    controller.emit(
        {"first_name": "yosi", "last_name": "levi", "some_data": 6, "time": setup_teardown_test.test_base_time},
        ["yosi", "levi"],
    )

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "first_name": "moshe",
            "last_name": "cohen",
            "number_of_stuff_sum_1h": 1.0,
            "some_data": 4,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "first_name": "moshe",
            "last_name": "levi",
            "some_data": 5,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "first_name": "yosi",
            "last_name": "levi",
            "number_of_stuff_sum_1h": 5.0,
            "some_data": 6,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_aggregate_multiple_keys_and_aggregationless_query(setup_teardown_test):
    t0 = pd.Timestamp(setup_teardown_test.test_base_time)
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
                time_field="time",
                emit_policy=EmitAfterMaxEvent(1),
            ),
            NoSqlTarget(table),
        ]
    ).run()

    controller.await_termination()

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver(is_aggregationless_driver=True))

    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(
                ["number_of_stuff_sum_1h"], other_table, key_field=["first_name", "last_name"], time_field="time"
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit(
        {"first_name": "moshe", "last_name": "cohen", "some_data": 4, "time": setup_teardown_test.test_base_time},
        ["moshe", "cohen"],
    )
    controller.emit(
        {"first_name": "moshe", "last_name": "levi", "some_data": 5, "time": setup_teardown_test.test_base_time},
        ["moshe", "levi"],
    )
    controller.emit(
        {"first_name": "yosi", "last_name": "levi", "some_data": 6, "time": setup_teardown_test.test_base_time},
        ["yosi", "levi"],
    )

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number_of_stuff_sum_1h": 1.0,
            "first_name": "moshe",
            "last_name": "cohen",
            "some_data": 4,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "first_name": "moshe",
            "last_name": "levi",
            "some_data": 5,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        # number_of_stuff_sum_1h is 2 because the events were inserted out of order, and reading back
        # aggregationless takes the value relative to the event time of the last event that was inserted.
        {
            "number_of_stuff_sum_1h": 2.0,
            "first_name": "yosi",
            "last_name": "levi",
            "some_data": 6,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
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
                setup_teardown_test.test_base_time - pd.Timedelta(minutes=25),
                setup_teardown_test.test_base_time - pd.Timedelta(minutes=30),
                setup_teardown_test.test_base_time - pd.Timedelta(minutes=35),
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
                time_field="time",
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
                ["number_of_stuff_sum_1h"],
                other_table,
                key_field="first_name",
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"last_name": "levi", "some_data": 5}, "non_existing_key")

    controller.terminate()
    actual = controller.await_termination()

    assert actual == [None]


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
                [FieldAggregator("attr1", "attr1", ["sum"], SlidingWindows(["1h"], "10m"))],
                table1,
                time_field="time",
            ),
            NoSqlTarget(table1),
        ]
    ).run()
    controller2 = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [FieldAggregator("attr2", "attr2", ["sum"], SlidingWindows(["1h"], "10m"))],
                table2,
                time_field="time",
            ),
            NoSqlTarget(table2),
        ]
    ).run()

    try:
        for i in range(10):
            controller1.emit({"attr1": i, "time": setup_teardown_test.test_base_time}, key="onekey")
            controller2.emit({"attr2": i, "time": setup_teardown_test.test_base_time}, key="onekey")
    finally:
        controller1.terminate()
        controller2.terminate()
        controller1.await_termination()
        controller2.await_termination()

    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["attr1", "attr2"], table, key_field="mykey", time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"mykey": "onekey", "time": setup_teardown_test.test_base_time})

    controller.terminate()
    result = controller.await_termination()

    assert result == [
        {
            "attr1": 9,
            "attr2": 9,
            "mykey": "onekey",
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        }
    ]


def test_separate_aggregate_steps(setup_teardown_test):
    def map_multiply(x):
        x["some_data"] = x["some_data"] * 10
        return x

    t0 = pd.Timestamp(setup_teardown_test.test_base_time)
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
                time_field="time",
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
                time_field="time",
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
                key_field=["first_name"],
                time_field="time",
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"first_name": "moshe", "time": setup_teardown_test.test_base_time}, ["moshe"])

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "first_name": "moshe",
            "number_of_stuff2_sum_2h": 11.0,
            "number_of_stuff_avg_1h": 5.5,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
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
                [FieldAggregator("attr", "attr", ["first", "last"], SlidingWindows(["1h"], "10m"))],
                table,
                time_field="time",
            ),
            NoSqlTarget(table),
        ]
    ).run()

    try:
        for i in range(1, 10):
            controller.emit(
                {"attr": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=i)},
                key="onekey",
            )
            controller.emit(
                {"attr": i * 10, "time": setup_teardown_test.test_base_time + timedelta(hours=1, minutes=i)},
                key="onekey",
            )
    finally:
        controller.terminate()
        controller.await_termination()

    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["attr_first_1h", "attr_last_1h"], table, key_field="mykey", time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"mykey": "onekey", "time": setup_teardown_test.test_base_time + timedelta(minutes=10)})
    controller.emit({"mykey": "onekey", "time": setup_teardown_test.test_base_time + timedelta(hours=1, minutes=10)})

    controller.terminate()
    result = controller.await_termination()

    assert result == [
        {
            "attr_first_1h": 1.0,
            "attr_last_1h": 9.0,
            "mykey": "onekey",
            "time": datetime.datetime(2020, 7, 21, 21, 50, tzinfo=datetime.timezone.utc),
        },
        {
            "attr_first_1h": 10.0,
            "attr_last_1h": 90.0,
            "mykey": "onekey",
            "time": datetime.datetime(2020, 7, 21, 22, 50, tzinfo=datetime.timezone.utc),
        },
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
            QueryByKey(["color"], table, key_field=["name"]),
            QueryByKey(["city"], table, key_field="name"),
        ]
    ).run()

    controller.emit({"nameeeee": "katya"}, "katya")
    controller.terminate()
    controller.await_termination()


def test_multiple_keys_int(setup_teardown_test):
    t0 = pd.Timestamp(setup_teardown_test.test_base_time)
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
                time_field="time",
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
            QueryByKey(["number_of_stuff_sum_1h"], other_table, key_field=keys, time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit(
        {
            "key_column1": 10,
            "key_column2": 30,
            "key_column3": 5,
            "key_column4": 50,
            "time": setup_teardown_test.test_base_time,
        },
        key=[10, 30, 5, 50],
    )

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "key_column1": 10,
            "key_column2": 30,
            "key_column3": 5,
            "key_column4": 50,
            "number_of_stuff_sum_1h": 1.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_column_begin_t(setup_teardown_test):
    t0 = pd.Timestamp(setup_teardown_test.test_base_time)
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
                time_field="time",
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
            QueryByKey(["number_of_stuff_sum_1h", "t_col"], other_table, key_field=keys, time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"key_column": "a", "time": setup_teardown_test.test_base_time}, key=["a"])

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "key_column": "a",
            "number_of_stuff_sum_1h": 1.0,
            "t_col": "storey",
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_aggregate_float_key(setup_teardown_test):
    t0 = pd.Timestamp(setup_teardown_test.test_base_time)
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
                time_field="time",
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
            QueryByKey(["number_of_stuff_sum_1h"], other_table, key_field=keys, time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"key_column2": 8.6, "time": setup_teardown_test.test_base_time}, key=[8.6])

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "key_column2": 8.6,
            "number_of_stuff_sum_1h": 2.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


@pytest.mark.parametrize("flush_interval", [None, 1, 300])
def test_aggregate_and_query_persist_before_advancing_window(setup_teardown_test, flush_interval):
    table = Table(
        setup_teardown_test.table_name,
        setup_teardown_test.driver(),
        flush_interval_secs=flush_interval,
    )
    controller = build_flow(
        [
            SyncEmitSource(),
            AggregateByKey(
                [FieldAggregator("particles", "sample", ["count"], FixedWindows(["30m"]))],
                table,
                key_field="sample",
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    for i in range(22, -1, -1):
        data = {"number": i, "sample": "U235", "time": setup_teardown_test.test_base_time - timedelta(minutes=3 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "number": 22,
            "particles_count_30m": 1.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 20, 34, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 21,
            "particles_count_30m": 2.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 20, 37, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 20,
            "particles_count_30m": 3.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 20, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 19,
            "particles_count_30m": 4.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 20, 43, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 18,
            "particles_count_30m": 5.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 20, 46, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 17,
            "particles_count_30m": 6.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 20, 49, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 16,
            "particles_count_30m": 7.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 20, 52, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 15,
            "particles_count_30m": 8.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 20, 55, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 14,
            "particles_count_30m": 9.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 20, 58, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 13,
            "particles_count_30m": 1.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 1, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 12,
            "particles_count_30m": 2.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 4, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 11,
            "particles_count_30m": 3.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 7, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 10,
            "particles_count_30m": 4.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 10, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 9,
            "particles_count_30m": 5.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 13, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 8,
            "particles_count_30m": 6.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 16, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 7,
            "particles_count_30m": 7.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 19, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 6,
            "particles_count_30m": 8.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 22, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 5,
            "particles_count_30m": 9.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 25, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 4,
            "particles_count_30m": 10.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 28, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 3,
            "particles_count_30m": 1.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 31, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 2,
            "particles_count_30m": 2.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 34, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 1,
            "particles_count_30m": 3.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 37, tzinfo=datetime.timezone.utc),
        },
        {
            "number": 0,
            "particles_count_30m": 4.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
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
                key_field="sample",
                time_field="time",
                fixed_window_type=FixedWindowType.LastClosedWindow,
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()
    data = {"sample": "U235", "time": setup_teardown_test.test_base_time}
    controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "particles_count_30m": 10.0,
            "sample": "U235",
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        }
    ]
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
                time_field="time",
            ),
            NoSqlTarget(table),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    items_in_ingest_batch = 5
    for i in range(items_in_ingest_batch):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.emit({"col1": 8, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * 8)}, "tal")
    controller.emit({"col1": 9, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * 9)}, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 0,
            "number_of_stuff_sum_1h": 0.0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 1,
            "number_of_stuff_sum_1h": 1.0,
            "time": datetime.datetime(2020, 7, 21, 22, 5, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 2,
            "number_of_stuff_sum_1h": 3.0,
            "time": datetime.datetime(2020, 7, 21, 22, 30, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 3,
            "number_of_stuff_sum_1h": 6.0,
            "time": datetime.datetime(2020, 7, 21, 22, 55, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 4,
            "number_of_stuff_sum_1h": 9.0,
            "time": datetime.datetime(2020, 7, 21, 23, 20, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 8,
            "number_of_stuff_sum_1h": 8.0,
            "time": datetime.datetime(2020, 7, 22, 1, 0, tzinfo=datetime.timezone.utc),
        },
        {
            "col1": 9,
            "number_of_stuff_sum_1h": 17.0,
            "time": datetime.datetime(2020, 7, 22, 1, 25, tzinfo=datetime.timezone.utc),
        },
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    other_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["number_of_stuff_sum_1h"], other_table, time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = setup_teardown_test.test_base_time + timedelta(minutes=25 * 10)
    data = {"col1": 10, "time": base_time}
    controller.emit(data, "tal")

    controller.terminate()
    actual = controller.await_termination()
    expected_results = [
        {
            "col1": 10,
            "number_of_stuff_sum_1h": 17.0,
            "time": datetime.datetime(2020, 7, 22, 1, 50, tzinfo=datetime.timezone.utc),
        }
    ]

    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


def test_float_format(setup_teardown_test):
    t0 = pd.Timestamp(setup_teardown_test.test_base_time)
    floats_array = [1.5089695129344164e05, 1, 1.071743290547756e-05]
    data = pd.DataFrame(
        {
            "key_column2": [8.6, 8.6, 8.6],
            "float_data": floats_array,
            "time": [t0 - pd.Timedelta(minutes=25), t0 - pd.Timedelta(minutes=30), t0 - pd.Timedelta(minutes=31)],
        }
    )

    keys = ["key_column2"]
    table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            DataframeSource(data, key_field=keys),
            AggregateByKey(
                [
                    FieldAggregator(
                        "number_of_stuff",
                        "float_data",
                        ["sum"],
                        SlidingWindows(["1h"], "10m"),
                    )
                ],
                table,
                time_field="time",
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
            QueryByKey(["float_data", "number_of_stuff_sum_1h"], other_table, key_field=keys, time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()
    controller.emit({"key_column2": 8.6, "time": setup_teardown_test.test_base_time}, key=[8.6])
    controller.terminate()
    actual = controller.await_termination()

    expected_results = [
        {
            "float_data": floats_array[-1],
            "number_of_stuff_sum_1h": math.fsum(floats_array),
            "key_column2": 8.6,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        }
    ]
    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"


# ML-3378
def test_huge_fixed_window_starting_at_epoch(setup_teardown_test):

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
                        FixedWindows(["30000d"]),
                    )
                ],
                table,
                time_field="time",
            ),
            NoSqlTarget(table),
        ]
    ).run()

    for i in range(10):
        data = {"col1": i, "time": setup_teardown_test.test_base_time + timedelta(minutes=25 * i)}
        controller.emit(data, "tal")

    controller.terminate()
    controller.await_termination()

    query1_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["number_of_stuff_count_30000d"], query1_table, key_field=["key"], time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()
    controller.emit({"key": "tal", "time": setup_teardown_test.test_base_time}, key=[8.6])
    controller.terminate()
    actual = controller.await_termination()

    expected_results = [
        {
            "key": "tal",
            "number_of_stuff_count_30000d": 10,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        }
    ]
    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"

    query2_table = Table(setup_teardown_test.table_name, setup_teardown_test.driver())
    controller = build_flow(
        [
            SyncEmitSource(),
            QueryByKey(["number_of_stuff_count_1h"], query2_table, key_field=["key"], time_field="time"),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()
    controller.emit({"key": "tal", "time": setup_teardown_test.test_base_time}, key=[8.6])
    controller.terminate()
    actual = controller.await_termination()

    expected_results = [
        {
            "key": "tal",
            "number_of_stuff_count_1h": 0,
            "time": datetime.datetime(2020, 7, 21, 21, 40, tzinfo=datetime.timezone.utc),
        }
    ]
    assert (
        actual == expected_results
    ), f"actual did not match expected. \n actual: {actual} \n expected: {expected_results}"
