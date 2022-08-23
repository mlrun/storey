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
from datetime import datetime, timedelta

import pandas as pd
import pytest

from storey import SyncEmitSource, Map, Reduce, build_flow, Complete, Driver, FieldAggregator, AggregateByKey, Table, Batch, \
    AsyncEmitSource, \
    DataframeSource
from storey.dtypes import SlidingWindows

test_base_time = datetime.fromisoformat("2020-07-21T21:40:00+00:00")


@pytest.mark.parametrize('n', [0, 1, 1000, 5000])
def test_simple_flow_n_events(benchmark, n):
    def inner():
        controller = build_flow([
            SyncEmitSource(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        for i in range(n):
            controller.emit(i)
        controller.terminate()
        controller.await_termination()

    benchmark(inner)


@pytest.mark.parametrize('n', [0, 1, 1000, 5000])
def test_simple_async_flow_n_events(benchmark, n):
    async def async_inner():
        controller = build_flow([
            AsyncEmitSource(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        for i in range(n):
            await controller.emit(i)
        await controller.terminate()
        await controller.await_termination()

    def inner():
        asyncio.run(async_inner())

    benchmark(inner)


@pytest.mark.parametrize('n', [0, 1, 1000, 5000])
def test_complete_flow_n_events(benchmark, n):
    def inner():
        controller = build_flow([
            SyncEmitSource(),
            Map(lambda x: x + 1),
            Complete()
        ]).run()

        for i in range(n):
            result = controller.emit(i, return_awaitable_result=True).await_result()
            assert result == i + 1
        controller.terminate()
        controller.await_termination()

    benchmark(inner)


@pytest.mark.parametrize('n', [0, 1, 1000, 5000])
def test_aggregate_by_key_n_events(benchmark, n):
    def inner():
        controller = build_flow([
            SyncEmitSource(),
            AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                            SlidingWindows(['1h', '2h', '24h'], '10m'))],
                           Table("test", Driver())),
        ]).run()

        for i in range(n):
            data = {'col1': i}
            controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

        controller.terminate()
        controller.await_termination()

    benchmark(inner)


@pytest.mark.parametrize('n', [0, 1, 1000, 5000])
def test_batch_n_events(benchmark, n):
    def inner():
        controller = build_flow([
            SyncEmitSource(),
            Batch(4, 100),
        ]).run()

        for i in range(n):
            controller.emit(i)

        controller.terminate()
        controller.await_termination()

    benchmark(inner)


def test_aggregate_df_86420_events(benchmark):
    df = pd.read_csv('bench/early_sense.csv', parse_dates=['timestamp'])

    def inner():
        driver = Driver()
        table = Table(f'test', driver)

        controller = build_flow([
            DataframeSource(df, key_field='patient_id', time_field='timestamp'),
            AggregateByKey([FieldAggregator("hr", "hr", ["avg", "min", "max"],
                                            SlidingWindows(['1h', '2h'], '10m')),
                            FieldAggregator("rr", "rr", ["avg", "min", "max"],
                                            SlidingWindows(['1h', '2h'], '10m')),
                            FieldAggregator("spo2", "spo2", ["avg", "min", "max"],
                                            SlidingWindows(['1h', '2h'], '10m'))],
                           table)
        ]).run()

        controller.await_termination()

    benchmark(inner)


def test_aggregate_df_86420_events_basic(benchmark):
    df = pd.read_csv('bench/early_sense.csv', parse_dates=['timestamp'])

    def inner():
        driver = Driver()
        table = Table(f'test', driver)

        controller = build_flow([
            DataframeSource(df, key_field='patient_id', time_field='timestamp'),
            AggregateByKey([FieldAggregator("hr", "hr", ["sum", "count"],
                                            SlidingWindows(['1h', '2h'], '10m')),
                            FieldAggregator("rr", "rr", ["sum", "count"],
                                            SlidingWindows(['1h', '2h'], '10m')),
                            FieldAggregator("spo2", "spo2", ["sum", "count"],
                                            SlidingWindows(['1h', '2h'], '10m'))],
                           table),
        ]).run()

        controller.await_termination()

    benchmark(inner)
