import asyncio
from datetime import datetime, timedelta

import pytest

from storey import Source, Map, Reduce, build_flow, Complete, NoopDriver, FieldAggregator, AggregateByKey, Cache, Batch, AsyncSource
from storey.dtypes import SlidingWindows

test_base_time = datetime.fromisoformat("2020-07-21T21:40:00+00:00")


@pytest.mark.parametrize('n', [0, 1, 1000, 5000])
def test_simple_flow_n_events(benchmark, n):
    def inner():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        for i in range(n):
            controller.emit(i)
        controller.terminate()
        termination_result = controller.await_termination()

    benchmark(inner)


@pytest.mark.parametrize('n', [0, 1, 1000, 5000])
def test_simple_async_flow_n_events(benchmark, n):
    async def async_inner():
        controller = await build_flow([
            AsyncSource(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        for i in range(n):
            await controller.emit(i)
        await controller.terminate()
        termination_result = await controller.await_termination()

    def inner():
        asyncio.run(async_inner())

    benchmark(inner)


@pytest.mark.parametrize('n', [0, 1, 1000, 5000])
def test_complete_flow_n_events(benchmark, n):
    def inner():
        controller = build_flow([
            Source(),
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
            Source(),
            AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                            SlidingWindows(['1h', '2h', '24h'], '10m'))],
                           Cache("test", NoopDriver())),
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
            Source(),
            Batch(4, 100),
        ]).run()

        for i in range(n):
            controller.emit(i)

        controller.terminate()
        controller.await_termination()

    benchmark(inner)
