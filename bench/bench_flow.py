import asyncio
from datetime import datetime, timedelta

from storey import Source, Map, Reduce, build_flow, Complete, NoopDriver, FieldAggregator, AggregateByKey, Cache, Batch, AsyncSource
from storey.dtypes import SlidingWindows

test_base_time = datetime.fromisoformat("2020-07-21T21:40:00+00:00")


def test_simple_flow_zero_events(benchmark):
    def inner():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        controller.terminate()
        termination_result = controller.await_termination()
        assert termination_result == 0

    benchmark(inner)


def test_simple_flow_one_event(benchmark):
    def inner():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        controller.emit(0)
        controller.terminate()
        termination_result = controller.await_termination()
        assert termination_result == 1

    benchmark(inner)


def test_complete_flow_one_event(benchmark):
    def inner():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Complete()
        ]).run()

        result = controller.emit(0, return_awaitable_result=True).await_result()
        assert result == 1
        controller.terminate()
        controller.await_termination()

    benchmark(inner)


def test_simple_flow_1000_events(benchmark):
    def inner():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        for i in range(1000):
            controller.emit(i)
        controller.terminate()
        termination_result = controller.await_termination()
        assert termination_result == 500500

    benchmark(inner)


def test_simple_async_flow_1000_events(benchmark):
    async def async_inner():
        controller = await build_flow([
            AsyncSource(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        for i in range(1000):
            await controller.emit(i)
        await controller.terminate()
        termination_result = await controller.await_termination()
        assert termination_result == 500500

    def inner():
        asyncio.run(async_inner())

    benchmark(inner)


def test_aggregate_by_key_1000_events(benchmark):
    def inner():
        controller = build_flow([
            Source(),
            AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max"],
                                            SlidingWindows(['1h', '2h', '24h'], '10m'))],
                           Cache("test", NoopDriver())),
        ]).run()

        for i in range(1000):
            data = {'col1': i}
            controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

        controller.terminate()
        controller.await_termination()

    benchmark(inner)


def test_batch_1000_events(benchmark):
    def inner():
        controller = build_flow([
            Source(),
            Batch(4, 100),
        ]).run()

        for i in range(1000):
            controller.emit(i)

        controller.terminate()
        controller.await_termination()

    benchmark(inner)
