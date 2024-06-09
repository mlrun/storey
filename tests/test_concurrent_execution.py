import asyncio
import time

import pytest

from storey import AsyncEmitSource
from storey.flow import Complete, ConcurrentExecution, Reduce, build_flow
from tests.test_flow import append_and_return

event_processing_duration = 0.5


class SomeContext:
    def __init__(self):
        self.fn = lambda x: x


async def process_event_slow_asyncio(event, context):
    assert isinstance(context, SomeContext) and callable(context.fn)
    await asyncio.sleep(event_processing_duration)
    return event


def process_event_slow_io(event, context):
    assert isinstance(context, SomeContext) and callable(context.fn)
    time.sleep(event_processing_duration)
    return event


def process_event_slow_processing(event):
    start = time.monotonic()
    while time.monotonic() - start < event_processing_duration:
        pass
    return event


async def async_test_concurrent_execution(concurrency_mechanism, event_processor, pass_context):
    controller = build_flow(
        [
            AsyncEmitSource(),
            ConcurrentExecution(
                event_processor=event_processor,
                concurrency_mechanism=concurrency_mechanism,
                pass_context=pass_context,
                max_in_flight=10,
                context=SomeContext(),
            ),
            Reduce([], append_and_return),
        ]
    ).run()

    num_events = 8

    start = time.monotonic()
    for counter in range(num_events):
        await controller.emit(counter)

    await controller.terminate()
    result = await controller.await_termination()
    end = time.monotonic()

    assert result == list(range(num_events))
    assert end - start > event_processing_duration, "Run time cannot be less than the time to process a single event"
    assert (
        end - start < event_processing_duration * num_events
    ), "Run time must be less than the time to process all events in serial"


@pytest.mark.parametrize(
    ["concurrency_mechanism", "event_processor", "pass_context"],
    [
        ("asyncio", process_event_slow_asyncio, True),
        ("threading", process_event_slow_io, True),
        ("multiprocessing", process_event_slow_processing, False),
    ],
)
def test_concurrent_execution(concurrency_mechanism, event_processor, pass_context):
    asyncio.run(async_test_concurrent_execution(concurrency_mechanism, event_processor, pass_context))


async def async_test_concurrent_execution_multiprocessing_and_complete():
    controller = build_flow(
        [
            AsyncEmitSource(),
            ConcurrentExecution(
                event_processor=process_event_slow_processing,
                concurrency_mechanism="multiprocessing",
                max_in_flight=2,
            ),
            Complete(),
        ]
    ).run()

    event_body = "hello"
    try:
        res = await controller.emit(event_body)
        assert res == event_body
    finally:
        await controller.terminate()
        await controller.await_termination()


def test_concurrent_execution_multiprocessing_and_complete():
    asyncio.run(async_test_concurrent_execution_multiprocessing_and_complete())


def test_concurrent_execution_multiprocessing_and_full_event():
    with pytest.raises(
        ValueError,
        match='concurrency_mechanism="multiprocessing" may not be used in conjunction with full_event=True',
    ):
        ConcurrentExecution(
            event_processor=process_event_slow_processing,
            concurrency_mechanism="multiprocessing",
            full_event=True,
        )
