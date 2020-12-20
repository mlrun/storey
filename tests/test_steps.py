from pytest import fail

from storey import build_flow, Source, Map
from storey.dtypes import FlowError, Event
from storey.steps import Flatten, Sample, Assert, ForEach
from time import sleep


def test_assert():
    try:
        controller = build_flow(
            [Source(), Assert(lambda event: event > 10, raise_on_exception=True)]
        ).run()
        controller.emit(1)
        controller.await_termination()
    except FlowError:
        return True

    fail("User defined assert not failing", False)


def test_sample_by_count():
    class CountUntil:
        def __init__(self, max: int):
            self.max = max
            self.count = 0

        def __call__(self, event):
            self.count += 1
            return self.count <= self.max

    controller = build_flow(
        [Source(), Assert(CountUntil(5)), Sample(5), Assert(CountUntil(1))]
    ).run()

    controller.emit(1)
    controller.emit(1)
    controller.emit(1)
    controller.emit(1)
    controller.emit(1)
    controller.terminate()
    controller.await_termination()


def test_sample_by_seconds():
    class CountUntil:
        def __init__(self, max: int):
            self.max = max
            self.count = 0

        def __call__(self, event):
            self.count += 1
            return self.count <= self.max

    controller = build_flow(
        [Source(), Assert(CountUntil(6)), Sample(rate_count=2), Assert(CountUntil(2))]
    ).run()

    controller.emit(1)
    controller.emit(1)
    controller.emit(1)

    sleep(2)

    controller.emit(1)
    controller.emit(1)
    controller.emit(1)

    controller.terminate()
    controller.await_termination()


def test_flatten():
    controller = build_flow(
        [Source(), Flatten(), Assert(lambda event: event == [1, 2, 3, 4, 5, 6])]
    ).run()

    controller.emit([[1, 2], [3, 4], [5, 6]])
    controller.terminate()
    controller.await_termination()


def test_flatten_to_other_type():
    controller = build_flow(
        [Source(), Flatten(to_set=True), Assert(lambda event: event == {1})]
    ).run()

    controller.emit([[1, 1], [1, 1], [1, 1]])
    controller.terminate()
    controller.await_termination()


def test_foreach():
    class EventRegistry:
        def __init__(self):
            self.events = set()

        def __call__(self, event: Event):
            self.events.add(event.id)
            return event

    er = EventRegistry()
    controller = build_flow(
        [
            Source(),
            Map(er, full_event=True),
            ForEach(lambda x: None),
            Assert(lambda event: event.id in er.events, full_event=True),
        ]
    ).run()

    controller.emit(1)
    controller.emit(1)
    controller.emit(1)

    sleep(2)

    controller.emit(1)
    controller.emit(1)
    controller.emit(1)

    controller.terminate()
    controller.await_termination()
