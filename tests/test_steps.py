from pytest import fail

from storey import build_flow, Source, Map
from storey.dtypes import FlowError, Event
from storey.steps import Flatten, SampleWindow, EmitPeriod, Assert, ForEach, Partition


def test_assert_each_event():
    try:
        controller = build_flow(
            [
                Source(),
                Assert().each_event(lambda event: event > 10)
            ]
        ).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except FlowError:
        return True

    fail("User defined assert not failing", False)


def test_assert_at_least():
    try:
        controller = build_flow(
            [
                Source(),
                Assert().at_least(2)
            ]
        ).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow(
            [
                Source(),
                Assert().at_least(2)
            ]
        ).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)


def test_assert_above():
    try:
        controller = build_flow(
            [
                Source(),
                Assert().above(1)]
        ).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow(
            [
                Source(),
                Assert().above(1)
            ]
        ).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)


def test_assert_at_most():
    try:
        controller = build_flow(
            [
                Source(),
                Assert().at_most(2)
            ]
        ).run()
        controller.emit(1)
        controller.emit(2)
        controller.emit(3)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow(
            [
                Source(),
                Assert().at_most(2)
            ]
        ).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)


def test_assert_exactly():
    try:
        controller = build_flow(
            [
                Source(),
                Assert().exactly(2)
            ]
        ).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow(
            [
                Source(),
                Assert().exactly(2)
            ]
        ).run()
        controller.emit(1)
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow(
            [
                Source(),
                Assert().exactly(2)
            ]
        ).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)

    try:
        controller = build_flow(
            [
                Source(),
                Assert().match_exactly([1, 1, 1])
            ]
        ).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow(
            [
                Source(),
                Assert().match_exactly([1, 1, 1])
            ]
        ).run()
        controller.emit(1)
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)


def test_assert_all_of():
    try:
        controller = build_flow(
            [
                Source(),
                Assert().match_all_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            ]
        ).run()
        controller.emit([1, 2, 3])
        controller.emit([4, 5, 6])
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow(
            [
                Source(),
                Assert().match_all_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            ]
        ).run()
        controller.emit([1, 2, 3])
        controller.emit([4, 5, 6])
        controller.emit([7, 8, 9])
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)


def test_assert_any_of():
    try:
        controller = build_flow(
            [
                Source(),
                Assert().match_any_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            ]
        ).run()
        controller.emit([10, 11, 12])
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow(
            [
                Source(),
                Assert().match_any_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            ]
        ).run()
        controller.emit([1, 2, 3])
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)


def test_assert_none_of():
    try:
        controller = build_flow(
            [
                Source(),
                Assert().match_none_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            ]
        ).run()
        controller.emit([10, 11, 12])
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)

    try:
        controller = build_flow(
            [
                Source(),
                Assert().match_none_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            ]
        ).run()
        controller.emit([1, 2, 3])
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass


def test_sample_emit_first():
    controller = build_flow(
        [
            Source(),
            Assert().exactly(5),
            SampleWindow(5),
            Assert().exactly(1).match_exactly([0]),
        ]
    ).run()

    for i in range(0, 5):
        controller.emit(i)
    controller.terminate()
    controller.await_termination()


def test_sample_emit_first_with_emit_before_termination():
    controller = build_flow(
        [
            Source(),
            Assert().exactly(5),
            SampleWindow(5, emit_before_termination=True),
            Assert().exactly(2).match_exactly([0, 4]),
        ]
    ).run()

    for i in range(0, 5):
        controller.emit(i)
    controller.terminate()
    controller.await_termination()


def test_sample_emit_last():
    controller = build_flow(
        [
            Source(),
            Assert().exactly(5),
            SampleWindow(5, emit_period=EmitPeriod.LAST),
            Assert().exactly(1).match_exactly([5]),
        ]
    ).run()

    for i in range(0, 5):
        controller.emit(i)
    controller.terminate()
    controller.await_termination()


def test_sample_emit_last_with_emit_before_termination():
    controller = build_flow(
        [
            Source(),
            Assert().exactly(5),
            SampleWindow(5, emit_period=EmitPeriod.LAST, emit_before_termination=True),
            Assert().exactly(1).match_exactly([5]),
        ]
    ).run()

    for i in range(0, 5):
        controller.emit(i)
    controller.terminate()
    controller.await_termination()


def test_flatten():
    controller = build_flow(
        [
            Source(),
            Flatten(),
            Assert().match_exactly([1, 2, 3, 4, 5, 6])
        ]
    ).run()

    controller.emit([[1, 2], [3, 4], [5, 6]])
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
            Assert(full_event=True).each_event(lambda event: event.id in er.events),
        ]
    ).run()

    controller.emit(1)
    controller.emit(1)
    controller.emit(1)
    controller.emit(1)
    controller.emit(1)
    controller.emit(1)

    controller.terminate()
    controller.await_termination()


def test_partition():
    dividable_by_two = {2, 4, 6}
    not_dividable_by_two = {1, 3, 5}

    def check_partition(event: Event):
        first = event.body.left
        second = event.body.right

        if first is not None:
            return first in dividable_by_two and first not in not_dividable_by_two and second is None
        else:
            return second in not_dividable_by_two and second not in dividable_by_two

    controller = build_flow(
        [
            Source(),
            Assert().exactly(6),
            Partition(lambda event: event.body % 2 == 0),
            Assert().exactly(6),
            Assert(full_event=True).each_event(lambda event: check_partition(event))
        ]
    ).run()

    controller.emit(1)
    controller.emit(2)
    controller.emit(3)
    controller.emit(4)
    controller.emit(5)
    controller.emit(6)

    controller.terminate()
    controller.await_termination()
