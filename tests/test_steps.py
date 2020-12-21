from time import sleep

from pytest import fail

from storey import build_flow, Source, Map
from storey.dtypes import FlowError, Event
from storey.steps import Flatten, Sample, Assert, ForEach


def test_assert_each_event():
    try:
        controller = build_flow(
            [Source(), Assert().each_event(lambda event: event > 10)]
        ).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except FlowError:
        return True

    fail("User defined assert not failing", False)


def test_assert_at_least():
    try:
        controller = build_flow([Source(), Assert().at_least(2)]).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow([Source(), Assert().at_least(2)]).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)


def test_assert_above():
    try:
        controller = build_flow([Source(), Assert().above(1)]).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow([Source(), Assert().above(1)]).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)


def test_assert_at_most():
    try:
        controller = build_flow([Source(), Assert().at_most(2)]).run()
        controller.emit(1)
        controller.emit(2)
        controller.emit(3)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow([Source(), Assert().at_most(2)]).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)


def test_assert_exactly():
    try:
        controller = build_flow([Source(), Assert().exactly(2)]).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow([Source(), Assert().exactly(2)]).run()
        controller.emit(1)
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow([Source(), Assert().exactly(2)]).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)

    try:
        controller = build_flow([Source(), Assert().match_exactly([1, 1, 1])]).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow([Source(), Assert().match_exactly([1, 1, 1])]).run()
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
            [Source(), Assert().match_all_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])]
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
            [Source(), Assert().match_all_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])]
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
            [Source(), Assert().match_any_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])]
        ).run()
        controller.emit([10, 11, 12])
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass

    try:
        controller = build_flow(
            [Source(), Assert().match_any_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])]
        ).run()
        controller.emit([1, 2, 3])
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)


def test_assert_none_of():
    try:
        controller = build_flow(
            [Source(), Assert().match_none_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])]
        ).run()
        controller.emit([10, 11, 12])
        controller.terminate()
        controller.await_termination()
    except FlowError:
        fail("Assert failed unexpectedly", False)

    try:
        controller = build_flow(
            [Source(), Assert().match_none_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]])]
        ).run()
        controller.emit([1, 2, 3])
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except FlowError:
        pass


def test_sample_by_count():
    controller = build_flow(
        [
            Source(),
            Assert().exactly(5),
            Sample(5),
            Assert().exactly(1).match_exactly([1]),
        ]
    ).run()

    controller.emit(1)
    controller.emit(2)
    controller.emit(3)
    controller.emit(4)
    controller.emit(5)
    controller.terminate()
    controller.await_termination()


def test_sample_by_seconds():
    controller = build_flow(
        [Source(), Assert().exactly(6), Sample(rate_count=2), Assert().exactly(2)]
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
        [Source(), Flatten(), Assert().match_exactly([1, 2, 3, 4, 5, 6])]
    ).run()

    controller.emit([[1, 2], [3, 4], [5, 6]])
    controller.terminate()
    controller.await_termination()


def test_flatten_to_other_type():
    controller = build_flow(
        [
            Source(),
            Assert().exactly(1).match_exactly([[[1, 1], [1, 1], [1, 1]]]),
            Flatten(to_set=True),
            Assert().exactly(1).match_exactly([1]),
        ]
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
