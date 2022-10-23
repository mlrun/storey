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
from pytest import fail

from storey import SyncEmitSource, build_flow
from storey.dtypes import Event
from storey.steps import Assert, EmitPeriod, Flatten, ForEach, Partition, SampleWindow


def test_assert_each_event():
    try:
        controller = build_flow([SyncEmitSource(), Assert().each_event(lambda event: event > 10)]).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except AssertionError:
        pass


def test_assert_greater_or_equal_to():
    try:
        controller = build_flow([SyncEmitSource(), Assert().greater_or_equal_to(2)]).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except AssertionError:
        pass

    try:
        controller = build_flow([SyncEmitSource(), Assert().greater_or_equal_to(2)]).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except AssertionError:
        fail("Assert failed unexpectedly", False)


def test_assert_greater_than():
    try:
        controller = build_flow([SyncEmitSource(), Assert().greater_than(1)]).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except AssertionError:
        pass

    try:
        controller = build_flow([SyncEmitSource(), Assert().greater_than(1)]).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except AssertionError:
        fail("Assert failed unexpectedly", False)


def test_assert_less_or_equal():
    try:
        controller = build_flow([SyncEmitSource(), Assert().less_or_equal_to(2)]).run()
        controller.emit(1)
        controller.emit(2)
        controller.emit(3)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except AssertionError:
        pass

    try:
        controller = build_flow([SyncEmitSource(), Assert().less_or_equal_to(2)]).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except AssertionError:
        fail("Assert failed unexpectedly", False)


def test_assert_exactly():
    try:
        controller = build_flow([SyncEmitSource(), Assert().exactly(2)]).run()
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except AssertionError:
        pass

    try:
        controller = build_flow([SyncEmitSource(), Assert().exactly(2)]).run()
        controller.emit(1)
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except AssertionError:
        pass

    try:
        controller = build_flow([SyncEmitSource(), Assert().exactly(2)]).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except AssertionError:
        fail("Assert failed unexpectedly", False)


def test_assert_match_exactly():
    try:
        controller = build_flow([SyncEmitSource(), Assert(full_event=False).match_exactly([1, 1, 1])]).run()
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except AssertionError:
        pass

    try:
        controller = build_flow([SyncEmitSource(), Assert().match_exactly([1, 1, 1])]).run()
        controller.emit(1)
        controller.emit(1)
        controller.emit(1)
        controller.terminate()
        controller.await_termination()
    except AssertionError:
        fail("Assert failed unexpectedly", False)


def test_assert_all_of():
    try:
        controller = build_flow(
            [
                SyncEmitSource(),
                Assert().contains_all_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
            ]
        ).run()
        controller.emit([1, 2, 3])
        controller.emit([4, 5, 6])
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except AssertionError:
        pass

    try:
        controller = build_flow(
            [
                SyncEmitSource(),
                Assert().contains_all_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
            ]
        ).run()
        controller.emit([1, 2, 3])
        controller.emit([4, 5, 6])
        controller.emit([7, 8, 9])
        controller.terminate()
        controller.await_termination()
    except AssertionError:
        fail("Assert failed unexpectedly", False)


def test_assert_any_of():
    try:
        controller = build_flow(
            [
                SyncEmitSource(),
                Assert().contains_any_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
            ]
        ).run()
        controller.emit([10, 11, 12])
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except AssertionError:
        pass

    try:
        controller = build_flow(
            [
                SyncEmitSource(),
                Assert().contains_any_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
            ]
        ).run()
        controller.emit([1, 2, 3])
        controller.terminate()
        controller.await_termination()
    except AssertionError:
        fail("Assert failed unexpectedly", False)


def test_assert_none_of():
    try:
        controller = build_flow(
            [
                SyncEmitSource(),
                Assert().contains_none_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
            ]
        ).run()
        controller.emit([10, 11, 12])
        controller.terminate()
        controller.await_termination()
    except AssertionError:
        fail("Assert failed unexpectedly", False)

    try:
        controller = build_flow(
            [
                SyncEmitSource(),
                Assert().contains_none_of([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
            ]
        ).run()
        controller.emit([1, 2, 3])
        controller.terminate()
        controller.await_termination()
        fail("Assert not failing", False)
    except AssertionError:
        pass


def test_sample_emit_first():
    controller = build_flow(
        [
            SyncEmitSource(),
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
            SyncEmitSource(),
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
            SyncEmitSource(),
            Assert().exactly(5),
            SampleWindow(5, emit_period=EmitPeriod.LAST),
            Assert().exactly(1).match_exactly([4]),
        ]
    ).run()

    for i in range(0, 5):
        controller.emit(i)
    controller.terminate()
    controller.await_termination()


def test_sample_emit_last_with_emit_before_termination():
    controller = build_flow(
        [
            SyncEmitSource(),
            Assert().exactly(5),
            SampleWindow(5, emit_period=EmitPeriod.LAST, emit_before_termination=True),
            Assert().exactly(1).match_exactly([4]),
        ]
    ).run()

    for i in range(0, 5):
        controller.emit(i)
    controller.terminate()
    controller.await_termination()


def test_sample_emit_event_per_key():
    controller = build_flow(
        [
            SyncEmitSource(key_field=str),
            Assert().exactly(25),
            SampleWindow(5, key="$key"),
            Assert().exactly(5).match_exactly([0, 1, 2, 3, 4]),
        ]
    ).run()

    for i in range(0, 25):
        key = f"key_{i % 5}"
        controller.emit(i, key=key)
    controller.terminate()
    controller.await_termination()


def test_flatten():
    controller = build_flow([SyncEmitSource(), Flatten(), Assert().contains_all_of([1, 2, 3, 4, 5, 6])]).run()

    controller.emit([1, 2, 3, 4, 5, 6])
    controller.terminate()
    controller.await_termination()


def test_flatten_forces_full_event_false():
    controller = build_flow(
        [
            SyncEmitSource(),
            Flatten(full_event=True),
            Assert().contains_all_of([1, 2, 3, 4, 5, 6]),
        ]
    ).run()

    controller.emit([1, 2, 3, 4, 5, 6])
    controller.terminate()
    controller.await_termination()


def test_foreach():
    event_ids = set()
    controller = build_flow(
        [
            SyncEmitSource(),
            ForEach(lambda e: event_ids.add(e.id), full_event=True),
            Assert(full_event=True).each_event(lambda event: event.id in event_ids),
        ]
    ).run()

    for i in range(0, 5):
        controller.emit(i)

    controller.terminate()
    controller.await_termination()


def test_partition():
    divisible_by_two = {2, 4, 6}
    not_divisible_by_two = {1, 3, 5}

    def check_partition(event: Event):
        first = event.body.left
        second = event.body.right

        if first is not None:
            return first in divisible_by_two and first not in not_divisible_by_two and second is None
        else:
            return second in not_divisible_by_two and second not in divisible_by_two

    controller = build_flow(
        [
            SyncEmitSource(),
            Assert().exactly(6),
            Partition(lambda event: event.body % 2 == 0),
            Assert().exactly(6),
            Assert(full_event=True).each_event(lambda event: check_partition(event)),
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
