from dataclasses import dataclass
from typing import Callable, List, Any, Iterable

from storey.dtypes import _termination_obj
from storey.flow import Flow


@dataclass
class _Operator:
    str: str
    fn: Callable[[Any, Any], bool]

    def __call__(self, x, y):
        return self.fn(x, y)

    def __str__(self):
        return self.str


_EQUALS = _Operator("==", lambda x, y: x == y)
_NOT_EQUAL = _Operator("!=", lambda x, y: x != y)
_GREATER_THAN = _Operator(">", lambda x, y: x > y)
_LESS_THEN = _Operator("<", lambda x, y: x < y)
_GREATER_OR_EQUAL = _Operator(">=", lambda x, y: x >= y)
_LESS_OR_EQUAL = _Operator("<=", lambda x, y: x <= y)


def _intersect(first_iterable: Iterable, second_iterable: Iterable):
    return any(set(first_iterable).intersection(second_iterable))


def _subset(first_iterable: Iterable, second_iterable: Iterable):
    return set(first_iterable).issubset(second_iterable)


def _disjoint(first_iterable: Iterable, second_iterable: Iterable):
    return set(first_iterable).isdisjoint(second_iterable)


_INTERSECT = _Operator("any of", _intersect)
_SUBSET = _Operator("all of", _subset)
_IDENTICAL = _Operator("exactly", lambda x, y: _SUBSET(x, y) and _SUBSET(y, x))
_DISJOINT = _Operator("none of", _disjoint)
_NONE = _Operator("none of", lambda x, y: not _INTERSECT(x, y))

_NOTHING = _Operator("do nothing", lambda x, y: False)


class _Assertable:
    def __call__(self, event: Any):
        raise NotImplementedError

    def check(self):
        raise NotImplementedError


class _AssertEventCount(_Assertable):
    def __init__(self, expected: int = 0, operator: _Operator = _NOTHING):
        self.expected: int = expected
        self.operator: _Operator = operator
        self.actual: int = 0

    def __call__(self, event):
        self.actual += 1

    def check(self):
        op = self.operator(self.actual, self.expected)
        assert (
            op
        ), f"Expected event count {self.operator} {self.expected}, got {self.actual} instead"


class _AssertCollection(_Assertable):
    def __init__(
        self,
        expected: Iterable[Any],
        operator: _Operator = _NOTHING,
    ):
        self.expected = expected
        self.operator: _Operator = operator
        self.actual = []

    def __call__(self, event):
        self.actual.append(event)

    def check(self):
        op = self.operator(self.expected, self.actual)
        assert op, f"Expected {self.operator} {self.actual} in {self.expected}"


class _AssertPredicate(_Assertable):
    def __init__(self, predicate: Callable[[Any], bool]):
        self.predicate = predicate

    def __call__(self, event):
        predicate = self.predicate(event)
        assert predicate, f"Predicate results in False for Event {event}"

    def check(self):
        pass


class Assert(Flow):
    """Exposes an API for testing the flow between steps."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.termination_assertions: List[_Assertable] = []
        self.execution_assertions: List[_Assertable] = []

    def each_event(self, predicate: Callable[[Any], bool]):
        self.execution_assertions.append(_AssertPredicate(predicate))
        return self

    def greater_or_equal_to(self, expected: int):
        self.termination_assertions.append(
            _AssertEventCount(expected, _GREATER_OR_EQUAL)
        )
        return self

    def greater_than(self, expected: int):
        self.termination_assertions.append(_AssertEventCount(expected, _GREATER_THAN))
        return self

    def less_than(self, expected: int):
        self.termination_assertions.append(_AssertEventCount(expected, _LESS_THEN))
        return self

    def less_or_equal_to(self, expected: int):
        self.termination_assertions.append(_AssertEventCount(expected, _LESS_OR_EQUAL))
        return self

    def exactly(self, expected: int):
        self.termination_assertions.append(_AssertEventCount(expected, _EQUALS))
        return self

    def match_exactly(self, expected: Iterable[Any]):
        self.termination_assertions.append(_AssertCollection(expected, _IDENTICAL))
        return self

    def match_all_of(self, expected: Iterable[Any]):
        self.termination_assertions.append(_AssertCollection(expected, _SUBSET))
        return self

    def match_any_of(self, expected: Iterable[Any]):
        self.termination_assertions.append(_AssertCollection(expected, _INTERSECT))
        return self

    def match_none_of(self, expected: Iterable[Any]):
        self.termination_assertions.append(_AssertCollection(expected, _NONE))
        return self

    async def _do(self, event):
        if event is _termination_obj:
            for assertion in self.termination_assertions:
                assertion.check()
            return await self._do_downstream(_termination_obj)

        element = self._get_event_or_body(event)

        for assertion in self.execution_assertions:
            assertion(element)

        for assertion in self.termination_assertions:
            assertion(element)

        await self._do_downstream(event)
