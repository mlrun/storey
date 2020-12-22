from dataclasses import dataclass
from typing import Callable, List, Any, Collection

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
_NOT_EQUALS = _Operator("!=", lambda x, y: x != y)
_ABOVE = _Operator(">", lambda x, y: x > y)
_BELOW = _Operator("<", lambda x, y: x < y)
_ABOVE_OR_EQUAL = _Operator(">=", lambda x, y: x >= y)
_BELOW_OR_EQUAL = _Operator("<=", lambda x, y: x <= y)

_ANY = _Operator("any of", lambda x, y: any([i for i in x if i in y]))
_ALL = _Operator("all of", lambda x, y: len([i for i in x if i in x]) == len(y))
_EXACTLY = _Operator("exactly", lambda x, y: _ALL(x, y) and _ALL(y, x))
_NONE = _Operator("none of", lambda x, y: not _ANY(x, y))

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
        assert op, f"Expected event count {self.operator} {self.expected}, got {self.actual} instead"


class _AssertCollection(_Assertable):
    def __init__(
        self,
        expected: Collection[Any],
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

    def at_least(self, expected: int):
        self.termination_assertions.append(_AssertEventCount(expected, _ABOVE_OR_EQUAL))
        return self

    def above(self, expected: int):
        self.termination_assertions.append(_AssertEventCount(expected, _ABOVE))
        return self

    def below(self, expected: int):
        self.termination_assertions.append(_AssertEventCount(expected, _BELOW))
        return self

    def at_most(self, expected: int):
        self.termination_assertions.append(_AssertEventCount(expected, _BELOW_OR_EQUAL))
        return self

    def exactly(self, expected: int):
        self.termination_assertions.append(_AssertEventCount(expected, _EQUALS))
        return self

    def match_exactly(self, expected: Collection[Any]):
        self.termination_assertions.append(_AssertCollection(expected, _EXACTLY))
        return self

    def match_all_of(self, expected: Collection[Any]):
        self.termination_assertions.append(_AssertCollection(expected, _ALL))
        return self

    def match_any_of(self, expected: Collection[Any]):
        self.termination_assertions.append(_AssertCollection(expected, _ANY))
        return self

    def match_none_of(self, expected: Collection[Any]):
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
