from enum import Enum

from storey import Filter


class Sample(Filter):
    """
    Samples stream events, passes a single event downstream accordingly to emit_policy. By default emit_policy is set to
    EMIT_FIRST, meaning that for Sample(rate_count=n) - first event will be passed downstream, while the n-1 next events
    are filtered out. Other wise if emit_policy is set to EMIT_LAST - the first n-1 events are filtered out, but event
    n is passed downstream.
    """

    class EmitPolicy(Enum):
        EMIT_FIRST = 1
        EMIT_LAST = 2

    def __init__(
        self,
        rate_count: int = 0,
        emit_policy: EmitPolicy = EmitPolicy.EMIT_FIRST,
        **kwargs,
    ):
        super().__init__(lambda event: self._sample(), full_event=True, **kwargs)

        if rate_count <= 0:
            raise ValueError(f"Expected rate_count > 0, found {rate_count}")

        self._rate_count = rate_count
        self._emit_policy = emit_policy
        self._count = None

    def _sample(self):
        if self._count is None:
            self._count = 1
            if self._emit_policy == self.EmitPolicy.EMIT_FIRST:
                return True
            elif self._emit_policy == self.EmitPolicy.EMIT_LAST:
                return False

        self._count += 1

        if self._count == self._rate_count:
            self._count = None
            if self._emit_policy == self.EmitPolicy.EMIT_FIRST:
                return False
            elif self._emit_policy == self.EmitPolicy.EMIT_LAST:
                return True

        return False
