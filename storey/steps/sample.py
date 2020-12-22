from enum import Enum

from storey import Filter


class Sample(Filter):
    """Samples stream events, passes a single event downstream after either reaching 'rate_count' or 'rate_seconds'"""

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
        self.emit_policy = emit_policy

    def _sample(self):
        if self._count is None:
            self._count = 1
            if self.emit_policy == self.EmitPolicy.EMIT_FIRST:
                return True
            elif self.emit_policy == self.EmitPolicy.EMIT_LAST:
                return False

        self._count += 1

        if self._count == self._rate_count:
            self._count = None
            if self.emit_policy == self.EmitPolicy.EMIT_FIRST:
                return False
            elif self.emit_policy == self.EmitPolicy.EMIT_LAST:
                return True

        return False
