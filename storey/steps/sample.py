from datetime import datetime, timezone
from typing import Optional, Union
from enum import Enum
from storey import Filter, Event


class Sample(Filter):
    """Samples stream events, passes a single event downstream after either reaching 'rate_count' or 'rate_seconds'"""

    class EmitPolicy(Enum):
        EMIT_FIRST = 1
        EMIT_LAST = 2

    def __init__(
        self,
        rate_count: Optional[int] = None,
        rate_seconds: Optional[Union[int, float]] = None,
        emit_policy: EmitPolicy = EmitPolicy.EMIT_FIRST,
        **kwargs,
    ):
        super().__init__(lambda event: self._sample(event), full_event=True, **kwargs)

        if rate_count is None and rate_seconds is None:
            raise ValueError(
                "Either rate_count or rate_seconds (or both) must be initialized"
            )

        if rate_count is not None and rate_count <= 0:
            raise ValueError(f"Expected rate_count > 0, got {rate_count}")

        if rate_seconds is not None and rate_seconds <= 0:
            raise ValueError(f"Expected rate_seconds > 0, got {rate_seconds}")

        self._rate_count = rate_count
        self._rate_seconds = rate_seconds
        self.emit_policy = emit_policy

        self._count = None
        self._seconds = None

    def _sample(self, event: Event):
        do_sample = False
        if self._rate_count is not None:
            do_sample = do_sample or self._sample_by_count(event)
        if self._rate_seconds is not None:
            do_sample = do_sample or self._sample_by_seconds(event)
        return do_sample

    def _sample_by_count(self, event: Event):
        if self._count is None:
            if self._rate_seconds is not None and self._seconds is None:
                self._seconds = event.time
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

    def _sample_by_seconds(self, event: Event):
        if self._seconds is None:
            self._seconds = event.time
            if self.emit_policy == self.EmitPolicy.EMIT_FIRST:
                return True
            elif self.emit_policy == self.EmitPolicy.EMIT_LAST:
                return False

        time_delta = datetime.now(timezone.utc) - self._seconds
        delta_seconds = time_delta.total_seconds()
        if self._rate_seconds <= delta_seconds:
            self._seconds = None
            if self.emit_policy == self.EmitPolicy.EMIT_FIRST:
                return False
            elif self.emit_policy == self.EmitPolicy.EMIT_LAST:
                return True

        return False
