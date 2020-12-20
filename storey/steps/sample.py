from datetime import datetime, timezone
from typing import Optional, Union

from storey import Filter, Event


class Sample(Filter):
    """Samples stream events, passes a single event downstream after either reaching 'rate_count' or 'rate_seconds'"""

    def __init__(
        self,
        rate_count: Optional[int] = None,
        rate_seconds: Optional[Union[int, float]] = None,
        **kwargs,
    ):
        super().__init__(lambda event: self._sample(event), **kwargs)

        if rate_count is None and rate_seconds is None:
            raise ValueError(
                f"Either rate_count or rate_seconds (or both) must be initialized"
            )

        if rate_count is not None and rate_count <= 0:
            raise ValueError(f"Expected rate_count > 0, got {rate_count}")

        if rate_seconds is not None and rate_seconds <= 0:
            raise ValueError(f"Expected rate_seconds > 0, got {rate_seconds}")

        self._rate_count = rate_count
        self._rate_seconds = rate_seconds

        self._count = None
        self._seconds = None

    def _sample(self, event: Event):
        return self._sample_by_count(event) or self._sample_by_seconds(event)

    def _sample_by_count(self, event: Event):
        if self._rate_count is None:
            return False

        if self._count is None:
            if self._rate_seconds is not None and self._seconds is None:
                self._seconds = event.time
            self._count = 0
            return True

        self._count += 1
        if self._count == self._rate_count:
            self._count = None

        return False

    def _sample_by_seconds(self, event: Event):
        if self._rate_seconds is None:
            return False

        if self._seconds is None:
            self._seconds = event.time
            return True

        time_delta = datetime.now(timezone.utc) - self._seconds
        delta_seconds = time_delta.total_seconds()
        if self._rate_seconds >= delta_seconds:
            self._seconds = None

        return False
