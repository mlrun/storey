from enum import Enum

from storey import Flow
from storey.dtypes import _termination_obj


class EmitPeriod(Enum):
    FIRST = 1
    LAST = 2


class SampleWindow(Flow):
    """
    Emits a single event in a window of `window_size` events, in accordance with `emit_period` and `emit_before_termination`.

    :param window_size: The size of the window we want to sample a single event from.
    :param emit_period: What event should this step emit for each `window_size` (default: EmitPeriod.First).
    Available options:
        1.1) EmitPeriod.FIRST - will emit the first event in a window `window_size` events.
        1.2) EmitPeriod.LAST - will emit the last event in a window of `window_size` events.
    :param emit_before_termination: On termination signal, should the step emit the last event it seen (default: False).
    Available options:
        2.1) True - The last event seen will be emitted downstream.
        2.2) False - The last event seen will NOT be emitted downstream.
    """

    def __init__(
        self,
        window_size: int,
        emit_period: EmitPeriod = EmitPeriod.FIRST,
        emit_before_termination: bool = False,
        **kwargs,
    ):
        super().__init__(full_event=True, **kwargs)

        if window_size <= 1:
            raise ValueError(f"Expected window_size > 1, found {window_size}")

        if not isinstance(emit_period, EmitPeriod):
            raise ValueError(f"Expected emit_period of type `EmitPeriod`, got {type(emit_period)}")

        self._window_size = window_size
        self._emit_period = emit_period
        self._emit_before_termination = emit_before_termination

        self._count = 0
        self._last_event = None

    async def _do(self, event):
        if event is _termination_obj:
            if self._last_event is not None:
                await self._do_downstream(self._last_event)
            return await self._do_downstream(_termination_obj)
        else:
            self._count += 1

            if self._emit_before_termination:
                self._last_event = event

            if self._should_emit():
                self._last_event = None
                await self._do_downstream(event)

            if self._count == self._window_size:
                self._count = 0

    def _should_emit(self):
        if self._emit_period == EmitPeriod.FIRST and self._count == 1:
            return True
        elif self._emit_period == EmitPeriod.LAST and self._count == self._window_size:
            return True
        return False
