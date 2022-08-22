# Copyright 2018 Iguazio
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
from enum import Enum

from storey import Flow
from storey.dtypes import Event, _termination_obj
from typing import Optional, Union, Callable


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
    :param key: The key by which events are sampled. By default (None), events are not sampled by key.
        Other options may be:
        Set to '$key' to sample events by the Event.key property.
        set to 'str' key to sample events by Event.body[str].
        set a Callable[[Event], str] to sample events by a custom key extractor.
    """

    def __init__(
        self,
        window_size: int,
        emit_period: EmitPeriod = EmitPeriod.FIRST,
        emit_before_termination: bool = False,
        key: Optional[Union[str, Callable[[Event], str]]] = None,
        **kwargs,
    ):
        kwargs["full_event"] = True
        super().__init__(**kwargs)

        if window_size <= 1:
            raise ValueError(f"Expected window_size > 1, found {window_size}")

        if not isinstance(emit_period, EmitPeriod):
            raise ValueError(f"Expected emit_period of type `EmitPeriod`, got {type(emit_period)}")

        self._window_size = window_size
        self._emit_period = emit_period
        self._emit_before_termination = emit_before_termination
        self._per_key_count = dict()
        self._count = 0
        self._last_event = None
        self._extract_key: Callable[[Event], str] = self._create_key_extractor(key)

    @staticmethod
    def _create_key_extractor(key) -> Callable:
        if key is None:
            return lambda event: None
        elif callable(key):
            return key
        elif isinstance(key, str):
            if key == '$key':
                return lambda event: event.key
            else:
                return lambda event: event.body[key]
        else:
            raise ValueError(f'Unsupported key type {type(key)}')

    async def _do(self, event):
        if event is _termination_obj:
            if self._last_event is not None:
                await self._do_downstream(self._last_event)
            return await self._do_downstream(_termination_obj)
        else:
            key = self._extract_key(event)

            if key is not None:
                if key not in self._per_key_count:
                    self._per_key_count[key] = 1
                else:
                    self._per_key_count[key] += 1
                count = self._per_key_count[key]
            else:
                self._count += 1
                count = self._count

            if self._emit_before_termination:
                self._last_event = event

            if count == self._window_size:
                if key is not None:
                    self._per_key_count[key] = 0
                else:
                    self._count = 0
            if self._should_emit(count):
                self._last_event = None
                await self._do_downstream(event)

    def _should_emit(self, count):
        if self._emit_period == EmitPeriod.FIRST and count == 1:
            return True
        elif self._emit_period == EmitPeriod.LAST and count == self._window_size:
            return True
        return False
