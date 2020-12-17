from typing import Optional, Union

from storey.dtypes import Event
from storey.flow import _Batching


class GroupByKey(_Batching):
    """
    Groups events by given key (or key extractor function) into lists of up to max_events events. Each emitted list
    contained max_events events, unless timeout_secs seconds have passed since the first event in the group was
    received, at which the group is emitted with potentially fewer than max_events
    event.
    :param key: The key on which events are grouped by.
    :param max_events: Maximum number of events per emitted batch. Set to None to emit all events in one batch on flow termination.
    :param timeout_secs: Maximum number of seconds to wait before a batch is emitted.
    """

    def __init__(
        self,
        key: Optional[Union[str, callable]] = None,
        max_events: Optional[int] = None,
        timeout_secs: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(max_events, timeout_secs, True, key, **kwargs)

    async def _emit(self, batch, batch_time):
        event = Event(batch, time=batch_time)
        return await self._do_downstream(event)
