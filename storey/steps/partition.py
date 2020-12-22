from collections import namedtuple
from typing import Callable, Any

from storey import Flow, Event
from storey.dtypes import _termination_obj

Partitioned = namedtuple("Partitioned", ["left", "right"], defaults=[None, None])


class Partition(Flow):
    """
    Partitions events by calling a predicate function on each event. Each processed event results in a `Partitioned`
    namedtuple of either (left=Event, right=None) or (left=None, right=Event).
    """

    def __init__(self, predicate: Callable[[Any], bool], **kwargs):
        super().__init__(**kwargs)
        self.predicate = predicate

    async def _do(self, event: Event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            if self.predicate(event):
                event.body = Partitioned(left=event.body, right=None)
                await self._do_downstream(event)
            else:
                event.body = Partitioned(left=None, right=event.body)
                await self._do_downstream(event)
