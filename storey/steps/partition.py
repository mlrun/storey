from collections import namedtuple
from typing import Callable, Any

from storey import Flow
from storey.dtypes import _termination_obj

Partitioned = namedtuple("Partitioned", ["left", "right"], defaults=[None, None])


class Partition(Flow):
    """
    Partitions events by calling a predicate function on each event. Each processed event results in a `Partitioned`
    namedtuple of (left=Optional[Event], right=Optional[Event]).

    For a given event, if the predicate function results is `True`, the event is assigned to `left`. Otherwise, the
    event is assigned to `right`.

    :param predicate: A predicate function that results in a boolean.
    """

    def __init__(self, predicate: Callable[[Any], bool], **kwargs):
        super().__init__(**kwargs)
        self.predicate = predicate

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            if self.predicate(event):
                event.body = Partitioned(left=event.body, right=None)
                await self._do_downstream(event)
            else:
                event.body = Partitioned(left=None, right=event.body)
                await self._do_downstream(event)
