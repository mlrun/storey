from itertools import chain

from storey import Flow, Event
from storey.dtypes import _termination_obj


class Flatten(Flow):
    """
    Flattens sequences of sequences (i.e lists of lists, sets of sets, etc...) into a single sequence, by default
    casts sequence to list. When setting to_set=True, casts sequence to set.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def _do(self, event: Event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            for flattened in chain.from_iterable(event.body):
                await self._do_downstream(event.copy(body=flattened))
