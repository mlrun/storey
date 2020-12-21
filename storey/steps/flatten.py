from itertools import chain

from storey import Flow, Event
from storey.dtypes import _termination_obj


class Flatten(Flow):
    """
    Flattens sequences of sequences (i.e lists of lists, sets of sets, etc...) into a single sequence, by default
    casts sequence to list. When setting to_set=True, casts sequence to set.
    """

    def __init__(self, to_set: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.to_set: bool = to_set

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            if self.to_set:
                flat_elements = set(chain.from_iterable(event.body))
            else:
                flat_elements = chain.from_iterable(event.body)

            for flattened in flat_elements:
                await self._do_downstream(
                    Event(
                        body=flattened,
                        key=event.key,
                        time=event.time,
                        id=event.id
                    )
                )
