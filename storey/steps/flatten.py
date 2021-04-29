from storey import Flow
from storey.dtypes import _termination_obj


class Flatten(Flow):
    """
    Splits an event with an iterable body into multiple events.
    """

    def __init__(self, **kwargs):
        super().__init__(full_event=True, **kwargs)

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            for element in event.body:
                await self._do_downstream(event.copy(body=element))
