from typing import Callable

from storey.dtypes import _termination_obj
from storey.flow import _UnaryFunctionFlow


class Assert(_UnaryFunctionFlow):
    """Asserts the boolean result of a custom function"""
    def __init__(self, fn: Callable, **kwargs):
        super().__init__(fn, **kwargs)

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            element = self._get_event_or_body(event)
            fn_result = await self._call(element)
            assert fn_result
            return event
