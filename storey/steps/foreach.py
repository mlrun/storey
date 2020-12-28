from storey.flow import _UnaryFunctionFlow


class ForEach(_UnaryFunctionFlow):
    """Applies given function on each event in the stream, passes original event downstream."""

    async def _do_internal(self, element, fn_result):
        self._user_fn_output_to_event(element, fn_result)
        await self._do_downstream(element)
