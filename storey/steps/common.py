from typing import List, Optional, Dict, Any, Set, Union

from storey import FlatMap, Map, Filter
from storey.flow import _UnaryFunctionFlow


class Flatten(FlatMap):
    """
    Flattens sequences of sequences i.e lists of lists, sets of sets, etc... Be advised,
    """
    def __init__(self, **kwargs):
        super().__init__(fn=lambda event: event, **kwargs)


class ForEach(_UnaryFunctionFlow):
    async def _do_internal(self, element, fn_result):
        self._user_fn_output_to_event(element, fn_result)
        await self._do_downstream(element)


class FilterKeys(Map):
    def __init__(
        self,
        keys: List[str],
        defaults: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        super().__init__(fn=self._select_keys, **kwargs)
        keys = set(keys)
        for key in keys:
            if not isinstance(key, str):
                raise TypeError(f"Expected a str key name, got {type(key)}")

        if defaults is not None:
            for default in defaults.keys():
                if default not in keys:
                    raise KeyError(f"Key {default} not found in fields")

        self.keys: Set[str] = keys
        self.defaults: Dict[str, Any] = defaults or {}

    def _select_keys(self, event: Union[Dict, List, Set]) -> Union[Dict, List]:
        if isinstance(event, (list, set)):
            return list(map(self._select, event))
        else:
            return self._select(event)

    def _select(self, event):
        filtered_event = {}
        for key in self.keys:
            value = event.get(key)
            if value is None:
                value = self.defaults.get(key)
            filtered_event[key] = value
        return filtered_event


class Sample(Filter):
    def __init__(self, sample_rate: int, **kwargs):
        super().__init__(self.sample_by_rate, **kwargs)
        if sample_rate <= 0:
            raise ValueError(f"Expected sample_rate > 0, got {sample_rate}")

        self.sample_rate = sample_rate
        self.count = 0

    def sample_by_rate(self, event):
        return self.count % self.sample_rate == 0
