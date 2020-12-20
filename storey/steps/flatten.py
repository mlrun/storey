from itertools import chain
from storey import FlatMap


class Flatten(FlatMap):
    """Flattens sequences of sequences i.e lists of lists, sets of sets, etc."""

    def __init__(self, **kwargs):
        super().__init__(fn=lambda event_body: list(chain.from_iterable(event_body)), **kwargs)
