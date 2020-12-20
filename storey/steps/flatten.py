from itertools import chain

from storey import MapClass


class Flatten(MapClass):
    """Flattens sequences of sequences (i.e lists of lists, sets of sets, etc...) into a single list or set"""

    def __init__(self, to_set: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.to_set = to_set

    def do(self, event):
        type_cast = set if self.to_set else list
        return type_cast(chain.from_iterable(event))
