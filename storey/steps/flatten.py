from itertools import chain

from storey import MapClass


class Flatten(MapClass):
    """
    Flattens sequences of sequences (i.e lists of lists, sets of sets, etc...) into a single sequence, by default
    casts sequence to list. When setting to_set=True, casts sequence to set.
    """

    def __init__(self, to_set: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.cast_to = set if to_set else list

    def do(self, event):
        return self.cast_to(chain.from_iterable(event))
