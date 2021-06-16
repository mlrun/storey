from storey import FlatMap


def Flatten(**kwargs):
    return FlatMap(lambda x: x, **kwargs)
