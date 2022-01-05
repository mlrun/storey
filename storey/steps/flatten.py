from storey import FlatMap


def Flatten(**kwargs):
    """Flatten is equivalent to FlatMap(lambda x: x)."""

    # Please note that Flatten forces full_event=False, since otherwise we can't iterate the body of the event
    if kwargs:
        kwargs["full_event"] = False
    return FlatMap(lambda x: x, **kwargs)
