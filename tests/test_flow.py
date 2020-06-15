from storey import build_flow, Source, Map, Filter, FlatMap, Reduce, FlowError


class TestException(Exception):
    pass


class RaiseEx:
    _counter = 0

    def __init__(self, raise_after):
        self._raise_after = raise_after

    def raise_ex(self, element):
        if self._counter == self._raise_after:
            raise TestException("test")
        self._counter += 1
        return element


def test_functional_flow():
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 3),
        FlatMap(lambda x: [x, x * 10]),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    for _ in range(100):
        for i in range(10):
            controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 3300 == termination_result


def test_error_flow():
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Map(RaiseEx(500).raise_ex),
        Reduce(0, lambda acc, x: acc + x),
    ]).run()

    try:
        for i in range(1000):
            controller.emit(i)
    except FlowError as flow_ex:
        assert isinstance(flow_ex.__cause__, TestException)


def test_broadcast():
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 3, termination_result_fn=lambda x, y: x + y),
        [
            Reduce(0, lambda acc, x: acc + x)
        ],
        [
            Reduce(0, lambda acc, x: acc + x)
        ]
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 6 == termination_result


def test_broadcast_complex():
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 3, termination_result_fn=lambda x, y: x + y),
        [
            Reduce(0, lambda acc, x: acc + x),
        ],
        [
            Map(lambda x: x * 100),
            Reduce(0, lambda acc, x: acc + x)
        ],
        [
            Map(lambda x: x * 1000),
            Reduce(0, lambda acc, x: acc + x)
        ]
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 3303 == termination_result


# Same as test_broadcast_complex but without using build_flow
def test_broadcast_complex_no_sugar():
    source = Source()
    filter = Filter(lambda x: x < 3, termination_result_fn=lambda x, y: x + y)
    source.to(Map(lambda x: x + 1)).to(filter)
    filter.to(Reduce(0, lambda acc, x: acc + x), )
    filter.to(Map(lambda x: x * 100)).to(Reduce(0, lambda acc, x: acc + x))
    filter.to(Map(lambda x: x * 1000)).to(Reduce(0, lambda acc, x: acc + x))
    controller = source.run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 3303 == termination_result
