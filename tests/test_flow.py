import storey


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
    controller = storey.build_flow([
        storey.Source(),
        storey.Map(lambda x: x + 1),
        storey.Filter(lambda x: x < 3),
        storey.FlatMap(lambda x: [x, x * 10]),
        storey.Reduce(0, lambda acc, x: acc + x),
    ]).run()

    for _ in range(100):
        for i in range(10):
            controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 3300 == termination_result


def test_error_flow():
    controller = storey.build_flow([
        storey.Source(),
        storey.Map(lambda x: x + 1),
        storey.Map(RaiseEx(500).raise_ex),
        storey.Reduce(0, lambda acc, x: acc + x),
    ]).run()

    try:
        for i in range(1000):
            controller.emit(i)
    except storey.FlowError as flow_ex:
        assert isinstance(flow_ex.__cause__, TestException)


def test_broadcast():
    controller = storey.build_flow([
        storey.Source(),
        storey.Map(lambda x: x + 1),
        storey.Filter(
            lambda x: x < 3, termination_result_fn=lambda x, y: x + y),
        [
            storey.Reduce(0, lambda acc, x: acc + x)
        ],
        [
            storey.Reduce(0, lambda acc, x: acc + x)
        ]
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 6 == termination_result


def test_broadcast_complex():
    controller = storey.build_flow([
        storey.Source(),
        storey.Map(lambda x: x + 1),
        storey.Filter(
            lambda x: x < 3, termination_result_fn=lambda x, y: x + y),
        [
            storey.Reduce(0, lambda acc, x: acc + x),
        ],
        [
            storey.Map(lambda x: x * 100),
            storey.Reduce(0, lambda acc, x: acc + x)
        ],
        [
            storey.Map(lambda x: x * 1000),
            storey.Reduce(0, lambda acc, x: acc + x)
        ]
    ]).run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 3303 == termination_result


# Same as test_broadcast_complex but without using build_flow
def test_broadcast_complex_no_sugar():
    source = storey.Source()
    filter = storey.Filter(
        lambda x: x < 3, termination_result_fn=lambda x, y: x + y)
    source.to(storey.Map(lambda x: x + 1)).to(filter)
    filter.to(storey.Reduce(0, lambda acc, x: acc + x), )
    filter.to(storey.Map(lambda x: x * 100)).to(storey.Reduce(0, lambda acc, x: acc + x))
    filter.to(storey.Map(lambda x: x * 1000)).to(storey.Reduce(0, lambda acc, x: acc + x))
    controller = source.run()

    for i in range(10):
        controller.emit(i)
    controller.terminate()
    termination_result = controller.await_termination()
    assert 3303 == termination_result
