import unittest

from flow import *


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


class TestFlow(unittest.TestCase):
    def test_functional_flow(self):
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
        self.assertEqual(3300, termination_result)

    def test_error_flow(self):
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Map(RaiseEx(500).raise_ex),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        try:
            for i in range(1000):
                controller.emit(i)
        except FlowException as flow_ex:
            self.assertEqual(TestException, type(flow_ex.__cause__))

    def test_broadcast(self):
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            Broadcast(lambda x, y: x + y),
            Reduce(0, lambda acc, x: acc + x),
            Reduce(0, lambda acc, x: acc + x)
        ]).run()

        for i in range(10):
            controller.emit(i)
        controller.terminate()
        termination_result = controller.await_termination()
        self.assertEqual(6, termination_result)

    def test_broadcast_complex(self):
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            Broadcast(lambda x, y: x + y),
            Reduce(0, lambda acc, x: acc + x),
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
        self.assertEqual(3303, termination_result)


if __name__ == '__main__':
    unittest.main()
