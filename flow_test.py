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
        source = Source()

        build_flow([
            source,
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            FlatMap(lambda x: [x, x * 10]),
            Reduce(0, lambda acc, x: acc + x),
        ])
        mat = source.run()
        for _ in range(100):
            for i in range(10):
                mat.emit(i)
        mat.terminate()
        materialized_result = mat.await_termination()
        self.assertEqual(3300, materialized_result)

    def test_error_flow(self):
        source = Source()

        build_flow([
            source,
            Map(lambda x: x + 1),
            Map(RaiseEx(500).raise_ex),
            Reduce(0, lambda acc, x: acc + x),
        ])
        mat = source.run()
        try:
            for i in range(1000):
                mat.emit(i)
        except FlowException as flow_ex:
            self.assertEqual(TestException, type(flow_ex.__cause__))

    def test_broadcast(self):
        source = Source()
        broadcast = Broadcast(lambda x, y: x + y)

        build_flow([
            source,
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            broadcast
        ])

        broadcast.to(Reduce(0, lambda acc, x: acc + x))
        broadcast.to(Reduce(0, lambda acc, x: acc + x))

        mat = source.run()
        for i in range(10):
            mat.emit(i)
        mat.terminate()
        materialized_result = mat.await_termination()
        self.assertEqual(6, materialized_result)

    def test_broadcast_complex(self):
        source = Source()
        broadcast = Broadcast(lambda x, y: x + y)

        build_flow([
            source,
            Map(lambda x: x + 1),
            Filter(lambda x: x < 3),
            broadcast,
            Reduce(0, lambda acc, x: acc + x)
        ])

        build_flow([
            broadcast,
            Map(lambda x: x * 100),
            Reduce(0, lambda acc, x: acc + x)
        ])

        mat = source.run()
        for i in range(10):
            mat.emit(i)
        mat.terminate()
        materialized_result = mat.await_termination()
        self.assertEqual(303, materialized_result)


if __name__ == '__main__':
    unittest.main()
