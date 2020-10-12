from storey import Source, Map, Reduce, build_flow, Complete


def test_simple_flow_zero_events(benchmark):
    def inner():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        controller.terminate()
        termination_result = controller.await_termination()
        assert termination_result == 0

    benchmark(inner)


def test_simple_flow_one_event(benchmark):
    def inner():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        controller.emit(0)
        controller.terminate()
        termination_result = controller.await_termination()
        assert termination_result == 1

    benchmark(inner)


def test_complete_flow_one_event(benchmark):
    def inner():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Complete()
        ]).run()

        result = controller.emit(0, return_awaitable_result=True).await_result()
        assert result == 1
        controller.terminate()
        controller.await_termination()

    benchmark(inner)


def test_simple_flow_1000_events(benchmark):
    def inner():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        for i in range(1000):
            controller.emit(i)
        controller.terminate()
        termination_result = controller.await_termination()
        assert termination_result == 500500

    benchmark(inner)
