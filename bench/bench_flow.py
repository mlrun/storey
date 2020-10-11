from timeit import timeit

from storey import Source, Map, Reduce, build_flow, Complete


class Bench:
    @staticmethod
    def simple_flow_zero_events():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        controller.terminate()
        termination_result = controller.await_termination()
        assert termination_result == 0

    @staticmethod
    def simple_flow_one_event():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Reduce(0, lambda acc, x: acc + x),
        ]).run()

        controller.emit(0)
        controller.terminate()
        termination_result = controller.await_termination()
        assert termination_result == 1

    @staticmethod
    def complete_flow_one_event():
        controller = build_flow([
            Source(),
            Map(lambda x: x + 1),
            Complete()
        ]).run()

        result = controller.emit(0, return_awaitable_result=True).await_result()
        assert result == 1
        controller.terminate()
        controller.await_termination()

    @staticmethod
    def simple_flow_1000_events(iterations=5):
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


default_iterations = 500
ms_per_sec = 1000

for method_name in dir(Bench):
    if method_name.startswith('__'):
        continue
    method = getattr(Bench, method_name)
    num_iterations = default_iterations
    if method.__defaults__:  # Override num iterations
        num_iterations = method.__defaults__[0]
    runtime = timeit(method, number=num_iterations) / num_iterations * ms_per_sec
    print(f'{method_name}: {runtime:.3f} ms (avg of {num_iterations} runs)')
