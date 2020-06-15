import storey


def test_functional_flow():
    flow = storey.build_flow([
        storey.Source(),
        storey.Map(lambda x: x + 1),
        storey.Filter(lambda x: x < 3),
        storey.JoinWithTable(lambda x: x, lambda x, y: y['secret'], '/bigdata/gal'),
        storey.Map(lambda x: print(x))
    ])
    mat = flow.run()
    for _ in range(100):
        for i in range(10):
            mat.emit(i)
    mat.terminate()
    mat.await_termination()
