from conftest import has_v3io_creds
from storey import Filter, JoinWithTable, Map, Source, build_flow

import pytest


@pytest.mark.skipif(not has_v3io_creds, reason='missing v3io credentials')
def test_functional_flow():
    flow = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 3),
        JoinWithTable(lambda x: x, lambda x, y: y['secret'], '/bigdata/gal'),
        Map(lambda x: print(x))
    ])
    mat = flow.run()
    for _ in range(100):
        for i in range(10):
            mat.emit(i)
    mat.terminate()
    mat.await_termination()
