from datetime import datetime, timedelta

from flow import *
from windowed_store import *


def test_normal_flow():
    flow = build_flow([
        Source(),
        Map(lambda x: x + 1),
        JoinWithTable(lambda x: x, lambda x, y: y['secret'], '/bigdata/gal'),
        Map(aprint)
    ])

    start = time.monotonic()

    mat = flow.run()
    for outer in range(100):
        for i in range(10):
            mat.emit(i)
    mat.emit(None)

    end = time.monotonic()
    print(end - start)


async def aprint_store(store):
    print('store: ')
    for elem in store:
        print(elem[0], '-', elem[1].features, f'start time - {elem[1].first_bucket_start_time}')
    print()


def test_windowed_flow():
    flow = build_flow([
        Source(),
        Window(FixedWindow('1h'), 'key', 'time', EmitAfterMaxEvent(10)),
        Map(aprint_store)
    ])

    start = time.monotonic()
    running_flow = flow.run()
    for i in range(32):
        data = {'key': f'{i % 4}', 'time': datetime.now() + timedelta(minutes=i), 'col1': i, 'other_col': i * 2}
        running_flow.emit(data)

    end = time.monotonic()
    print(end - start)

    running_flow.terminate()
