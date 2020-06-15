import time
from datetime import timedelta, datetime

from storey import build_flow, Source, Map
from storey.dtypes import FixedWindow
from storey.windowed_store import Window, EmitAfterMaxEvent


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
