from datetime import datetime, timedelta

from storey import build_flow, SyncEmitSource, Filter, Reduce
from storey.dtypes import EmissionType, SlidingWindow
from storey.windowed_store import EmitAfterMaxEvent, Window


def append_return(lst, x):
    lst.append(x)
    return lst


def validate_window(expected, window):
    for elem in window:
        key = elem[0]
        data = elem[1]
        for column in data.features:
            index = 0
            for bucket in data.features[column]:
                if len(bucket.data) > 0:
                    assert expected[key][index] == bucket.data

                index = index + 1


def to_millis(t):
    return t.timestamp() * 1000


def test_windowed_flow():
    controller = build_flow([
        SyncEmitSource(),
        Filter(lambda x: x['col1'] > 3),
        Window(SlidingWindow('30m', '5m'), EmitAfterMaxEvent(max_events=3, emission_type=EmissionType.Incremental)),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    base_time = datetime.now()

    for i in range(10):
        data = {'col1': i}
        controller.emit(data, f'{i % 2}', base_time + timedelta(minutes=i))

    controller.terminate()
    window_list = controller.await_termination()
    assert len(window_list) == 2

    expected_window_1 = {'0': {0: [(to_millis(base_time + timedelta(minutes=4)), 4)],
                               1: [(to_millis(base_time + timedelta(minutes=6)), 6)]},
                         '1': {0: [(to_millis(base_time + timedelta(minutes=5)), 5)]}}

    expected_window_2 = {'0': {1: [(to_millis(base_time + timedelta(minutes=8)), 8)]},
                         '1': {1: [(to_millis(base_time + timedelta(minutes=7)), 7),
                                   (to_millis(base_time + timedelta(minutes=9)), 9)]}}

    validate_window(expected_window_1, window_list[0])
    validate_window(expected_window_2, window_list[1])
