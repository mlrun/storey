# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from datetime import datetime, timedelta

from storey import Filter, Reduce, SyncEmitSource, build_flow
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
                    assert bucket.data == expected[key][index]

                index = index + 1


def to_millis(t):
    return t.timestamp() * 1000


def test_windowed_flow():
    controller = build_flow(
        [
            SyncEmitSource(),
            Filter(lambda x: x["col1"] > 3),
            Window(
                SlidingWindow("30m", "5m"),
                "time",
                EmitAfterMaxEvent(max_events=3, emission_type=EmissionType.Incremental),
            ),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    base_time = datetime.now()

    for i in range(10):
        data = {"col1": i, "time": base_time + timedelta(minutes=i)}
        controller.emit(data, f"{i % 2}")

    controller.terminate()
    window_list = controller.await_termination()
    assert len(window_list) == 2

    expected_window_1 = {
        "0": {
            0: [(to_millis(base_time + timedelta(minutes=4)), 4)],
            1: [(to_millis(base_time + timedelta(minutes=6)), 6)],
        },
        "1": {0: [(to_millis(base_time + timedelta(minutes=5)), 5)]},
    }

    expected_window_2 = {
        "0": {1: [(to_millis(base_time + timedelta(minutes=8)), 8)]},
        "1": {
            1: [
                (to_millis(base_time + timedelta(minutes=7)), 7),
                (to_millis(base_time + timedelta(minutes=9)), 9),
            ]
        },
    }

    validate_window(expected_window_1, window_list[0])
    validate_window(expected_window_2, window_list[1])
