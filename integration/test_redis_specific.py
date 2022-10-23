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
import pytest

from storey import (Complete, JoinWithTable, NoSqlTarget, Reduce,
                    SyncEmitSource, Table, build_flow)
from storey.redis_driver import RedisDriver

from .integration_test_utils import append_return, get_redis_client


@pytest.fixture()
def redis():
    return get_redis_client()


def test_redis_driver_write(redis):
    driver = RedisDriver(redis)
    controller = build_flow(
        [SyncEmitSource(), NoSqlTarget(Table("test", driver)), Complete()]
    ).run()
    controller.emit({"col1": 0}, "key").await_result()
    controller.terminate()
    controller.await_termination()

    data = driver.redis.hgetall("storey:test/key:static")
    data_strings = {}
    for key, val in data.items():
        if isinstance(key, bytes):
            data_strings[key.decode("utf-8")] = val.decode("utf-8")
        else:
            data_strings[key] = val

    assert data_strings == {"col1": "0"}


def test_redis_driver_join(redis):
    driver = RedisDriver(redis)
    table = Table("test", driver)

    # Create the data we'll join with in Redis.
    driver.redis.hset("storey:test/2:static", mapping={"name": "1234"})
    controller = build_flow(
        [
            SyncEmitSource(),
            JoinWithTable(table, lambda x: x["col2"]),
            Reduce([], lambda acc, x: append_return(acc, x)),
        ]
    ).run()

    controller.emit({"col1": 1, "col2": "2"}, "key")
    controller.emit({"col1": 1, "col2": "2"}, "key")
    controller.terminate()
    termination_result = controller.await_termination()

    expected_result = [
        {"col1": 1, "col2": "2", "name": 1234},
        {"col1": 1, "col2": "2", "name": 1234},
    ]

    assert termination_result == expected_result
