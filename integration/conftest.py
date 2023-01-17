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
import asyncio
import os
from datetime import datetime

import fakeredis
import pytest

from integration.integration_test_utils import (
    V3ioHeaders,
    _generate_table_name,
    create_temp_kv,
    create_temp_redis_kv,
    drivers_list,
    get_redis_client,
    recursive_delete,
    remove_redis_table,
    remove_sql_tables,
)
from storey import V3ioDriver
from storey.redis_driver import RedisDriver
from storey.sql_driver import SQLDriver

SQLITE_DB = "sqlite:///test.db"


@pytest.fixture(params=drivers_list)
def setup_teardown_test(request):
    # Setup
    if request.param == "SQLDriver" and request.fspath.basename != "test_flow_integration.py":
        pytest.skip("SQLDriver test only in test_flow_integration")
    test_context = ContextForTests(request.param, table_name=_generate_table_name())

    # Test runs
    yield test_context

    # Teardown
    if test_context.driver_name == "V3ioDriver":
        asyncio.run(recursive_delete(test_context.table_name, V3ioHeaders()))
    elif test_context.driver_name == "RedisDriver":
        remove_redis_table(test_context.table_name)
    elif test_context.driver_name == "SQLDriver":
        remove_sql_tables()
    else:
        raise ValueError(f'Unsupported driver name "{test_context.driver_name}"')


@pytest.fixture(params=drivers_list)
def setup_kv_teardown_test(request):
    # Setup
    test_context = ContextForTests(request.param, table_name=_generate_table_name())

    if test_context.driver_name == "V3ioDriver":
        asyncio.run(create_temp_kv(test_context.table_name))
    elif test_context.driver_name == "RedisDriver":
        create_temp_redis_kv(test_context)
    elif test_context.driver_name == "SQLDriver":
        pytest.skip(msg="test not relevant for SQLDriver")
    else:
        raise ValueError(f'Unsupported driver name "{test_context.driver_name}"')

    # Test runs
    yield test_context

    # Teardown
    if test_context.driver_name == "V3ioDriver":
        asyncio.run(recursive_delete(test_context.table_name, V3ioHeaders()))
    elif test_context.driver_name == "RedisDriver":
        remove_redis_table(test_context.table_name)
    else:
        raise ValueError(f'Unsupported driver name "{test_context.driver_name}"')


@pytest.fixture()
def assign_stream_teardown_test():
    # Setup
    stream_path = _generate_table_name("bigdata/storey_ci/stream_test")

    # Test runs
    yield stream_path

    # Teardown
    asyncio.run(recursive_delete(stream_path, V3ioHeaders()))


# Can't call it TestContext because then pytest tries to run it as if it were a test suite
class ContextForTests:
    def __init__(self, driver_name: str, table_name: str):
        self._driver_name = driver_name
        self._table_name = table_name
        # sqlite cant save time zone
        self.test_base_time = (
            datetime.fromisoformat("2020-07-21T21:40:00+00:00")
            if driver_name != "SQLDriver"
            else datetime.fromisoformat("2020-07-21T21:40:00")
        )

        self._redis_fake_server = None
        if driver_name == "RedisDriver":
            redis_url = os.environ.get("MLRUN_REDIS_URL")
            if not redis_url:
                # if we are using fakeredis, create fake-server to support tests involving multiple clients
                self._redis_fake_server = fakeredis.FakeServer()
        if driver_name == "SQLDriver":
            self._sql_db_path = SQLITE_DB
            self._sql_table_name = table_name.split("/")[-2]
            self._table_name = f"{SQLITE_DB}/{self._sql_table_name}"

    @property
    def table_name(self):
        return self._table_name

    @property
    def redis_fake_server(self):
        return self._redis_fake_server

    @property
    def driver_name(self):
        return self._driver_name

    @property
    def sql_db_path(self):
        return self._sql_db_path

    class AggregationlessV3ioDriver(V3ioDriver):
        def supports_aggregations(self):
            return False

    class AggregationlessRedisDriver(RedisDriver):
        def supports_aggregations(self):
            return False

    def driver(self, *args, primary_key=None, is_aggregationless_driver=False, time_fields=None, **kwargs):
        if self.driver_name == "V3ioDriver":
            v3io_driver_class = ContextForTests.AggregationlessV3ioDriver if is_aggregationless_driver else V3ioDriver
            return v3io_driver_class(*args, **kwargs)
        elif self.driver_name == "RedisDriver":
            redis_driver_class = (
                ContextForTests.AggregationlessRedisDriver if is_aggregationless_driver else RedisDriver
            )
            return redis_driver_class(
                *args,
                redis_client=get_redis_client(self.redis_fake_server),
                key_prefix="storey-test:",
                **kwargs,
            )
        elif self.driver_name == "SQLDriver":
            if is_aggregationless_driver:
                sql_driver_class = SQLDriver
                return sql_driver_class(db_path=SQLITE_DB, primary_key=primary_key, time_fields=time_fields)
            else:
                pytest.skip("SQLDriver does not support aggregation")
        else:
            driver_name = self.driver_name
            raise ValueError(f'Unsupported driver name "{driver_name}"')
