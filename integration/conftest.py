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

import pytest

from integration.integration_test_utils import (
    TestContext,
    V3ioHeaders,
    _generate_table_name,
    create_temp_kv,
    create_temp_redis_kv,
    drivers_list,
    recursive_delete,
    remove_redis_table,
    remove_sql_tables,
)


@pytest.fixture(params=drivers_list)
def setup_teardown_test(request):
    # Setup
    if request.param == "SQLDriver" and request.fspath.basename != "test_flow_integration.py":
        pytest.skip("SQLDriver test only in test_flow_integration")
    test_context = TestContext(request.param, table_name=_generate_table_name())

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
    test_context = TestContext(request.param, table_name=_generate_table_name())

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
