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
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List, Union, Optional
import pandas as pd
import asyncio
from functools import partial
from .drivers import Driver

import redis
import redis.cluster
from .dtypes import RedisError
from .utils import schema_file_name


class RedisType(Enum):
    STANDALONE = 1
    CLUSTER = 2
    UNKNOWN = 3


class NeedsRedisAccess:
    """
    Checks that params for access to Redis exist and are legal

    :param redis_utl: URL to the redis server. If not set, the MLRUN_REDIS_URL environment variable will be used.
    """

    def __init__(self, redis_url=None):
        redis_url = redis_url or os.getenv('MLRUN_REDIS_URL')
        if not redis_url:
            raise ValueError('no redis_client object or MLRUN_REDIS_URL environment-var provided, aborting')

        self._redis_url = redis_url


class RedisDriver(NeedsRedisAccess, Driver):
    REDIS_TIMEOUT = 5  # Seconds
    REDIS_WATCH_INTERVAL = 1  # Seconds
    DATETIME_FIELD_PREFIX = "_dt:"
    TIMEDELTA_FIELD_PREFIX = "_td:"
    DEFAULT_KEY_PREFIX = "storey:"

    def __init__(self,
                 redis_client: Optional[Union[redis.Redis, redis.cluster.RedisCluster]] = None,
                 redis_type: RedisType = RedisType.UNKNOWN,
                 key_prefix: str = None,
                 redis_url: Optional[str] = None,
                 aggregation_attribute_prefix: str = 'aggr_',
                 aggregation_time_attribute_prefix: str = '_',
                 use_parallel_operations: bool = True,  # unused
                 ):
        # if client provided a redis-client object, use it. otherwise store the redis url, and create redis-client
        # upon demand (any access to self.redis)
        if redis_client is not None:
            self._redis = redis_client
        else:
            NeedsRedisAccess.__init__(self, redis_url)
            self._redis = None

        if not isinstance(redis_type, RedisType):
            raise ValueError(f'unsupported RedisType value provided  ("{redis_type}"), aborting')

        self._key_prefix = key_prefix if key_prefix is not None else self.DEFAULT_KEY_PREFIX
        self._type = redis_type
        self._mtime_name = '$_mtime_'

        self._aggregation_attribute_prefix = aggregation_attribute_prefix
        self._aggregation_time_attribute_prefix = aggregation_time_attribute_prefix
        self._aggregation_prefixes = (self._aggregation_attribute_prefix,
                                      self._aggregation_time_attribute_prefix)

    @staticmethod
    def asyncify(fn):
        """Run a synchronous function asynchronously."""
        async def inner_fn(*args, **kwargs):
            loop = asyncio.get_event_loop()
            partial_fn = partial(fn, *args, **kwargs)
            return await loop.run_in_executor(None, partial_fn)
        return inner_fn

    @property
    def redis(self):
        if self._redis is None:
            if self._type is RedisType.UNKNOWN:
                try:
                    self._redis = redis.cluster.RedisCluster.from_url(self._redis_url, decode_response=True)
                except redis.exceptions.RedisClusterException as e:
                    if not e.args[0].endswith('Cluster mode is not enabled on this node'):
                        raise e
                self._redis = redis.Redis.from_url(self._redis_url, decode_responses=True)
            elif self._type is RedisType.STANDALONE:
                self._redis = redis.Redis.from_url(self._redis_url, decode_responses=True)
            else:
                self._redis = redis.cluster.RedisCluster.from_url(self._redis_url, decode_response=True)
        return self._redis

    async def close(self):
        if self._redis:
            self._redis.close()

    @staticmethod
    def make_key(key_prefix, *parts):
        return f"{key_prefix}{''.join([str(p) for p in parts])}"

    def _make_key(self, *parts):
        return RedisDriver.make_key(self._key_prefix, *parts)

    @staticmethod
    def _static_data_key(redis_key_prefix):
        """
        The Redis key for the Hash containing static data (AKA, "additional data").

        The `redis_key_prefix` string should be the Redis key prefix used to store
        data for a Storey container, table path, and key combination.
        """
        return f"{redis_key_prefix}:static"

    def _aggregation_time_key(self, redis_key_prefix, feature_name):
        """
        The Redis key containing the associated timestamp for an aggregation.

        The `redis_key_prefix` string should be the Redis key prefix used to store
        data for a Storey container, table path, and key combination.
        """
        return f"{redis_key_prefix}:{self._aggregation_time_attribute_prefix}{feature_name}"

    @staticmethod
    def _list_key(redis_key_prefix, list_attribute_name):
        """
        The key containing a list of aggregation data.

        The `redis_key_prefix` string should be the Redis key prefix used to store
        data for a Storey container, table path, and key combination.
        """
        return f"{redis_key_prefix}:{list_attribute_name}"

    @classmethod
    def _convert_python_obj_to_redis_value(cls, value):
        if isinstance(value, datetime):
            if pd.isnull(value):
                return None
            return f'{cls.DATETIME_FIELD_PREFIX}{value.timestamp()}'
        elif isinstance(value, timedelta):
            return f'{cls.TIMEDELTA_FIELD_PREFIX}{value.total_seconds()}'
        else:
            if isinstance(value, float):
                if value == math.inf or value == -math.inf:
                    value = str(value)  # stored in redis as "inf" string
                elif math.isnan(value):
                    value = str(value)  # stored in redis as "nan" string
                elif value % 1 == 0:
                    value = int(value)  # cast whole numbers to integer

            return json.dumps(value)

    @classmethod
    def _convert_python_obj_to_lua_value(cls, value):
        if isinstance(value, datetime):
            if pd.isnull(value):
                return None
            return f'"{cls.DATETIME_FIELD_PREFIX}{value.timestamp()}"'
        elif isinstance(value, timedelta):
            return f'"{cls.TIMEDELTA_FIELD_PREFIX}{value.total_seconds()}"'
        elif isinstance(value, bool):
            return f'"{json.dumps(value)}"'
        else:
            if isinstance(value, float):
                if value == math.inf:
                    value = "math.huge"
                elif value == -math.inf:
                    value = "-math.huge"
                elif math.isnan(value):
                    value = "0/0"
                elif value % 1 == 0:
                    # Store whole numbers as integers to save space.
                    value = int(value)

            return json.dumps(value)

    @classmethod
    def convert_redis_value_to_python_obj(cls, value):
        if value is None:
            return None

        str_value = cls.convert_to_str(value)

        value = str_value.lower()
        if value.startswith(cls.TIMEDELTA_FIELD_PREFIX):
            ret = timedelta(seconds=float(value.split(":")[1]))
        elif value.startswith(cls.DATETIME_FIELD_PREFIX):
            ret = datetime.fromtimestamp(float(value.split(":")[1]), tz=timezone.utc)
        elif value == "inf":
            ret = math.inf
        elif value == "-inf":
            ret = -math.inf
        elif value == "nan" or value == "-nan":
            ret = float("nan")
        elif value == "true":
            ret = True
        elif value == "false":
            ret = False
        else:
            # if value is a number, convert type to int / float
            no_sign_value = value[1:] if value[0] in ('-', '+') else value
            if no_sign_value.replace('.', '', 1).isdigit():
                ret = int(value) if int(float(value)) == float(value) else float(value)
            else:
                ret = str_value
        return ret

    @classmethod
    def convert_to_str(cls, key):
        if isinstance(key, bytes):
            return key.decode('utf-8')
        else:
            return key

    def _discard_old_pending_items(self, pending, max_window_millis):
        res = {}
        if pending:
            last_time = int(list(pending.keys())[-1] / max_window_millis) * max_window_millis
            min_time = last_time - max_window_millis
            for _time, value in pending.items():
                if _time > min_time:
                    res[_time] = value
        return res

    def _build_feature_store_lua_update_script(self, redis_key_prefix, aggregation_element, partitioned_by_key, additional_data):
        redis_keys_involved = []
        pending_updates = {}
        condition_expression = None
        additional_data_lua_script = ""

        # Static attributes, like "name," "age," -- everything that isn't an agg.
        if additional_data:
            redis_keys_involved.append(self._static_data_key(redis_key_prefix))
            additional_data_lua_script = f'local additional_data_key="{self._static_data_key(redis_key_prefix)}";\n'
            for name, value in additional_data.items():
                expression_value = self._convert_python_obj_to_lua_value(value)
                # NOTE: This logic assumes that static attributes we're supposed
                # to delete will appear in the `additional_data` dict with a
                # "falsey" value. This is the same logic the V3ioDriver uses.
                if expression_value:
                    additional_data_lua_script = (
                        f'{additional_data_lua_script}redis.call("HSET",additional_data_key, "{name}", {expression_value});\n')
                else:
                    additional_data_lua_script = f'{additional_data_lua_script}redis.call("HDEL",additional_data_key, "{name}");\n'

        lua_script = additional_data_lua_script

        if aggregation_element:
            times_updates = {}
            new_cached_times = {}
            initialized_attributes = {}
            if partitioned_by_key:
                condition_expression = aggregation_element.storage_specific_cache.get(self._mtime_name, None)
            lua_tonum_function = ('local function tonum(str) if str == "inf" then return math.huge elseif str == "-inf" then '
                                  'return -math.huge elseif str == "nan" then return 0/0 end return tonumber(str) end')
            lua_script = f'{lua_script}local len;local redis_key_prefix="{redis_key_prefix}";local list_attribute_key;\n'
            lua_script = f'{lua_script}{lua_tonum_function}\n'
            for name, bucket in aggregation_element.aggregation_buckets.items():
                # Only save raw aggregates, not virtual
                if bucket.should_persist:
                    # In case we have pending data that spreads over more than 2 windows, discard the old ones.
                    pending_updates[name] = self._discard_old_pending_items(bucket.get_and_flush_pending(),
                                                                            bucket.max_window_millis)
                    for bucket_start_time, aggregation_values in pending_updates[name].items():
                        # the relevant attribute out of the 2 feature attributes
                        feature_attr = 'a' if int(bucket_start_time / bucket.max_window_millis) % 2 == 0 else 'b'

                        aggr_time_attribute_name = f'{bucket.name}_{feature_attr}'
                        array_time_attribute_key = self._aggregation_time_key(redis_key_prefix, aggr_time_attribute_name)

                        cached_time = bucket.storage_specific_cache.get(array_time_attribute_key, 0)

                        expected_time = int(bucket_start_time / bucket.max_window_millis) * bucket.max_window_millis
                        expected_time_expr = self._convert_python_obj_to_lua_value(
                            datetime.fromtimestamp(expected_time / 1000))
                        index_to_update = int((bucket_start_time - expected_time) / bucket.period_millis)

                        for aggregation, aggregation_value in aggregation_values.items():
                            list_attribute_name = f'{self._aggregation_attribute_prefix}{name}_{aggregation}_{feature_attr}'
                            list_attribute_key = self._list_key(redis_key_prefix, list_attribute_name)
                            if list_attribute_key not in redis_keys_involved:
                                redis_keys_involved.append(list_attribute_key)
                            lua_script = f'{lua_script}list_attribute_key="{list_attribute_key}";\n'

                            if cached_time < expected_time:
                                if not initialized_attributes.get(list_attribute_key, 0) == expected_time:
                                    initialized_attributes[list_attribute_key] = expected_time
                                    lua_script = (
                                        f'{lua_script}local t=redis.call("GET","{array_time_attribute_key}");\n'
                                        f'if (type(t)~="boolean" and (tonumber(t) < {expected_time})) then '
                                        f'redis.call("DEL",list_attribute_key); end;\n')
                                    default_value = self._convert_python_obj_to_redis_value(aggregation_value.default_value)
                                    lua_script = (
                                        f'{lua_script}len=redis.call("LLEN",list_attribute_key);\n'
                                        f'for i=1,({bucket.total_number_of_buckets}-len) do '
                                        f'redis.call("RPUSH",list_attribute_key,{default_value}) end;\n')
                                if array_time_attribute_key not in times_updates:
                                    times_updates[array_time_attribute_key] = expected_time_expr
                                new_cached_times[name] = (array_time_attribute_key, expected_time)

                            # Updating the specific cells
                            if cached_time <= expected_time:
                                lua_script = f'{lua_script}local old_value=redis.call("LINDEX",list_attribute_key,{index_to_update});\n'
                                lua_script = f'{lua_script}old_value=tonum(old_value)\n'
                                new_value_expression = aggregation_value.aggregate_lua_script(
                                    'old_value', aggregation_value.value)
                                lua_script = (
                                    f'{lua_script}redis.call("LSET", list_attribute_key, {index_to_update}, {new_value_expression});\n'
                                )

                        redis_keys_involved.append(array_time_attribute_key)
                        lua_script = f'{lua_script}redis.call("SET","{array_time_attribute_key}",{expected_time}); \n'

        return lua_script, condition_expression, pending_updates, redis_keys_involved

    async def _save_key(self, container, table_path, key, aggr_item, partitioned_by_key, additional_data):
        redis_key_prefix = self._make_key(container, table_path, key)
        update_expression, mtime_condition, pending_updates, redis_keys_involved = self._build_feature_store_lua_update_script(
            redis_key_prefix, aggr_item, partitioned_by_key, additional_data)
        if not update_expression:
            return
        current_time = int(time.time_ns() / 1000)
        if mtime_condition is not None:
            update_expression = (
                f'if redis.call("HGET", "{redis_key_prefix}","{self._mtime_name}") == "{mtime_condition}" then\n'
                f'{update_expression}redis.call("HSET","{redis_key_prefix}","{self._mtime_name}",{current_time});\n'
                'return 1;else return 0;end;')
        else:
            update_expression = f'{update_expression}redis.call("HSET","{redis_key_prefix}","{self._mtime_name}",{current_time});return 1;'

        redis_keys_involved.append(redis_key_prefix)
        update_ok = await self.asyncify(self.redis.eval)(update_expression, len(redis_keys_involved), *redis_keys_involved)

        if update_ok:
            if aggr_item:
                aggr_item.storage_specific_cache[self._mtime_name] = current_time
        # In case Mtime condition evaluated to False, we run the conditioned
        # expression, then fetch and cache the latest key's state
        else:
            update_expression, _, _, redis_keys_involved = self._build_feature_store_lua_update_script(
                redis_key_prefix, aggr_item, False, additional_data)
            update_expression = f'{update_expression}redis.call("HSET","{redis_key_prefix}","{self._mtime_name}",{current_time});return 1;'
            redis_keys_involved.append(redis_key_prefix)
            update_ok = await RedisDriver.asyncify(self.redis.eval)(update_expression, len(redis_keys_involved), *redis_keys_involved)
            if update_ok and aggr_item:
                await self._fetch_state_by_key(aggr_item, container, table_path, key)

    async def _get_all_fields(self, redis_key: str):
        try:
            # TODO: This should be HSCAN, not HGETALL, to avoid blocking Redis
            # with very large hashes.
            values = await RedisDriver.asyncify(self.redis.hgetall)(redis_key)
        except redis.ResponseError as e:
            raise RedisError(f'Failed to get key {redis_key}. Response error was: {e}')
        res = {RedisDriver.convert_to_str(key): RedisDriver.convert_redis_value_to_python_obj(val) for key, val in values.items()
               if not str(key).startswith(self._aggregation_prefixes)}
        return res

    async def _get_specific_fields(self, redis_key: str, attributes: List[str]):
        non_aggregation_attrs = [
            name for name in attributes
            if not name.startswith(self._aggregation_prefixes)
        ]
        try:
            values = await RedisDriver.asyncify(self.redis.hmget)(redis_key, non_aggregation_attrs)
        except redis.ResponseError as e:
            raise RedisError(f'Failed to get key {redis_key}. Response error was: {e}') from e
        if len(values) == 1:
            if values == [None]:
                return {}
            return {non_aggregation_attrs[0]: RedisDriver.convert_redis_value_to_python_obj(values[0])}
        else:
            return {RedisDriver.convert_to_str(k): RedisDriver.convert_redis_value_to_python_obj(v) for k, v in values.items()}

    async def _load_by_key(self, container, table_path, key, attributes):
        """
        Return all static attributes, or certain attributes.

        NOTE: Following the V3IO driver's implementation, this method will not
        return aggregation attributes or associated time values -- just static
        data, AKA "additional data."
        """
        redis_key_prefix = self._make_key(container, table_path, key)
        static_key = self._static_data_key(redis_key_prefix)
        if attributes == "*":
            values = await self._get_all_fields(static_key)
        else:
            values = await self._get_specific_fields(static_key, attributes)
        return values

    async def _get_associated_time_attr(self, redis_key_prefix, aggr_name):
        aggr_without_relevant_attr = aggr_name[:-2]
        feature_name_only = aggr_without_relevant_attr[:aggr_without_relevant_attr.rindex('_')]
        feature_with_relevant_attr = f"{feature_name_only}{aggr_name[-2:]}"
        associated_time_key = self._aggregation_time_key(redis_key_prefix,
                                                         feature_with_relevant_attr)
        time_val = await RedisDriver.asyncify(self.redis.get)(associated_time_key)
        time_val = RedisDriver.convert_to_str(time_val)

        try:
            time_in_millis = int(time_val)
        except TypeError as e:
            raise RedisError(f"Invalid associated time attribute: {associated_time_key} "
                             f"-> {time_val}") from e

        # Return a form of the time attribute that Storey expects. This should include
        # name of the feature but not the aggregation name (min, max) or relevant
        # attribute (_a, _b).
        associated_time_attr = f"{self._aggregation_time_attribute_prefix}" \
                               f"{feature_with_relevant_attr}"

        return associated_time_attr, time_in_millis

    async def _load_aggregates_by_key(self, container, table_path, key):
        """
        Loads a specific key from the store, and returns it in the following format
        {
            'feature_name_aggr1': {<start time A>: [], {<start time B>: []}},
            'feature_name_aggr2': {<start time A>: [], {<start time B>: []}}
        }
        """
        redis_key_prefix = self._make_key(container, table_path, key)
        additional_data = await self._get_all_fields(self._static_data_key(redis_key_prefix))
        aggregations = {}
        # Aggregation Redis keys start with the Redis key prefix for this Storey container, table
        # path, and "key," followed by ":aggr_"
        aggr_key_prefix = f"{redis_key_prefix}:{self._aggregation_attribute_prefix}"
        # We can't use `async for` here...
        for aggr_key in self.redis.scan_iter(f"{aggr_key_prefix}*"):
            aggr_key = RedisDriver.convert_to_str(aggr_key)
            value = await RedisDriver.asyncify(self.redis.lrange)(aggr_key, 0, -1)
            # Build an attribute for this aggregation in the format that Storey
            # expects to receive from this method. The feature and aggregation
            # name are embedded in the Redis key. Also, drop the "_a" or "_b"
            # portion of the key, which is "the relevant attribute out of the 2
            # feature attributes," according to comments in the V3IO driver.
            feature_and_aggr_name = aggr_key[len(aggr_key_prefix):-2]

            # To get the associated time, we need the aggregation name and the relevant
            # attribute (a or b), so we take a second form of the string for that purpose.
            aggr_name_with_relevant_attribute = aggr_key[len(aggr_key_prefix):]
            associated_time_attr, time_in_millis = await self._get_associated_time_attr(
                redis_key_prefix, aggr_name_with_relevant_attribute)
            if feature_and_aggr_name not in aggregations:
                aggregations[feature_and_aggr_name] = {}
            aggregations[feature_and_aggr_name][time_in_millis] = [
                float(RedisDriver.convert_redis_value_to_python_obj(v)) for v in value]
            aggregations[feature_and_aggr_name][associated_time_attr] = time_in_millis

        # Story expects to get None back if there were no aggregations, and the
        # same for additional data.
        aggregations_to_return = aggregations if aggregations else None
        additional_data_to_return = additional_data if additional_data else None
        return aggregations_to_return, additional_data_to_return

    async def _fetch_state_by_key(self, aggr_item, container, table_path, key):
        key = str(key)
        # Aggregation Redis keys start with the Redis key prefix for this Storey container, table
        # path, and "key," followed by ":aggr_"
        aggregations = {}

        redis_key_prefix = self._make_key(container, table_path, key)
        aggr_key_prefix = f"{redis_key_prefix}:{self._aggregation_attribute_prefix}"
        # We can't use `async for` here...
        for aggr_key in self.redis.scan_iter(f"{aggr_key_prefix}*"):
            aggr_key = RedisDriver.convert_to_str(aggr_key)
            value = await RedisDriver.asyncify(self.redis.lrange)(aggr_key, 0, -1)
            # Build an attribute for this aggregation in the format that Storey
            # expects to receive from this method. The feature and aggregation
            # name are embedded in the Redis key. Also, drop the "_a" or "_b"
            # portion of the key, which is "the relevant attribute out of the 2
            # feature attributes," according to comments in the V3IO driver.
            feature_and_aggr_name = aggr_key[len(aggr_key_prefix):-2]

            # To get the associated time, we need the aggregation name and the relevant
            # attribute (a or b), so we take a second form of the string for that purpose.
            aggr_name_with_relevant_attribute = aggr_key[len(aggr_key_prefix):]
            associated_time_attr, time_in_millis = await self._get_associated_time_attr(
                redis_key_prefix, aggr_name_with_relevant_attribute)
            if feature_and_aggr_name not in aggregations:
                aggregations[feature_and_aggr_name] = {}
            aggregations[feature_and_aggr_name][time_in_millis] = [
                float(RedisDriver.convert_redis_value_to_python_obj(v)) for v in value]
            aggregations[feature_and_aggr_name][associated_time_attr] = time_in_millis

    def _get_time_attributes_from_aggregations(self, aggregation_element):
        attributes = {}
        for bucket in aggregation_element.aggregation_buckets.values():
            attributes[f'{bucket.name}_a'] = f'{self._aggregation_time_attribute_prefix}{bucket.name}_a'
            attributes[f'{bucket.name}_b'] = f'{self._aggregation_time_attribute_prefix}{bucket.name}_b'
        return list(attributes.values())

    async def _save_schema(self, container, table_path, schema):
        redis_key = self._make_key(container, table_path, schema_file_name)
        await RedisDriver.asyncify(self.redis.set)(redis_key, json.dumps(schema))

    async def _load_schema(self, container, table_path):
        redis_key = self._make_key(container, table_path, schema_file_name)
        schema = await RedisDriver.asyncify(self.redis.get)(redis_key)
        if schema:
            return json.loads(schema)
