import base64
import json
import os
import time
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
from typing import Optional
from collections import OrderedDict
from typing import Optional, Union
from typing import List, Optional, Union
import pandas as pd

import redis
import rediscluster
import v3io
import v3io.aio.dataplane
from v3io.dataplane import kv_array
from redis import Redis
from rediscluster import RedisCluster
from redis import WatchError
from redis.client import Pipeline
from redis import WatchError, ResponseError

from .dtypes import V3ioError, RedisError, RedisTimeoutError
from .utils import schema_file_name, asyncify


class Driver:
    """Abstract class for database connection"""

    async def _save_schema(self, container, table_path, schema):
        pass

    async def _load_schema(self, container, table_path):
        pass

    async def _save_key(self, container, table_path, key, aggr_item, partitioned_by_key, additional_data):
        pass

    async def _load_aggregates_by_key(self, container, table_path, key):
        return None, None

    async def _load_by_key(self, container, table_path, key, attributes):
        pass

    async def close(self):
        pass

    # Override if driver does not support aggregations
    def supports_aggregations(self):
        return True


class NoopDriver(Driver):
    pass


class NeedsV3ioAccess:
    """Checks that params for access to V3IO exist and are legal

    :param webapi: URL to the web API (https or http). If not set, the V3IO_API environment variable will be used.
    :param access_key: V3IO access key. If not set, the V3IO_ACCESS_KEY environment variable will be used.

    """

    def __init__(self, webapi=None, access_key=None):
        webapi = webapi or os.getenv('V3IO_API')
        if not webapi:
            raise ValueError('Missing webapi parameter or V3IO_API environment variable')

        if not webapi.startswith('http://') and not webapi.startswith('https://'):
            webapi = f'http://{webapi}'

        self._webapi_url = webapi

        access_key = access_key or os.getenv('V3IO_ACCESS_KEY')
        if not access_key:
            raise ValueError('Missing access_key parameter or V3IO_ACCESS_KEY environment variable')

        self._access_key = access_key


class V3ioDriver(NeedsV3ioAccess, Driver):
    """
    Database connection to V3IO.
    :param webapi: URL to the web API (https or http). If not set, the V3IO_API environment variable will be used.
    :param access_key: V3IO access key. If not set, the V3IO_ACCESS_KEY environment variable will be used.
    """

    def __init__(self, webapi: Optional[str] = None, access_key: Optional[str] = None, use_parallel_operations=True,
                 v3io_client_kwargs=None):
        NeedsV3ioAccess.__init__(self, webapi, access_key)
        self._v3io_client = None
        self._v3io_client_kwargs = v3io_client_kwargs or {}
        self._closed = True

        self._aggregation_attribute_prefix = 'aggr_'
        self._aggregation_time_attribute_prefix = '_'
        self._error_code_string = "ErrorCode"
        self._false_condition_error_code = "16777244"
        self._mtime_header_name = 'X-v3io-transaction-verifier'
        self._parallel_ops = use_parallel_operations

    def _lazy_init(self):
        self._closed = False
        if not self._v3io_client:
            self._v3io_client = v3io.aio.dataplane.Client(endpoint=self._webapi_url, access_key=self._access_key,
                                                          **self._v3io_client_kwargs)

    async def close(self):
        """Closes database connection to V3IO"""
        if self._v3io_client and not self._closed:
            self._closed = True
            await self._v3io_client.close()
            self._v3io_client = None

    saved_engine_words = {'min', 'max', 'sqrt', 'avg', 'stddev', 'sum', 'length', 'init_array', 'set', 'remove', 'regexp_replace', 'find',
                          'find_all_indices', 'extract_by_indices', 'sort_array', 'blob', 'if_else', 'in', 'true', 'false', 'and', 'or',
                          'not', 'contains', 'ends', 'starts', 'exists', 'select', 'str', 'to_int', 'if_not_exists', 'regexp_instr',
                          'delete', 'as', 'maxdbl', 'inf', 'nan', 'isnan', 'nanaszero'}
    parallel_aggregates = {'min': 'pmin', 'max': 'pmax', 'sum': 'psum', 'count': 'psum', 'sqr': 'psum'}

    async def _save_schema(self, container, table_path, schema):
        self._lazy_init()

        response = await self._v3io_client.object.put(container=container,
                                                      path=f'{table_path}/{schema_file_name}',
                                                      body=json.dumps(schema),
                                                      raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)

        if not response.status_code == 200:
            raise V3ioError(f'Failed to save schema file. Response status code was {response.status_code}: {response.body}')

    async def _load_schema(self, container, table_path):
        self._lazy_init()

        schema_path = f'{table_path}/{schema_file_name}'
        response = await self._v3io_client.object.get(container, schema_path,
                                                      raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
        if response.status_code == 404:
            schema = None
        elif response.status_code == 200:
            schema = json.loads(response.body)
        else:
            raise V3ioError(f'Failed to get schema at {schema_path}. Response status code was {response.status_code}: {response.body}')

        return schema

    async def _save_key(self, container, table_path, key, aggr_item, partitioned_by_key, additional_data=None):
        self._lazy_init()

        # test whether server support parallel operations
        if self._parallel_ops:
            try:
                test_expression = "a=init_array(2,'double',0.0);a[0..1]=pmax(a[0..1], a[0..1]);delete(a);"
                response = await self._v3io_client.kv.update(container, table_path, str(key),
                                                             expression=test_expression, condition="",
                                                             raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
                if response.status_code != 200:
                    self._parallel_ops = False
            except Exception:
                pass

        should_raise_error = False
        update_expression, condition_expression, pending_updates = self._build_feature_store_update_expression(aggr_item, additional_data,
                                                                                                               partitioned_by_key)
        if not update_expression:
            return

        key = str(key)
        response = await self._v3io_client.kv.update(container, table_path, key, expression=update_expression,
                                                     condition=condition_expression,
                                                     raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
        if response.status_code == 200:
            if aggr_item:
                aggr_item.storage_specific_cache[self._mtime_header_name] = response.headers[self._mtime_header_name]
        # In case Mtime condition evaluated to False, we run the conditioned expression, then fetch and cache the latest key's state
        elif self._is_false_condition_error(response):
            update_expression, condition_expression, pending_updates = self._build_feature_store_update_expression(aggr_item,
                                                                                                                   additional_data,
                                                                                                                   False,
                                                                                                                   pending_updates)
            response = await self._v3io_client.kv.update(container, table_path, key, expression=update_expression,
                                                         condition=condition_expression,
                                                         raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
            if response.status_code == 200 and aggr_item:
                await self._fetch_state_by_key(aggr_item, container, table_path, key)
            else:
                should_raise_error = True
        else:
            should_raise_error = True

        if should_raise_error:
            raise V3ioError(
                f'Failed to save aggregation for {table_path}/{key}. Response status code was {response.status_code}: {response.body}\n'
                f'Update expression was: {update_expression}'
            )

    async def _fetch_state_by_key(self, aggr_item, container, table_path, key):
        attributes_to_get = self._get_time_attributes_from_aggregations(aggr_item)
        key = str(key)
        get_item_response = await self._v3io_client.kv.get(container, table_path, key, attribute_names=attributes_to_get,
                                                           raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
        if get_item_response.status_code == 200:
            aggr_item.storage_specific_cache[self._mtime_header_name] = get_item_response.headers[self._mtime_header_name]

            # First reset all relevant cache items
            for bucket in aggr_item.aggregation_buckets.values():
                if bucket.should_persist:
                    for attribute_to_reset in attributes_to_get:
                        if attribute_to_reset.startswith(f'{self._aggregation_time_attribute_prefix}{bucket.name}_'):
                            bucket.storage_specific_cache.pop(attribute_to_reset, None)

            for name, value in get_item_response.output.item.items():
                for bucket in aggr_item.aggregation_buckets.values():
                    if bucket.should_persist:
                        if name.startswith(f'{self._aggregation_time_attribute_prefix}{bucket.name}_'):
                            bucket.storage_specific_cache[name] = int(value.timestamp() * 1000)
        else:
            raise V3ioError(
                f'Failed to query {table_path}/{key}. Response status code was {get_item_response.status_code}: {get_item_response.body}')

    def _get_time_attributes_from_aggregations(self, aggregation_element):
        attributes = {}
        for bucket in aggregation_element.aggregation_buckets.values():
            attributes[f'{bucket.name}_a'] = f'{self._aggregation_time_attribute_prefix}{bucket.name}_a'
            attributes[f'{bucket.name}_b'] = f'{self._aggregation_time_attribute_prefix}{bucket.name}_b'
        return list(attributes.values())

    def _is_false_condition_error(self, response):
        if response.status_code == 400:
            body = response.body.decode("utf-8")
            if self._error_code_string in body and self._false_condition_error_code in body:
                return True
        return False

    def _build_feature_store_update_expression(self, aggregation_element, additional_data, partitioned_by_key, pending=None):
        condition_expression = None
        expressions = []
        pending_updates = {}

        if aggregation_element:
            # Generating aggregation related expressions
            # In case we get an indication the data is (probably) not updated from multiple workers (for example: pre sharded by key) run a
            # simpler expression.
            if partitioned_by_key:
                expressions, pending_updates = self._build_simplified_feature_store_request(aggregation_element)
                condition_expression = aggregation_element.storage_specific_cache.get(self._mtime_header_name, "")
            else:
                expressions, pending_updates = self._build_conditioned_feature_store_request(aggregation_element, pending)

        # Generating additional cache expressions
        if additional_data:
            for name, value in additional_data.items():
                if name.casefold() in self.saved_engine_words:
                    name = f'`{name}`'
                expression_value = self._convert_python_obj_to_expression_value(value)
                if expression_value:
                    expressions.append(f'{name}={self._convert_python_obj_to_expression_value(value)}')
                else:
                    expressions.append(f'REMOVE {name}')

        update_expression = ';'.join(expressions)
        return update_expression, condition_expression, pending_updates

    def _discard_old_pending_items(self, pending, max_window_millis):
        res = OrderedDict()
        if pending:
            pending_keys = list(pending)
            pending_keys.sort()
            last_time = int(pending_keys[-1] / max_window_millis) * max_window_millis
            min_time = last_time - max_window_millis
            for time in pending_keys:
                if time > min_time:
                    res[time] = pending[time]
        return res

    def _build_conditioned_feature_store_request(self, aggregation_element, pending=None):
        expressions = []

        times_update_expressions = {}
        pending_updates = {}
        initialized_attributes = {}
        for name, bucket in aggregation_element.aggregation_buckets.items():
            # Only save raw aggregates, not virtual
            if bucket.should_persist:

                if pending:
                    items_to_update = pending[name]
                else:
                    # In case we have pending data that spreads over more then 2 windows discard the old ones.
                    pending_updates[name] = self._discard_old_pending_items(bucket.get_and_flush_pending(), bucket.max_window_millis)
                    items_to_update = pending_updates[name]
                for bucket_start_time, aggregation_values in items_to_update.items():
                    # the relevant attribute out of the 2 feature attributes
                    feature_attr = 'a' if int(bucket_start_time / bucket.max_window_millis) % 2 == 0 else 'b'

                    array_time_attribute_name = f'{self._aggregation_time_attribute_prefix}{bucket.name}_{feature_attr}'

                    expected_time = int(bucket_start_time / bucket.max_window_millis) * bucket.max_window_millis
                    expected_time_expr = self._convert_python_obj_to_expression_value(datetime.fromtimestamp(expected_time / 1000))
                    index_to_update = int((bucket_start_time - expected_time) / bucket.period_millis)

                    get_array_time_expr = f'if_not_exists({array_time_attribute_name},0:0)'

                    for aggregation, aggregation_value in aggregation_values.items():
                        array_attribute_name = f'{self._aggregation_attribute_prefix}{name}_{aggregation}_{feature_attr}'
                        if not initialized_attributes.get(array_attribute_name, 0) == expected_time:
                            initialized_attributes[array_attribute_name] = expected_time
                            init_expression = f'{array_attribute_name}=if_else(({get_array_time_expr}<{expected_time_expr}),' \
                                              f"init_array({bucket.total_number_of_buckets},'double'," \
                                              f'{aggregation_value.default_value}),{array_attribute_name})'
                            expressions.append(init_expression)

                        arr_at_index = f'{array_attribute_name}[{index_to_update}]'
                        update_array_expression = f'{arr_at_index}=if_else(({get_array_time_expr}>{expected_time_expr}),{arr_at_index},' \
                                                  f'{aggregation_value.get_update_expression(arr_at_index)})'

                        expressions.append(update_array_expression)

                        # Separating time attribute updates, so that they will be executed in the end and only once per feature name.
                        if array_time_attribute_name not in times_update_expressions:
                            times_update_expressions[array_time_attribute_name] = f'{array_time_attribute_name}=' \
                                                                                  f'if_else(({get_array_time_expr}<{expected_time_expr}),' \
                                                                                  f'{expected_time_expr},{array_time_attribute_name})'

        expressions.extend(times_update_expressions.values())

        return expressions, pending_updates

    def _build_simplified_feature_store_request(self, aggregation_element):
        expressions = []

        times_update_expressions = {}
        new_cached_times = {}
        pending_updates = {}
        initialized_attributes = {}
        pexpressions = {}
        use_parallel = self._parallel_ops

        for name, bucket in aggregation_element.aggregation_buckets.items():

            # Only save raw aggregates, not virtual
            if bucket.should_persist:

                # In case we have pending data that spreads over more then 2 windows discard the old ones.
                pending_updates[name] = self._discard_old_pending_items(bucket.get_and_flush_pending(),
                                                                        bucket.max_window_millis)
                use_parallel = use_parallel and len(pending_updates[name].items()) > 1

                for bucket_start_time, aggregation_values in pending_updates[name].items():
                    # the relevant attribute out of the 2 feature attributes
                    feature_attr = 'a' if int(bucket_start_time / bucket.max_window_millis) % 2 == 0 else 'b'

                    array_time_attribute_name = f'{self._aggregation_time_attribute_prefix}{bucket.name}_{feature_attr}'

                    cached_time = bucket.storage_specific_cache.get(array_time_attribute_name, 0)

                    expected_time = int(bucket_start_time / bucket.max_window_millis) * bucket.max_window_millis
                    expected_time_expr = self._convert_python_obj_to_expression_value(
                        datetime.fromtimestamp(expected_time / 1000))
                    index_to_update = int((bucket_start_time - expected_time) / bucket.period_millis)

                    for aggregation, aggregation_value in aggregation_values.items():
                        array_attribute_name = f'{self._aggregation_attribute_prefix}{name}_{aggregation}_{feature_attr}'

                        if use_parallel and aggregation in self.parallel_aggregates:
                            if array_attribute_name not in pexpressions:
                                pexpressions[array_attribute_name] = {
                                    "values": [aggregation_value.default_value] * bucket.total_number_of_buckets,
                                    "first_index": index_to_update,
                                    "last_index": index_to_update,
                                    "default_value": aggregation_value.default_value,
                                    "aggregation": aggregation
                                }
                        # Possibly initiating the array
                        if cached_time < expected_time:
                            if not initialized_attributes.get(array_attribute_name, 0) == expected_time:
                                initialized_attributes[array_attribute_name] = expected_time
                                expressions.append(
                                    f"{array_attribute_name}=init_array({bucket.total_number_of_buckets},'double',"
                                    f'{aggregation_value.default_value})'
                                )
                            if array_time_attribute_name not in times_update_expressions:
                                times_update_expressions[array_time_attribute_name] = \
                                    f'{array_time_attribute_name}={expected_time_expr}'
                            new_cached_times[name] = (array_time_attribute_name, expected_time)

                        # Updating the specific cells
                        if cached_time <= expected_time:
                            if use_parallel and aggregation in self.parallel_aggregates:
                                if pexpressions[array_attribute_name]["first_index"] > index_to_update:
                                    pexpressions[array_attribute_name]["first_index"] = index_to_update
                                if pexpressions[array_attribute_name]["last_index"] < index_to_update:
                                    pexpressions[array_attribute_name]["last_index"] = index_to_update
                                pexpressions[array_attribute_name]["values"][index_to_update] = aggregation_value.value
                            else:
                                arr_at_index = f'{array_attribute_name}[{index_to_update}]'
                                expressions.append(
                                    f'{arr_at_index}={aggregation_value.get_update_expression(arr_at_index)}'
                                )

        if use_parallel:
            for attr_name, d in pexpressions.items():
                encoded_array = kv_array.encode_list(d["values"][d["first_index"]:d["last_index"] + 1]).decode()
                paggregate = self.parallel_aggregates[d["aggregation"]]
                sliced_array = f'{attr_name}[{d["first_index"]}..{d["last_index"]}]'
                expressions.append(
                    f"{sliced_array}={paggregate}({sliced_array}, blob('{encoded_array}'))")

        # Separating time attribute updates, so that they will be executed in the end and only once per feature name.
        expressions.extend(times_update_expressions.values())

        for name, new_value in new_cached_times.items():
            attribute_name = new_value[0]
            new_time = new_value[1]
            aggregation_element.aggregation_buckets[name].storage_specific_cache[attribute_name] = new_time
        return expressions, pending_updates

    @staticmethod
    def _convert_python_obj_to_expression_value(value):
        if isinstance(value, str):
            return f"'{value}'"
        if isinstance(value, bool) or isinstance(value, float) or isinstance(value, int):
            return str(value)
        elif isinstance(value, bytes):
            return f"blob('{base64.b64encode(value).decode('ascii')}')"
        elif isinstance(value, datetime):
            if pd.isnull(value):
                return None
            timestamp = value.timestamp()

            secs = int(timestamp)
            nanosecs = int((timestamp - secs) * 1e+9)
            return f'{secs}:{nanosecs}'
        elif isinstance(value, timedelta):
            return str(value.value)
        else:
            raise V3ioError(f'Type {type(value)} in UpdateItem request is not supported')

    # Loads a specific key from the store, and returns it in the following format
    # {
    #   'feature_name_aggr1': {<start time A>: [], {<start time B>: []}},
    #   'feature_name_aggr2': {<start time A>: [], {<start time B>: []}}
    # }
    async def _load_aggregates_by_key(self, container, table_path, key):
        self._lazy_init()

        key = str(key)

        response = await self._v3io_client.kv.get(container, table_path, key, raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
        if response.status_code == 404:
            return None, None
        elif response.status_code == 200:
            aggregations, additional_data = {}, {}
            for name, value in response.output.item.items():
                if name.startswith(self._aggregation_attribute_prefix):
                    feature_and_aggr_name = name[len(self._aggregation_attribute_prefix):-2]
                    feature_name = feature_and_aggr_name[:feature_and_aggr_name.rindex('_')]
                    associated_time_attr = f'{self._aggregation_time_attribute_prefix}{feature_name}_{name[-1]}'

                    time_in_millis = int(response.output.item[associated_time_attr].timestamp() * 1000)
                    if feature_and_aggr_name not in aggregations:
                        aggregations[feature_and_aggr_name] = {}
                    aggregations[feature_and_aggr_name][time_in_millis] = value
                    aggregations[feature_and_aggr_name][associated_time_attr] = time_in_millis
                elif not name.startswith(self._aggregation_time_attribute_prefix):
                    additional_data[name] = value
            return aggregations, additional_data
        else:
            raise V3ioError(f'Failed to get item. Response status code was {response.status_code}: {response.body}')

    async def _load_by_key(self, container, table_path, key, attributes):
        self._lazy_init()

        response = await self._get_item(container, table_path, key, attributes)
        if response.status_code == 404:
            return None
        elif response.status_code == 200:
            res = {}
            for name in response.output.item.keys():
                if not name.startswith((self._aggregation_attribute_prefix, self._aggregation_time_attribute_prefix)):
                    res[name] = response.output.item[name]
            return res
        else:
            raise V3ioError(f'Failed to get item. Response status code was {response.status_code}: {response.body}')

    async def _describe(self, container, stream_path):
        self._lazy_init()

        response = await self._v3io_client.stream.describe(container, stream_path)
        if response.status_code != 200:
            raise V3ioError(f'Failed to get number of shards. Got {response.status_code} response: {response.body}')
        return response.output

    async def _create_stream(self, container, stream_path, shards, retention):
        self._lazy_init()

        res = await self._v3io_client.stream.create(
            container=container,
            stream_path=stream_path,
            shard_count=shards,
            retention_period_hours=retention,
            raise_for_status=v3io.aio.dataplane.RaiseForStatus.never,
        )
        res.raise_for_status([409, 204])
        return res.status_code

    async def _put_records(self, container, stream_path, payload):
        self._lazy_init()

        return await self._v3io_client.stream.put_records(container, stream_path, payload)

    async def _get_item(self, container, table_path, key, attributes):
        self._lazy_init()

        return await self._v3io_client.kv.get(container, table_path, str(key), attribute_names=attributes,
                                              raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)


class RedisType(Enum):
    STANDALONE = 1
    CLUSTER = 2


class RedisDriver(Driver):
    REDIS_TIMEOUT = 5  # Seconds
    REDIS_WATCH_INTERVAL = 1  # Seconds
    DATETIME_FIELD_PREFIX = "_dt:"
    TIMEDELTA_FIELD_PREFIX = "_td:"
    DEFAULT_KEY_PREFIX = "storey:"

    def __init__(self, redis_client: Optional[Union[redis.Redis, rediscluster.RedisCluster]] = None,
                 redis_type: RedisType = RedisType.STANDALONE,
                 key_prefix: str = None,
                 aggregation_attribute_prefix: str = 'aggr_',
                 aggregation_time_attribute_prefix: str = 't_'):

        self._redis = redis_client
        self._conn_str = os.getenv('REDIS_CONNECTION', 'redis://localhost:6379?')
        self._key_prefix = key_prefix if key_prefix else self.DEFAULT_KEY_PREFIX
        self._type = redis_type
        self._aggregation_attribute_prefix = aggregation_attribute_prefix
        self._aggregation_time_attribute_prefix = aggregation_time_attribute_prefix
        self._aggregation_prefixes = (self._aggregation_attribute_prefix,
                                      self._aggregation_time_attribute_prefix)

    @property
    def redis(self):
        if self._redis:
            return self._redis
        if self._type is RedisType.STANDALONE:
            return redis.Redis.from_url(self._conn_str, decode_responses=True)
        self._redis = rediscluster.RedisCluster.from_url(self._conn_str, decode_response=True)
        return self._redis

    def make_key(self, *parts):
        return f"{self._key_prefix}{':'.join([str(p) for p in parts])}"

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

    async def _lincr(self, key: str, index_to_update: int, incr_by: float):
        """Atomically increment the numeric value at an index in a list.

        This is, in effect, a client-side LINCR command (list-increment) -- a command
        that does not exist in Redis.
        """
        timeout = datetime.now() + timedelta(seconds=self.REDIS_TIMEOUT)
        while True:
            with self.redis.pipeline() as pipeline:
                try:
                    pipeline.watch(key)
                    old = pipeline.lindex(key, index_to_update)
                    pipeline.multi()
                    # XXX: Some aggregations use these values as their defaults, so we
                    # initialize our lists with them. But you can't create a float('inf')
                    # and then add to it, so we use 0 instead.
                    if old == 'inf' or old == '-inf':
                        old = 0
                    pipeline.lset(key, index_to_update, float(old) + incr_by)
                    return await asyncify(pipeline.execute)()
                except WatchError as e:
                    if datetime.now() < timeout:
                        time.sleep(self.REDIS_WATCH_INTERVAL)
                        continue
                    raise RedisTimeoutError(f"Timed out trying to increment key {key}") from e

    @classmethod
    def _convert_python_obj_to_redis_value(cls, value):
        if isinstance(value, datetime):
            if pd.isnull(value):
                return None
            return f"{cls.DATETIME_FIELD_PREFIX}{value.timestamp()}"
        elif isinstance(value, timedelta):
            return f"{cls.TIMEDELTA_FIELD_PREFIX}{value.total_seconds()}"
        else:
            return json.dumps(value)

    @classmethod
    def _convert_redis_value_to_python_obj(cls, value):
        if value.startswith(cls.TIMEDELTA_FIELD_PREFIX):
            return timedelta(seconds=float(value.split(":")[1]))
        elif value.startswith(cls.DATETIME_FIELD_PREFIX):
            return datetime.fromtimestamp(float(value.split(":")[1]))
        else:
            return json.loads(value)

    def _discard_old_pending_items(self, pending, max_window_millis):
        res = {}
        if pending:
            last_time = int(list(pending.keys())[-1] / max_window_millis) * max_window_millis
            min_time = last_time - max_window_millis
            for time, value in pending.items():
                if time > min_time:
                    res[time] = value
        return res

    async def _build_feature_store_request(self, redis_key_prefix, aggregation_element, pipeline):
        atomic_updates = []
        times_updates = {}
        new_cached_times = {}
        pending_updates = {}
        initialized_attributes = {}

        for name, bucket in aggregation_element.aggregation_buckets.items():
            # Only save raw aggregates, not virtual
            if bucket.should_persist:

                # In case we have pending data that spreads over more then 2 windows, discard the old ones.
                pending_updates[name] = self._discard_old_pending_items(bucket.get_and_flush_pending(),
                                                                        bucket.max_window_millis)
                for bucket_start_time, aggregation_values in pending_updates[name].items():
                    # the relevant attribute out of the 2 feature attributes
                    feature_attr = 'a' if int(bucket_start_time / bucket.max_window_millis) % 2 == 0 else 'b'

                    aggr_time_attribute_name = f'{bucket.name}_{feature_attr}'
                    array_time_attribute_key = self._aggregation_time_key(redis_key_prefix,
                                                                          aggr_time_attribute_name)

                    cached_time = bucket.storage_specific_cache.get(array_time_attribute_key, 0)

                    expected_time = int(bucket_start_time / bucket.max_window_millis) * bucket.max_window_millis
                    expected_time_expr = self._convert_python_obj_to_redis_value(
                        datetime.fromtimestamp(expected_time / 1000))
                    index_to_update = int((bucket_start_time - expected_time) / bucket.period_millis)

                    for aggregation, aggregation_value in aggregation_values.items():
                        list_attribute_name = f'{self._aggregation_attribute_prefix}{name}_{aggregation}_{feature_attr}'
                        list_attribute_key = self._list_key(redis_key_prefix, list_attribute_name)
                        if cached_time < expected_time:
                            if not initialized_attributes.get(list_attribute_key, 0) == expected_time:
                                initialized_attributes[list_attribute_key] = expected_time
                                # XXX: Run LLEN and LPUSH commands in immediate execution
                                # mode, instead of using the pipeline, to initialize the
                                # list here, in case we need the list to exist when we run
                                # our atomic _lincr() operations later in this method.
                                list_length = await asyncify(self.redis.llen)(list_attribute_key)
                                if list_length != bucket.total_number_of_buckets:
                                    # The list doesn't exist yet, or it's the wrong size,
                                    # so we initialize it (or right-pad it) to the
                                    # expected length with default values.
                                    new_length = bucket.total_number_of_buckets - list_length
                                    defaults = [aggregation_value.default_value] * new_length
                                    self.redis.rpush(list_attribute_key, *defaults)
                            if array_time_attribute_key not in times_updates:
                                times_updates[array_time_attribute_key] = expected_time_expr
                            new_cached_times[name] = (array_time_attribute_key, expected_time)

                        # Updating the specific cells
                        if cached_time <= expected_time:
                            # Increment the value in the list index by the new value
                            atomic_updates.append(
                                partial(self._lincr, list_attribute_key, index_to_update, aggregation_value.value))

        for atomic_update in atomic_updates:
            # TODO: Check result, handle RedisError?
            await atomic_update()

        # Separating time attribute updates, so that they will be executed in the end
        # and only once per feature name.
        for array_time_attribute_key, expected_time_expr in times_updates.items():
            pipeline.set(array_time_attribute_key, expected_time_expr)

        for name, new_value in new_cached_times.items():
            attribute_name = new_value[0]
            new_time = new_value[1]
            aggregation_element.aggregation_buckets[name].storage_specific_cache[attribute_name] = new_time
        return pending_updates

    async def _build_updates(self, redis_key_prefix, aggregation_element, additional_data, partitioned_by_key):
        # Use a transaction if multiple workers might be writing (AKA, "partitioned_by_key").
        # Otherwise, use a standard Redis pipeline.
        pipeline = self.redis.pipeline(transaction=partitioned_by_key)

        if aggregation_element:
            # Generating aggregation related expressions
            await self._build_feature_store_request(redis_key_prefix, aggregation_element, pipeline)

        # Static attributes, like "name," "age," -- everything that isn't an agg.
        if additional_data:
            fields_to_set = {}
            fields_to_delete = []
            for name, value in additional_data.items():
                expression_value = self._convert_python_obj_to_redis_value(value)
                if expression_value:
                    fields_to_set[name] = expression_value
                else:
                    fields_to_delete.append(name)
            if fields_to_set:
                pipeline.hset(self._static_data_key(redis_key_prefix), mapping=fields_to_set)
            if fields_to_delete:
                pipeline.delete(*fields_to_delete)

        result = await asyncify(pipeline.execute)()
        pipeline.reset()

        return result

    async def _save_key(self, container, table_path, key, aggr_item, partitioned_by_key, additional_data):
        redis_key_prefix = self.make_key(container, table_path, key)
        await self._build_updates(redis_key_prefix, aggr_item, additional_data, partitioned_by_key)

    async def _get_all_fields(self, redis_key: str):
        try:
            values = await asyncify(self.redis.hgetall)(redis_key)
        except redis.ResponseError as e:
            raise RedisError(f'Failed to get key {redis_key}. Response error was: {e}')
        return {key: self._convert_redis_value_to_python_obj(val) for key, val in values.items()
                if not str(key).startswith(self._aggregation_prefixes)}

    async def _get_specific_fields(self, redis_key: str, attributes: List[str]):
        non_aggregation_attrs = [
            name for name in attributes
            if not name.startswith(self._aggregation_prefixes)
        ]
        try:
            values = await asyncify(self.redis.hmget)(redis_key, non_aggregation_attrs)
        except redis.ResponseError as e:
            raise RedisError(f'Failed to get key {redis_key}. Response error was: {e}') from e
        return [{k: self._convert_redis_value_to_python_obj(v)} for k, v in values.items()]

    async def _load_by_key(self, container, table_path, key, attributes):
        """
        Return all static attributes, or certain attributes.

        NOTE: Following the V3IO driver's implementation, this method will not
        return aggregation attributes or associated time values -- just static
        data, AKA "additional data."
        """
        redis_key_prefix = self.make_key(container, table_path, key)
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
        time_val = await asyncify(self.redis.get)(associated_time_key)
        _, val = time_val.split(":")

        # TODO: Time values can hold either a timedelta in seconds or a UNIX timestamp.
        # What are we supposed to do (and how does Storey know) if the value was a
        # timedelta?
        try:
            # Storey requires the timestamp to be an integer when it processes this value.
            time_in_millis = int(float(val) * 1000)
        except TypeError as e:
            raise RedisError(f"Invalid associated time attribute: {associated_time_key} "
                             f"-> {time_val}") from e

        # Return a form of the time attribute that Storey expects. This should include
        # name of the feature but not the aggregation name (min, max) or relevant
        # attribute (_a, _b).
        associated_time_attr = f"{self._aggregation_time_attribute_prefix}" \
                               f"{feature_with_relevant_attr}"
        return associated_time_attr, time_in_millis

    def cast(self, value):
        """Cast `value` to the type Storey expects."""
        if '.' in value or value == 'inf' or value == "-inf":
            return float(value)
        else:
            return int(value)

    async def _load_aggregates_by_key(self, container, table_path, key):
        """
        Loads a specific key from the store, and returns it in the following format
        {
            'feature_name_aggr1': {<start time A>: [], {<start time B>: []}},
            'feature_name_aggr2': {<start time A>: [], {<start time B>: []}}
        }
        """
        redis_key_prefix = self.make_key(container, table_path, key)
        additional_data = await self._get_all_fields(self._static_data_key(redis_key_prefix))
        aggregations = {}
        # Aggregation Redis keys start with the Redis key prefix for this Storey container, table
        # path, and "key," followed by ":aggr_"
        aggr_key_prefix = f"{redis_key_prefix}:{self._aggregation_attribute_prefix}"
        # XXX: We can't use `async for` here...
        for aggr_key in self.redis.scan_iter(f"{aggr_key_prefix}*"):
            value = await asyncify(self.redis.lrange)(aggr_key, 0, -1)
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
            aggregations[feature_and_aggr_name][time_in_millis] = [self.cast(v) for v in value]
            aggregations[feature_and_aggr_name][associated_time_attr] = time_in_millis
        return aggregations, additional_data

    async def _save_schema(self, container, table_path, schema):
        redis_key = self.make_key(container, table_path, schema_file_name)
        await asyncify(self.redis.set)(redis_key, json.dumps(schema))

    async def _load_schema(self, container, table_path):
        redis_key = self.make_key(container, table_path, schema_file_name)
        schema = await asyncify(self.redis.get)(redis_key)
        if schema:
            return json.loads(schema)
