import base64
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
from typing import List, Optional, Union
import pandas as pd

import redis
import rediscluster
import v3io
import v3io.aio.dataplane
from redis import WatchError
from redis.client import Pipeline

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

    def __init__(self, webapi: Optional[str] = None, access_key: Optional[str] = None):
        NeedsV3ioAccess.__init__(self, webapi, access_key)
        self._v3io_client = None
        self._closed = True

        self._aggregation_attribute_prefix = 'aggr_'
        self._aggregation_time_attribute_prefix = 't_'
        self._error_code_string = "ErrorCode"
        self._false_condition_error_code = "16777244"
        self._mtime_header_name = 'X-v3io-transaction-verifier'

    def _lazy_init(self):
        self._closed = False
        if not self._v3io_client:
            self._v3io_client = v3io.aio.dataplane.Client(endpoint=self._webapi_url, access_key=self._access_key)

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
        res = {}
        if pending:
            last_time = int(list(pending.keys())[-1] / max_window_millis) * max_window_millis
            min_time = last_time - max_window_millis
            for time, value in pending.items():
                if time > min_time:
                    res[time] = value
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
        for name, bucket in aggregation_element.aggregation_buckets.items():
            # Only save raw aggregates, not virtual
            if bucket.should_persist:

                # In case we have pending data that spreads over more then 2 windows discard the old ones.
                pending_updates[name] = self._discard_old_pending_items(bucket.get_and_flush_pending(), bucket.max_window_millis)
                for bucket_start_time, aggregation_values in pending_updates[name].items():
                    # the relevant attribute out of the 2 feature attributes
                    feature_attr = 'a' if int(bucket_start_time / bucket.max_window_millis) % 2 == 0 else 'b'

                    array_time_attribute_name = f'{self._aggregation_time_attribute_prefix}{bucket.name}_{feature_attr}'

                    cached_time = bucket.storage_specific_cache.get(array_time_attribute_name, 0)

                    expected_time = int(bucket_start_time / bucket.max_window_millis) * bucket.max_window_millis
                    expected_time_expr = self._convert_python_obj_to_expression_value(datetime.fromtimestamp(expected_time / 1000))
                    index_to_update = int((bucket_start_time - expected_time) / bucket.period_millis)

                    for aggregation, aggregation_value in aggregation_values.items():
                        array_attribute_name = f'{self._aggregation_attribute_prefix}{name}_{aggregation}_{feature_attr}'
                        # Possibly initiating the array
                        if cached_time < expected_time:
                            if not initialized_attributes.get(array_attribute_name, 0) == expected_time:
                                initialized_attributes[array_attribute_name] = expected_time
                                expressions.append(f"{array_attribute_name}=init_array({bucket.total_number_of_buckets},'double',"
                                                   f'{aggregation_value.default_value})')
                            if array_time_attribute_name not in times_update_expressions:
                                times_update_expressions[array_time_attribute_name] = \
                                    f'{array_time_attribute_name}={expected_time_expr}'
                            new_cached_times[name] = (array_time_attribute_name, expected_time)

                        # Updating the specific cells
                        if cached_time <= expected_time:
                            arr_at_index = f'{array_attribute_name}[{index_to_update}]'
                            expressions.append(f'{arr_at_index}={aggregation_value.get_update_expression(arr_at_index)}')

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
    REDIS_TIMEOUT = 10  # Seconds
    REDIS_WATCH_INTERVAL = 2  # Seconds

    def __init__(self, redis_client: Optional[Union[redis.Redis, rediscluster.RedisCluster]] = None,
                 redis_type: RedisType = RedisType.STANDALONE,
                 key_prefix: str = "",
                 aggregation_attribute_prefix: str = 'aggr_',
                 aggregation_time_attribute_prefix: str = 't_'):

        self._redis = redis_client
        self._conn_str = os.getenv('REDIS_CONNECTION', 'redis://localhost:6379')
        self._key_prefix = key_prefix
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
            return redis.Redis.from_url(self._conn_str)
        self._redis = rediscluster.RedisCluster.from_url(self._conn_str)
        return self._redis

    def make_key(self, table_path, key):
        return "{}{}{}".format(self._key_prefix, table_path, key)

    async def _lincr(self, key: str, index_to_update: int, incr_by: float):
        """Atomically increment the numeric value at an index in a list.

        This is, in effect, a client-side LINCR command (list-increment) -- a command
        that does not exist in Redis.
        """
        timeout = datetime.now() + timedelta(seconds=self.REDIS_TIMEOUT)
        with self.redis.pipeline() as pipeline:
            while True:
                try:
                    pipeline.watch(key)
                    old = pipeline.lindex(key, index_to_update)
                    pipeline.multi()
                    pipeline.lset(key, index_to_update, old + incr_by)
                    return await asyncify(pipeline.execute)()
                except WatchError:
                    if datetime.now() < timeout:
                        time.sleep(self.REDIS_WATCH_INTERVAL)
                        continue
                    raise RedisTimeoutError(f"Timed out trying to increment key {key}")

    @staticmethod
    def _convert_python_obj_to_redis_value(value):
        if isinstance(value, datetime):
            if pd.isnull(value):
                return None
            timestamp = value.timestamp()
            secs = int(timestamp)
            nanosecs = int((timestamp - secs) * 1e+9)
            return f'{secs}:{nanosecs}'
        elif isinstance(value, timedelta):
            return str(value.total_seconds())
        else:
            return str(value)

    def _discard_old_pending_items(self, pending, max_window_millis):
        res = {}
        if pending:
            last_time = int(list(pending.keys())[-1] / max_window_millis) * max_window_millis
            min_time = last_time - max_window_millis
            for time, value in pending.items():
                if time > min_time:
                    res[time] = value
        return res

    def _build_simplified_feature_store_request(self, aggregation_element, pipeline):
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

                    array_time_attribute_name = f'{self._aggregation_time_attribute_prefix}{bucket.name}_{feature_attr}'

                    cached_time = bucket.storage_specific_cache.get(array_time_attribute_name, 0)

                    expected_time = int(bucket_start_time / bucket.max_window_millis) * bucket.max_window_millis
                    expected_time_expr = self._convert_python_obj_to_redis_value(
                        datetime.fromtimestamp(expected_time / 1000))
                    index_to_update = int((bucket_start_time - expected_time) / bucket.period_millis)

                    for aggregation, aggregation_value in aggregation_values.items():
                        list_attribute_key = f'{self._aggregation_attribute_prefix}{name}_{aggregation}_{feature_attr}'
                        if cached_time < expected_time:
                            if not initialized_attributes.get(list_attribute_key, 0) == expected_time:
                                initialized_attributes[list_attribute_key] = expected_time
                                pipeline.lset(list_attribute_key, 0, aggregation_value.default_value)
                            if array_time_attribute_name not in times_updates:
                                times_updates[array_time_attribute_name] = expected_time_expr
                            new_cached_times[name] = (array_time_attribute_name, expected_time)

                        # Updating the specific cells
                        if cached_time <= expected_time:
                            # Increment the value in the list index by the new value
                            atomic_updates.append(
                                partial(self._lincr, list_attribute_key, index_to_update, aggregation_value.value))

        for atomic_update in atomic_updates:
            # TODO: Check result, handle RedisError
            await atomic_update()

        # Separating time attribute updates, so that they will be executed in the end
        # and only once per feature name.
        for array_time_attribute_name, expected_time_expr in times_updates.items():
            pipeline.set(array_time_attribute_name, expected_time_expr)

        for name, new_value in new_cached_times.items():
            attribute_name = new_value[0]
            new_time = new_value[1]
            aggregation_element.aggregation_buckets[name].storage_specific_cache[attribute_name] = new_time
        return pending_updates

    def _build_updates(self, aggregation_element, additional_data, partitioned_by_key, pipeline):
        if aggregation_element:
            # In case we get an indication the data is (probably) not updated from
            # multiple workers (for example: pre sharded by key), run a simpler expression.
            if partitioned_by_key:
                self._build_simplified_feature_store_request(aggregation_element, pipeline)
            else:
                # TODO
                # self._build_conditioned_feature_store_request(aggregation_element, pipeline)
                pass

        # Static attributes, like "name," "age," -- everything that isn't an agg.
        if additional_data:
            # TODO: Is `name` the right key to use in Redis, or should we namespace it?
            for name, value in additional_data.items():
                expression_value = self._convert_python_obj_to_redis_value(value)
                if expression_value:
                    pipeline.set(name, self._convert_python_obj_to_redis_value(value))
                else:
                    pipeline.delete(name)

    async def _save_key(self, container, table_path, key, aggr_item, partitioned_by_key, additional_data):
        with self.redis.pipeline(transaction=False) as p:
            self._build_updates(aggr_item, additional_data, partitioned_by_key, p)
            return await asyncify(p.execute)()

    async def _get_all_fields(self, redis_key: str):
        try:
            values = await asyncify(self.redis.hgetall)(redis_key)
        except redis.ResponseError as e:
            raise RedisError(f'Failed to get key {redis_key}. Response error was: {e}')
        return {key: val for key, val in values.items()
                if not str(key).startswith(self._aggregation_prefixes)}

    async def _get_specific_fields(self, redis_key: str, attributes: List[str]):
        non_aggregation_attrs = [
            name for name in attributes
            if not name.startswith(self._aggregation_prefixes)
        ]
        try:
            values = await asyncify(self.redis.hmget)(redis_key, non_aggregation_attrs)
        except redis.ResponseError as e:
            raise RedisError(f'Failed to get key {redis_key}. Response error was: {e}')
        return values

    async def _load_by_key(self, container, table_path, key, attributes):
        redis_key = self.make_key(table_path, key)
        if attributes == "*":
            values = await self._get_all_fields(redis_key)
        else:
            values = await self._get_specific_fields(redis_key, attributes)
        return values
