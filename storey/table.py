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
import copy
import math
from asyncio import Lock
from typing import List, Optional

from . import utils
from .aggregation_utils import (
    get_all_raw_aggregates,
    get_all_raw_aggregates_with_hidden,
    get_implied_aggregates,
    get_virtual_aggregation_func,
    is_raw_aggregate,
)
from .drivers import Driver
from .dtypes import (
    FieldAggregator,
    FixedWindows,
    FixedWindowType,
    FlowError,
    SlidingWindows,
    _termination_obj,
)
from .utils import _split_path


class Table:
    """Table object, represents a single table in a specific storage.

    :param table_path: Path to the table in the storage.
    :param storage: Storage driver
    :param partitioned_by_key: Whether that data is partitioned by the key or not, based on this indication storage
        drivers can optimize writes. Defaults to True.
    :param flush_interval_secs: How often the cache will be flushed in seconds. None for flush every event.
        Default is 300 (5 minutes)
    :param max_updates_in_flight: Maximum number of concurrent updates.
    """

    def __init__(
        self,
        table_path: str,
        storage: Driver,
        partitioned_by_key: bool = True,
        flush_interval_secs: Optional[int] = 300,
        max_updates_in_flight: int = 8,
    ):
        self._container, self._table_path = _split_path(table_path)
        self._storage = storage
        self._partitioned_by_key = partitioned_by_key
        self._attrs_cache = {}
        self._aggregates = None
        self._schema = None
        self._schema_lock = None
        self._aggregations_read_only = False
        self._use_windows_from_schema = False
        self._q = None
        self._max_updates_in_flight = max_updates_in_flight
        self._pending_by_key = {}
        self._flush_interval_secs = flush_interval_secs
        self._flush_task = None
        self._terminated = False
        self._flush_exception = None
        self._changed_keys = set()
        self._pending_events = []
        self.fixed_window_type = None

    def __str__(self):
        return f"{self._container}/{self._table_path}"

    def supports_aggregations(self):
        return self._storage.supports_aggregations()

    def _clone(self):
        new_table = Table(
            self._table_path,
            self._storage,
            self._partitioned_by_key,
            self._flush_interval_secs,
            self._max_updates_in_flight,
        )
        new_table._container = self._container
        new_table._table_path = self._table_path
        return new_table

    def _get_lock(self, key):
        cache_element = self._attrs_cache.get(key)
        if cache_element is None:
            cache_element = _CacheElement({}, None)
            self._attrs_cache[key] = cache_element
        if cache_element.lock is None:
            cache_element.lock = Lock()
        return cache_element.lock

    def _get_schema_lock(self):
        if self._schema_lock is None:
            self._schema_lock = Lock()
        return self._schema_lock

    def _update_static_attrs(self, key, data):
        attrs = self._get_static_attrs(key)
        if attrs:
            for name, value in data.items():
                attrs[name] = value
        else:
            self._set_static_attrs(key, data)
        self._changed_keys.add(key)

    async def _lazy_load_key_with_aggregates(self, key, timestamp=None):
        if self._flush_exception is not None:
            raise self._flush_exception
        async with self._get_lock(key):
            if self._aggregations_read_only or not self._get_aggregations_attrs(key):
                # Try load from the store, and create a new one only if the key really is new
                (
                    aggregate_initial_data,
                    additional_data,
                ) = await self._storage._load_aggregates_by_key(self._container, self._table_path, key)

                # Create new aggregation element
                await self._add_aggregation_by_key(key, timestamp, aggregate_initial_data)

                if additional_data:
                    # Add additional data to simple cache
                    self._update_static_attrs(key, additional_data)

    async def _get_or_load_static_attributes_by_key(self, key, attributes="*"):
        if self._flush_exception is not None:
            raise self._flush_exception
        self._init_flush_task()
        async with self._get_lock(key):
            attrs = self._get_static_attrs(key)
            if not attrs:
                res = await self._storage._load_by_key(self._container, self._table_path, key, attributes)
                if res:
                    self._set_static_attrs(key, res)
                else:
                    self._set_static_attrs(key, {})
            return self._get_static_attrs(key)

    async def _internal_persist_key(self, key, event_data_to_persist, aggr_by_key=None, additional_data_persist=None):
        async with self._get_lock(key):
            if event_data_to_persist:
                if not additional_data_persist:
                    additional_data_persist = event_data_to_persist
                else:
                    additional_data_persist.update(event_data_to_persist)
            await self._storage._save_key(
                self._container,
                self._table_path,
                key,
                aggr_by_key,
                self._partitioned_by_key,
                additional_data_persist,
            )

    def _set_aggregation_metadata(self, aggregates: List[FieldAggregator], use_windows_from_schema: bool = False):
        self._use_windows_from_schema = use_windows_from_schema
        if self._aggregates:
            self._aggregates = self._aggregates + aggregates
        else:
            self._aggregates = aggregates

    def _init_flush_task(self):
        if not self._flush_task and self._flush_interval_secs:
            self._flush_task = asyncio.get_running_loop().create_task(self._flush_worker())

    async def close(self):
        await self._storage.close()

    async def _aggregate(self, key, event, data, timestamp):
        if self._flush_exception is not None:
            raise self._flush_exception
        if not self._schema:
            await self._load_and_update_schema()
        async with self._get_lock(key):
            cache_item = self._get_aggregations_attrs(key)
            await cache_item.aggregate(data, timestamp)
            self._changed_keys.add(key)
        self._pending_events.append(event)

    async def _get_features(self, key, timestamp):
        if self._flush_exception is not None:
            raise self._flush_exception
        if not self._schema:
            await self._load_and_update_schema()

        attrs = self._get_aggregations_attrs(key)

        if attrs is None:
            return {}

        return attrs.get_features(timestamp)

    def _new_aggregated_store_element(self):
        if self._aggregations_read_only:
            return ReadOnlyAggregatedStoreElement
        return AggregatedStoreElement

    async def _add_aggregation_by_key(self, key, base_timestamp, initial_data):
        if not self._schema:
            await self._load_and_update_schema()
        if self._aggregations_read_only and initial_data is None:
            self._set_aggregations_attrs(key, None)
        else:
            self._set_aggregations_attrs(
                key,
                self._new_aggregated_store_element()(
                    key,
                    self._aggregates,
                    base_timestamp,
                    initial_data,
                    self.fixed_window_type,
                    self._persist,
                ),
            )

    async def _load_and_update_schema(self):
        async with self._get_schema_lock():
            self._schema = await self._storage._load_schema(self._container, self._table_path)

            should_update = True
            if self._schema:
                if self._use_windows_from_schema:
                    for aggr in self._aggregates:
                        schema_aggr = self._schema[aggr.name]
                        window_type = schema_aggr["window_type"]
                        period_secs = str(int(schema_aggr["period_millis"] / 1000)) + "s"
                        if window_type == "SlidingWindow":
                            aggr.windows = SlidingWindows(aggr.windows.windows, period_secs)
                        elif window_type == "FixedWindow":
                            aggr.windows = FixedWindows(aggr.windows.windows)
                            aggr.windows.period_millis = schema_aggr["period_millis"]
                            aggr.windows.total_number_of_buckets = max(
                                int(aggr.windows.max_window_millis / aggr.windows.period_millis), utils.bucketPerWindow
                            )
                        else:
                            raise TypeError(f'"{window_type}" unknown window type')
                should_update = self._validate_schema_fit_aggregations(self._schema)

            if should_update and not self._aggregations_read_only:
                self._schema = await self._save_schema()

    async def _save_schema(self):
        schema = self._aggregates_to_schema()
        if self._schema:
            schema = self._merge_schemas(self._schema, schema)

        await self._storage._save_schema(self._container, self._table_path, schema)
        return schema

    def _merge_schemas(self, old, new):
        for name, schema_aggr in new.items():
            if name not in old:
                old[name] = schema_aggr
            else:
                new_aggregates = get_all_raw_aggregates(schema_aggr["aggregates"])
                old_aggregates = get_all_raw_aggregates(old[name]["aggregates"])
                old[name] = {
                    "period_millis": schema_aggr["period_millis"],
                    "aggregates": list(new_aggregates.union(old_aggregates)),
                }

        return old

    # Validate if schema corresponds to the requested aggregates, and return whether the schema needs to be updated
    def _validate_schema_fit_aggregations(self, schema):
        should_update = False
        for aggr in self._aggregates:
            if aggr.name not in schema:
                if self._aggregations_read_only:
                    raise ValueError(
                        f"Requested aggregate {aggr.name}, does not exist in existing feature store "
                        f"at {self._table_path}"
                    )
                else:
                    should_update = True
                    continue
            schema_aggr = schema[aggr.name]
            if not aggr.windows.period_millis == schema_aggr["period_millis"]:
                raise ValueError(
                    f"Requested period for aggregate {aggr.name} does not match existing period at {self._table_path}. "
                    f"Requested: {aggr.windows.period_millis}, existing: {schema_aggr['period_millis']}"
                )
            requested_raw_aggregates = aggr.get_all_raw_aggregates()
            existing_raw_aggregates = get_all_raw_aggregates(schema_aggr["aggregates"])
            # validate if current feature store contains all aggregates needed for the requested calculations
            if self._aggregations_read_only and not requested_raw_aggregates.issubset(existing_raw_aggregates):
                raise ValueError(
                    f"Requested aggregates for feature {aggr.name} do not match with existing aggregates"
                    f"at {self._table_path}. Requested: {aggr.aggregations}, existing: {schema_aggr['aggregates']}"
                )
            # Check if more raw aggregates are requested, in which case a schema update is required
            if not self._aggregations_read_only and requested_raw_aggregates != existing_raw_aggregates:
                should_update = True

        return should_update

    def _aggregates_to_schema(self):
        schema = {}
        for aggr in self._aggregates:
            if isinstance(aggr.windows, SlidingWindows):
                window_type = "SlidingWindow"
            else:
                window_type = "FixedWindow"
            schema[aggr.name] = {
                "period_millis": aggr.windows.period_millis,
                "aggregates": list(get_all_raw_aggregates(aggr.aggregations)),
                "window_type": window_type,
                "max_window_millis": aggr.windows.max_window_millis,
            }

        return schema

    def _flush_pending(self, key):
        aggregations_attrs = self._get_aggregations_attrs(key)
        if not aggregations_attrs:
            return
        for bucket in aggregations_attrs.aggregation_buckets.values():
            if bucket.should_persist:
                # In case we have pending data that spreads over more then 2 windows discard the old ones.
                bucket.get_and_flush_pending()

    def _get_aggregations_attrs(self, key):
        if key in self._attrs_cache:
            return self._attrs_cache[key].aggregations
        else:
            return None

    def _set_aggregations_attrs(self, key, element):
        if key in self._attrs_cache:
            self._attrs_cache[key].aggregations = element
            self._changed_keys.add(key)
        else:
            self._init_flush_task()
            self._attrs_cache[key] = _CacheElement({}, element)

    def _get_static_attrs(self, key):
        if key in self._attrs_cache:
            return self._attrs_cache[key].static_attrs
        else:
            return None

    def _set_static_attrs(self, key, value):
        if key in self._attrs_cache:
            self._attrs_cache[key].static_attrs = value
            self._changed_keys.add(key)
        else:
            self._attrs_cache[key] = _CacheElement(value, None)

    def _get_keys(self):
        return self._attrs_cache.keys()

    def __setitem__(self, key, value):
        """Sets attribute in table.

        :param key: attribute name
        :param value: attribute value
        """
        self._set_static_attrs(key, value)

    def __getitem__(self, key):
        """Gets attribute from table.

        :param key: attribute to get
        """
        return self._get_static_attrs(key)

    async def _flush_worker(self):
        try:
            while not self._terminated:
                await asyncio.sleep(self._flush_interval_secs)
                for key in self._changed_keys.copy():
                    if key not in self._pending_by_key:
                        await self._persist(_PersistJob(key, None, None))
                        self._changed_keys.discard(key)

        except BaseException as ex:
            if not isinstance(ex, asyncio.CancelledError):
                self._flush_exception = ex

        self._pending_events = []

    async def _persist_worker(self):
        task = None
        received_job_count = 0
        self_sent_jobs = {}
        try:
            while True:
                jobs = self_sent_jobs.pop(received_job_count, None)
                if jobs:
                    task = jobs[0]
                    if len(jobs) > 1:
                        self_sent_jobs[received_job_count] = jobs[1:]
                else:
                    task = await self._q.get()
                    received_job_count += 1
                    if task is _termination_obj:
                        if received_job_count in self_sent_jobs:
                            await self._q.put(_termination_obj)
                            continue
                        for _, pending_event in self._pending_by_key.items():
                            if pending_event.pending and not pending_event.in_flight:
                                for job in pending_event.pending:
                                    resp = await self._internal_persist_key(
                                        job.key,
                                        job.data,
                                        job.aggr_by_key,
                                        job.additional_data_persist,
                                    )
                                    if job.callback:
                                        await job.callback(job.extra_data, resp)
                        break

                job = task[0]
                completed = await task[1]

                for done_job in self._pending_by_key[job.key].in_flight:
                    if done_job.callback:
                        await done_job.callback(done_job.extra_data, completed)
                self._pending_by_key[job.key].in_flight = []

                # If we got more pending events for the same key process them
                if self._pending_by_key[job.key].pending:
                    self._pending_by_key[job.key].in_flight = self._pending_by_key[job.key].pending
                    self._pending_by_key[job.key].pending = []

                    future_task = self._safe_process_events(self._pending_by_key[job.key].in_flight)
                    tail_position = received_job_count + self._q.qsize()
                    jobs_at_tail = self_sent_jobs.get(tail_position, [])
                    jobs_at_tail.append((job, asyncio.get_running_loop().create_task(future_task)))
                    self_sent_jobs[tail_position] = jobs_at_tail
                else:
                    del self._pending_by_key[job.key]
        except BaseException as ex:
            if task and task is not _termination_obj:
                if task[0].extra_data and task[0].extra_data._awaitable_result:
                    task[0].extra_data._awaitable_result._set_error(ex)
            if not self._q.empty():
                await self._q.get()
            raise ex

    async def _terminate(self):
        if self._flush_task:
            self._flush_task.cancel()
            if self._flush_exception is not None:
                raise self._flush_exception
        if not self._terminated:
            self._terminated = True
            for key in self._changed_keys.copy():
                if key not in self._pending_by_key:
                    await self._persist(_PersistJob(key, None, None), from_terminate=True)
            if self._q:
                # in case there was no _persist for this table, q and worker_awaitable were never created
                await self._q.put(_termination_obj)
                await self._worker_awaitable
            for value in self._attrs_cache.values():
                value.lock = None
            self._q = None
            self._flush_task = None
            self._flush_exception = None

    async def _persist(self, job, from_terminate=False):
        if not self._flush_interval_secs:
            job.additional_data_persist = self._get_static_attrs(job.key)
            job.aggr_by_key = self._get_aggregations_attrs(job.key)
        else:
            job.save_additional_data_from_table(self)
            self._flush_pending(job.key)

        if self._flush_exception is not None:
            raise self._flush_exception
        if not self._q:
            self._q = asyncio.queues.Queue(self._max_updates_in_flight)
            self._worker_awaitable = asyncio.get_running_loop().create_task(self._persist_worker())
            if not from_terminate:
                # for flow reuse
                self._terminated = False

        if self._worker_awaitable.done():
            await self._worker_awaitable
            raise FlowError("Persist worker has already terminated")
        else:
            # Initializing the key with 2 lists. One for pending requests and one for
            # requests that an update request has been issued for.
            if job.key not in self._pending_by_key:
                self._pending_by_key[job.key] = _PendingEvent()

            # If there is a current update in flight for the key, add the event to
            # the pending list. Otherwise update the key.
            self._pending_by_key[job.key].pending.append(job)
            if len(self._pending_by_key[job.key].in_flight) == 0:
                self._pending_by_key[job.key].in_flight = self._pending_by_key[job.key].pending
                self._pending_by_key[job.key].pending = []
                task = self._safe_process_events(self._pending_by_key[job.key].in_flight)
                await self._q.put((job, asyncio.get_running_loop().create_task(task)))
                if self._worker_awaitable.done():
                    await self._worker_awaitable

    async def _safe_process_events(self, jobs):
        try:
            # TODO using only last event might not work correctly if
            #  there are different non aggregation attrs in each event
            job = jobs[-1]
            return await self._internal_persist_key(job.key, job.data, job.aggr_by_key, job.additional_data_persist)
        except BaseException as ex:
            for job in jobs:
                if job.extra_data and job.extra_data._awaitable_result:
                    none_or_coroutine = job.extra_data._awaitable_result._set_error(ex)
                    if none_or_coroutine:
                        await none_or_coroutine
            raise ex


class _CacheElement:
    def __init__(self, static_attrs, aggregations):
        self.static_attrs = static_attrs
        self.aggregations = aggregations
        self.lock = None


class ReadOnlyAggregatedStoreElement:
    def __init__(
        self,
        key,
        aggregates,
        base_time,
        initial_data=None,
        options=None,
        persist_func=None,
    ):
        self.aggregation_buckets = {}
        self.key = key
        self.aggregates = aggregates
        self.storage_specific_cache = {}
        self.options = options

        # Add all raw aggregates, including aggregates not explicitly requested.
        windows = {}
        for aggregation_metadata in aggregates:
            for meta in aggregation_metadata.aggregations:
                for aggr, is_hidden in get_all_raw_aggregates_with_hidden([meta]).items():
                    if (
                        aggregation_metadata.name,
                        aggr,
                        aggregation_metadata.max_value,
                    ) in windows:
                        aggr_windows = windows[
                            (
                                aggregation_metadata.name,
                                aggr,
                                aggregation_metadata.max_value,
                            )
                        ]
                        if is_hidden in aggr_windows:
                            aggr_windows[is_hidden].merge(aggregation_metadata.windows)
                        else:
                            aggr_windows[is_hidden] = copy.deepcopy(aggregation_metadata.windows)
                    else:
                        windows[
                            (
                                aggregation_metadata.name,
                                aggr,
                                aggregation_metadata.max_value,
                            )
                        ] = {is_hidden: copy.deepcopy(aggregation_metadata.windows)}

        for (name, aggr, max_value), calculated_windows in windows.items():
            column_name = f"{name}_{aggr}"
            initial_column_data = None
            if initial_data and column_name in initial_data:
                initial_column_data = initial_data[column_name]
            explicit_windows = None
            hidden_windows = None
            if False in calculated_windows:
                explicit_windows = calculated_windows[False]
            if True in calculated_windows:
                hidden_windows = calculated_windows[True]
            self.aggregation_buckets[column_name] = ReadOnlyAggregationBuckets(
                name,
                aggr,
                explicit_windows,
                hidden_windows,
                base_time,
                max_value,
                initial_column_data,
                self.options,
            )

        # Add all virtual aggregates
        for aggregation_metadata in aggregates:
            for aggr in aggregation_metadata.aggregations:
                if not is_raw_aggregate(aggr):
                    dependant_aggregate_names = get_implied_aggregates(aggr)
                    dependant_buckets = []
                    for dep in dependant_aggregate_names:
                        dependant_buckets.append(self.aggregation_buckets[f"{aggregation_metadata.name}_{dep}"])
                    self.aggregation_buckets[f"{aggregation_metadata.name}_{aggr}"] = VirtualAggregationBuckets(
                        aggregation_metadata.name,
                        aggr,
                        aggregation_metadata.windows,
                        base_time,
                        dependant_buckets,
                    )

    def aggregate(self, data, timestamp):
        # add a new point and aggregate
        for aggregation_metadata in self.aggregates:
            if aggregation_metadata.should_aggregate(data):
                curr_value = aggregation_metadata.value_extractor(data)
                if curr_value is None:
                    continue
                for aggr in aggregation_metadata.get_all_raw_aggregates():
                    self.aggregation_buckets[f"{aggregation_metadata.name}_{aggr}"].aggregate(timestamp, curr_value)

    def get_features(self, timestamp):
        result = {}
        for aggregation_bucket in self.aggregation_buckets.values():
            if isinstance(aggregation_bucket, VirtualAggregationBuckets) or aggregation_bucket.explicit_windows:
                result.update(aggregation_bucket.get_features(timestamp))

        return result


class ReadOnlyAggregationBuckets:
    def __init__(
        self,
        name,
        aggregation,
        explicit_windows,
        hidden_windows,
        base_time,
        max_value,
        initial_data=None,
        fixed_window_type=None,
    ):
        self.name = name
        self.aggregation = aggregation
        self.explicit_windows = explicit_windows
        self.hidden_windows = hidden_windows
        self.max_value = max_value
        self.buckets = []
        self.should_persist = True
        self.pending_aggr = {}
        self.storage_specific_cache = {}
        self.max_window_millis = self.get_max_window_millis()
        self.total_number_of_buckets = self.get_total_number_of_buckets()

        self._need_to_recalculate_pre_aggregates = False
        self._last_data_point_timestamp = base_time
        self._current_aggregate_values = {}

        if explicit_windows and hidden_windows:
            if type(explicit_windows) != type(hidden_windows):
                raise TypeError("explicit_windows type must match hidden_windows type")

        # If a user specified a max_value we need to recalculated features on every event
        self._precalculated_aggregations = max_value is None
        if explicit_windows:
            self.is_fixed_window = isinstance(self.explicit_windows, FixedWindows)
            if self.is_fixed_window:
                self._round_time_func = self.explicit_windows.round_up_time_to_window
                self.fixed_window_type = fixed_window_type
            self.period_millis = explicit_windows.period_millis
            self._window_start_time = explicit_windows.get_window_start_time_by_time(base_time)
            if self._precalculated_aggregations:
                for win in explicit_windows.windows:
                    self._current_aggregate_values[win] = AggregationValue.new_from_name(aggregation, self.max_value)
        if hidden_windows:
            if not explicit_windows:
                self.is_fixed_window = isinstance(self.hidden_windows, FixedWindows)
                if self.is_fixed_window:
                    self._round_time_func = self.hidden_windows.round_up_time_to_window
                    self.fixed_window_type = fixed_window_type
                self.period_millis = hidden_windows.period_millis
                self._window_start_time = hidden_windows.get_window_start_time_by_time(base_time)
            if self._precalculated_aggregations:
                for win in hidden_windows.windows:
                    if win not in self._current_aggregate_values:
                        self._current_aggregate_values[win] = AggregationValue.new_from_name(
                            aggregation, self.max_value
                        )

        if initial_data:
            self.last_bucket_start_time = None

            # Initializing the buckets from the stored data and calculating the initial pre aggregates
            self.initialize_from_data(initial_data, base_time)
            all_windows = []
            if self.explicit_windows:
                all_windows.extend(self.explicit_windows.windows)
            if self.hidden_windows:
                for win in self.hidden_windows.windows:
                    if win not in all_windows:
                        all_windows.append(win)
            self._need_to_recalculate_pre_aggregates = True
            self.calculate_features(base_time, all_windows)
        else:
            self.first_bucket_start_time = self._window_start_time
            self.last_bucket_start_time = (
                self.first_bucket_start_time + (self.total_number_of_buckets - 1) * self.period_millis
            )

            self.initialize_column()

    def initialize_column(self):
        self.buckets = []

        for _ in range(self.total_number_of_buckets):
            self.buckets.append(self.new_aggregation_value())

    def get_or_advance_bucket_index_by_timestamp(self, timestamp):
        if timestamp < self.last_bucket_start_time + self.period_millis:
            bucket_index = int((timestamp - self.first_bucket_start_time) / self.period_millis)

            if bucket_index > self.get_bucket_index_by_timestamp(self._last_data_point_timestamp):
                self.remove_old_values_from_pre_aggregations(timestamp)
            return bucket_index
        else:
            self.advance_window_period(timestamp)
            return self.total_number_of_buckets - 1  # return last index

    #  Get the index of the bucket corresponding to the requested timestamp
    #  Note: This method can return indexes outside the 'buckets' array
    def get_bucket_index_by_timestamp(self, timestamp):
        bucket_index = int((timestamp - self.first_bucket_start_time) / self.period_millis)
        return bucket_index

    def get_nearest_window_index_by_timestamp(self, timestamp, window_millis):
        bucket_index = int((timestamp - self.first_bucket_start_time) / window_millis)
        return bucket_index

    def remove_old_values_from_pre_aggregations(self, timestamp):
        if self._precalculated_aggregations:
            for win, aggr in self._current_aggregate_values.items():
                current_window_millis = win[0]
                previous_window_start, _ = self.get_window_range(self._last_data_point_timestamp, current_window_millis)
                current_window_start, _ = self.get_window_range(timestamp, current_window_millis)

                previous_window_start = max(0, previous_window_start)
                current_window_start = max(0, current_window_start)
                previous_window_start = min(len(self.buckets) - 1, previous_window_start)
                current_window_start = min(len(self.buckets), current_window_start)

                for bucket_id in range(previous_window_start, current_window_start):
                    current_pre_aggregated_value = aggr.value
                    bucket_aggregated_value = self.buckets[bucket_id].value
                    if (
                        self.aggregation == "min"
                        or self.aggregation == "max"
                        or self.aggregation == "first"
                        or self.aggregation == "last"
                    ):
                        if current_pre_aggregated_value == bucket_aggregated_value:
                            self._need_to_recalculate_pre_aggregates = True
                            return
                    else:
                        aggr._set_value(current_pre_aggregated_value - bucket_aggregated_value)

    def advance_window_period(self, advance_to):
        desired_bucket_index = int((advance_to - self.first_bucket_start_time) / self.period_millis)
        buckets_to_advance = desired_bucket_index - (self.total_number_of_buckets - 1)

        if buckets_to_advance > 0:
            if buckets_to_advance > self.total_number_of_buckets:
                self.initialize_column()
                self._need_to_recalculate_pre_aggregates = True
            else:
                # Updating the pre aggregated data per window
                self.remove_old_values_from_pre_aggregations(advance_to)
                buckets_to_reuse = self.buckets[:buckets_to_advance]
                self.buckets = self.buckets[buckets_to_advance:]
                for bucket_to_reuse in buckets_to_reuse:
                    bucket_to_reuse.reset()
                    self.buckets.append(buckets_to_reuse)

            self.first_bucket_start_time = self.first_bucket_start_time + buckets_to_advance * self.period_millis
            self.last_bucket_start_time = self.last_bucket_start_time + buckets_to_advance * self.period_millis

    def get_window_range(self, timestamp, windows_millis):
        if self.is_fixed_window:
            end_bucket = self.get_bucket_index_by_timestamp(self._round_time_func(timestamp) - 1)
        else:
            end_bucket = self.get_bucket_index_by_timestamp(timestamp)

        num_of_buckets_in_window = int(windows_millis / self.period_millis)
        return end_bucket - num_of_buckets_in_window + 1, end_bucket

    def aggregate(self, timestamp, value):
        index = self.get_or_advance_bucket_index_by_timestamp(timestamp)

        # Only aggregate points that are in range
        if index >= 0:
            self.buckets[index].aggregate(timestamp, value)
            self.add_to_pending(timestamp, value)

            if self._precalculated_aggregations:
                for win, aggr in self._current_aggregate_values.items():
                    current_window_millis = win[0]
                    start, _ = self.get_window_range(self._last_data_point_timestamp, current_window_millis)

                    if timestamp > self._last_data_point_timestamp or index >= start:
                        aggr.aggregate(timestamp, value)
                if timestamp > self._last_data_point_timestamp:
                    self._last_data_point_timestamp = timestamp

    def add_to_pending(self, timestamp, value):
        bucket_start_time = int(timestamp / self.period_millis) * self.period_millis
        if bucket_start_time not in self.pending_aggr:
            self.pending_aggr[bucket_start_time] = self.new_aggregation_value()

        self.pending_aggr[bucket_start_time].aggregate(timestamp, value)

    def new_aggregation_value(self):
        return AggregationValue.new_from_name(self.aggregation, self.max_value)

    def get_aggregation_for_aggregation(self):
        if self.aggregation == "count" or self.aggregation == "sqr":
            return "sum"
        return self.aggregation

    def get_features(self, timestamp, windows=None):
        result = {}
        if not windows:
            if self.explicit_windows:
                windows = self.explicit_windows.windows
            else:
                return result
        # In case we need to completely recalculate the aggregations
        # Either a) we were signaled b) the requested timestamp is prior to our pre aggregates
        if (
            self._need_to_recalculate_pre_aggregates
            or self.get_bucket_index_by_timestamp(timestamp)
            < self.get_bucket_index_by_timestamp(self._last_data_point_timestamp)
            or not self._precalculated_aggregations
        ):
            return self.calculate_features(timestamp, windows)

        # In case our pre aggregates already have the answer
        for win in windows:
            result[f"{self.name}_{self.aggregation}_{win[1]}"] = self._current_aggregate_values[win].value

        return result

    def calculate_features(self, timestamp, windows):
        result = {}

        current_time_bucket_index = self.get_bucket_index_by_timestamp(timestamp)
        if current_time_bucket_index < 0:
            self._need_to_recalculate_pre_aggregates = False
            return result

        aggregated_value = AggregationValue.new_from_name(self.get_aggregation_for_aggregation(), self.max_value)
        prev_windows_millis = 0
        for win in windows:
            window_string = win[1]
            window_millis = win[0]

            if self.is_fixed_window:
                aggregated_value.reset()
                first_bucket_start_time = self.buckets[0].time
                current_time_bucket_index = int((timestamp - first_bucket_start_time) / self.period_millis)
                window_indexes = int(window_millis / self.period_millis)
                start_index = int(current_time_bucket_index / window_indexes) * window_indexes if window_indexes else 0
                last_index = start_index + window_indexes - 1
                if self.fixed_window_type == FixedWindowType.LastClosedWindow:
                    last_index -= window_indexes
                    start_index -= window_indexes

                for bucket_index in range(start_index, last_index + 1):
                    bucket = self.buckets[bucket_index]
                    aggregated_value.aggregate(bucket.time, bucket.value)
            else:
                # In case the current bucket is outside our time range just create a feature with the current aggregated
                # value
                if current_time_bucket_index < 0:
                    result[f"{self.name}_{self.aggregation}_{window_string}"] = aggregated_value.value

                number_of_buckets_backwards = int((window_millis - prev_windows_millis) / self.period_millis)
                last_bucket_to_aggregate = current_time_bucket_index - number_of_buckets_backwards + 1

                if last_bucket_to_aggregate < 0:
                    last_bucket_to_aggregate = 0

                for bucket_index in range(current_time_bucket_index, last_bucket_to_aggregate - 1, -1):
                    if bucket_index < len(self.buckets):
                        bucket = self.buckets[bucket_index]
                        aggregated_value.aggregate(bucket.time, bucket.value)

                # advance the time bucket, so that next iteration won't calculate the same buckets again
                current_time_bucket_index = last_bucket_to_aggregate - 1

            current_aggregations_value = aggregated_value.value

            # create a feature for the current time window
            result[f"{self.name}_{self.aggregation}_{window_string}"] = current_aggregations_value
            prev_windows_millis = window_millis

            # Update the corresponding pre aggregate
            if self._precalculated_aggregations and self._need_to_recalculate_pre_aggregates:
                new_aggr = AggregationValue.new_from_name(
                    self.aggregation,
                    self.max_value,
                    set_data=current_aggregations_value,
                )
                new_aggr.time = aggregated_value.time
                self._current_aggregate_values[win] = new_aggr
        self._need_to_recalculate_pre_aggregates = False
        return result

    def _initialize_from_data_for_sliding_window(
        self, base_time, aggregation_bucket_initial_data, first_time, last_time
    ):
        period = self.period_millis
        self.buckets = [None] * self.total_number_of_buckets

        self.last_bucket_start_time = self._window_start_time
        self.first_bucket_start_time = self.last_bucket_start_time - (self.total_number_of_buckets - 1) * period

        bucket_index = self.total_number_of_buckets - 1
        start_index = int((base_time - last_time) / period)

        # In case base_time is newer than what is stored in the storage initialize
        # the buckets until reaching the stored data
        if start_index >= len(aggregation_bucket_initial_data[last_time]):
            # If the requested data is so new that the stored data is obsolete just
            # initialize the buckets regardless of the stored data.
            if start_index >= len(aggregation_bucket_initial_data[last_time]) + self.total_number_of_buckets:
                self.initialize_column()
                return
            for _ in range(start_index, len(aggregation_bucket_initial_data[last_time]) - 1, -1):
                if bucket_index < 0:
                    return
                self.buckets[bucket_index] = self.new_aggregation_value()
                bucket_index = bucket_index - 1
            start_index = len(aggregation_bucket_initial_data[last_time]) - 1

        # Initializing the buckets based in the stored data starting with the latest bucket
        for i in range(start_index, -1, -1):
            if bucket_index < 0:
                return
            curr_value = aggregation_bucket_initial_data[last_time][i]
            curr_time = last_time + period * i
            self.buckets[bucket_index] = AggregationValue.new_from_name(
                self.aggregation, self.max_value, curr_value, curr_time
            )
            bucket_index = bucket_index - 1

        # In case we still haven't finished initializing all buckets and there is
        # another stored bucket, initialize from there
        if first_time and bucket_index >= 0 and base_time > first_time:
            for i in range(len(aggregation_bucket_initial_data[first_time]) - 1, -1, -1):
                curr_value = aggregation_bucket_initial_data[first_time][i]
                curr_time = first_time + period * i
                self.buckets[bucket_index] = AggregationValue.new_from_name(
                    self.aggregation, self.max_value, curr_value, curr_time
                )
                bucket_index = bucket_index - 1

                if bucket_index < 0:
                    return

        # Initialize every remaining buckets
        for i in range(bucket_index + 1):
            self.buckets[i] = self.new_aggregation_value()

    def _initialize_from_data_for_fixed_window(self, base_time, aggregation_bucket_initial_data, first_time, last_time):
        period = self.period_millis
        window_millis = self.total_number_of_buckets * self.period_millis

        fix_first_time = False
        # there is big gap between recorded emits so set the first_time to previous window
        if first_time < last_time - window_millis:
            first_time = last_time - window_millis
            fix_first_time = True

        next_time = last_time + len(aggregation_bucket_initial_data[last_time]) * period

        if self.fixed_window_type == FixedWindowType.LastClosedWindow:
            self.first_bucket_start_time = int(base_time / window_millis) * window_millis - window_millis
            self.last_bucket_start_time = self.first_bucket_start_time + 2 * window_millis - period
        elif self.fixed_window_type == FixedWindowType.CurrentOpenWindow:
            self.first_bucket_start_time = int(base_time / window_millis) * window_millis
            self.last_bucket_start_time = self.first_bucket_start_time + window_millis - period

        buckets = int((self.last_bucket_start_time - self.first_bucket_start_time) / period) + 1
        self.buckets = [None] * buckets

        curr_time = self.first_bucket_start_time
        for bucket_index in range(buckets):
            if first_time <= curr_time < last_time:
                if fix_first_time:
                    self.buckets[bucket_index] = AggregationValue.new_from_name(
                        self.aggregation, self.max_value, set_time=curr_time
                    )
                else:
                    i = int((curr_time - first_time) / period)
                    curr_value = aggregation_bucket_initial_data[first_time][i]
                    self.buckets[bucket_index] = AggregationValue.new_from_name(
                        self.aggregation, self.max_value, curr_value, curr_time
                    )
            elif last_time <= curr_time < next_time:
                i = int((curr_time - last_time) / period)
                curr_value = aggregation_bucket_initial_data[last_time][i]
                self.buckets[bucket_index] = AggregationValue.new_from_name(
                    self.aggregation, self.max_value, curr_value, curr_time
                )
            else:
                self.buckets[bucket_index] = AggregationValue.new_from_name(
                    self.aggregation, self.max_value, set_time=curr_time
                )
            curr_time = curr_time + period

    def initialize_from_data(self, data, base_time):
        aggregation_bucket_initial_data = {}

        for key, value in data.items():
            if isinstance(key, int):
                aggregation_bucket_initial_data[key] = value
            else:
                self.storage_specific_cache[key] = value

        t0 = next(iter(aggregation_bucket_initial_data))
        first_time, last_time = t0, t0
        if len(aggregation_bucket_initial_data.keys()) == 2:
            timestamp1, timestamp2 = aggregation_bucket_initial_data.keys()
            first_time, last_time = min(timestamp1, timestamp2), max(timestamp1, timestamp2)

        if self.is_fixed_window:
            return self._initialize_from_data_for_fixed_window(
                base_time, aggregation_bucket_initial_data, first_time, last_time
            )
        else:
            return self._initialize_from_data_for_sliding_window(
                base_time, aggregation_bucket_initial_data, first_time, last_time
            )

    def get_and_flush_pending(self):
        pending = self.pending_aggr
        self.pending_aggr = {}
        return pending

    def get_max_window_millis(self):
        max_window = 0
        if self.explicit_windows:
            max_window = self.explicit_windows.max_window_millis
        if self.hidden_windows:
            max_window = max(max_window, self.hidden_windows.max_window_millis)
        return max_window

    def get_total_number_of_buckets(self):
        number_of_buckets = 0
        if self.explicit_windows:
            number_of_buckets = self.explicit_windows.total_number_of_buckets
        if self.hidden_windows:
            number_of_buckets = max(number_of_buckets, self.hidden_windows.total_number_of_buckets)
        return number_of_buckets


class VirtualAggregationBuckets:
    def __init__(self, name, aggregation, window, base_time, args):
        self.name = name
        self.args = args
        self.aggregation = aggregation
        self.aggregation_func = get_virtual_aggregation_func(aggregation)
        self.window = window
        self.is_hidden = False
        self.should_persist = False

        self.first_bucket_start_time = self.window.get_window_start_time_by_time(base_time)
        self.last_bucket_start_time = (
            self.first_bucket_start_time + (window.total_number_of_buckets - 1) * window.period_millis
        )

    def aggregate(self, timestamp, value):
        pass

    def get_features(self, timestamp):
        result = {}

        args_results = [list(bucket.get_features(timestamp, self.window.windows).values()) for bucket in self.args]

        for i in range(len(args_results[0])):
            window_string = self.window.windows[i][1]
            current_args = []
            for window_result in args_results:
                current_args.append(window_result[i])

            result[f"{self.name}_{self.aggregation}_{window_string}"] = self.aggregation_func(current_args)
        return result


class AggregationValue:
    default_value = math.nan

    def __init__(self, max_value=None, set_data=None, set_time=None):
        self.value = self.default_value
        self.time = set_time
        self._max_value = max_value
        self._set_value = self._set_value_with_max if max_value else self._set_value_without_max

        # In case we initialize the object from v3io data
        if set_data is not None:
            self.value = set_data

    def aggregate(self, time, value):
        raise NotImplementedError()

    def aggregate_lua_script(self, vl1, vl2):
        """
        The aggregate_lua_script() method is used for creating part of a lua script, to be sent
        to the Redis DB (see _build_feature_store_lua_update_script).
        The math function (for example Sqr: x*x, increment: x+1) is implemented in the aggregate method.
        """

        raise NotImplementedError()

    @staticmethod
    def new_from_name(aggregation, max_value=None, set_data=None, set_time=None):
        if aggregation == "min":
            return MinValue(max_value, set_data, set_time)
        elif aggregation == "max":
            return MaxValue(max_value, set_data, set_time)
        elif aggregation == "sum":
            return SumValue(max_value, set_data, set_time)
        elif aggregation == "count":
            return CountValue(max_value, set_data, set_time)
        elif aggregation == "sqr":
            return SqrValue(max_value, set_data, set_time)
        elif aggregation == "last":
            return LastValue(max_value, set_data, set_time)
        elif aggregation == "first":
            return FirstValue(max_value, set_data, set_time)

    def _set_value_with_max(self, value):
        if value > self._max_value:
            self.value = self._max_value
        else:
            self.value = float(value)

    def _set_value_without_max(self, value):
        self.value = float(value)

    def get_update_expression(self, old):
        return f"{old}+{self.value}"

    def reset(self, value=None):
        self.time = None
        if value is None:
            self.value = self.default_value
        else:
            self.value = float(value)


class MinValue(AggregationValue):
    name = "min"
    default_value = float("inf")

    def __init__(self, max_value=None, set_data=None, set_time=None):
        super().__init__(max_value, set_data, set_time)

    def aggregate(self, time, value):
        if value < self.value:
            self.value = float(value)  # bypass _set_value because there's no need to check max_value each time

    def get_update_expression(self, old):
        return f"min({old}, {self.value})"

    def reset(self, value=None):
        if value is None:
            self.value = self._max_value or self.default_value
        else:
            self.value = float(value)

    def aggregate_lua_script(self, vl1, vl2):
        return f'type({vl1}) == "number" and math.min({vl1},{vl2}) or {vl2}'


class MaxValue(AggregationValue):
    name = "max"
    default_value = float("-inf")

    def __init__(self, max_value=None, set_data=None, set_time=None):
        super().__init__(max_value, set_data, set_time)

    def aggregate(self, time, value):
        if value > self.value:
            self._set_value(value)

    def get_update_expression(self, old):
        return f"max({old}, {self.value})"

    def aggregate_lua_script(self, vl1, vl2):
        return f'type({vl1}) == "number" and math.max({vl1},{vl2}) or {vl2}'


class SumValue(AggregationValue):
    name = "sum"
    default_value = 0.0

    def __init__(self, max_value=None, set_data=None, set_time=None):
        super().__init__(max_value, set_data, set_time)

    def aggregate(self, time, value):
        self._set_value(self.value + value)

    def aggregate_lua_script(self, vl1, vl2):
        return f"{vl1}+{vl2}"


class CountValue(AggregationValue):
    aggregation = "count"
    default_value = 0.0

    def __init__(self, max_value=None, set_data=None, set_time=None):
        super().__init__(max_value, set_data, set_time)

    def aggregate(self, time, value):
        self._set_value(self.value + 1)

    def aggregate_lua_script(self, vl1, vl2):
        return f"{vl1}+{vl2}"


class SqrValue(AggregationValue):
    name = "sqr"
    default_value = 0.0

    def __init__(self, max_value=None, set_data=None, set_time=None):
        super().__init__(max_value, set_data, set_time)

    def aggregate(self, time, value):
        self._set_value(self.value + value * value)

    def aggregate_argument(self, time, argument):
        self._set_value(self.value + argument)

    def aggregate_lua_script(self, vl1, vl2):
        return f"{vl1}+{vl2}"


class LastValue(AggregationValue):
    name = "last"

    def __init__(self, max_value=None, set_data=None, set_time=None):
        if set_time is None:
            set_time = -math.inf
        super().__init__(max_value, set_data, set_time)

    def aggregate(self, time, value):
        if time is not None and not math.isnan(value) and (self.time is None or time > self.time):
            self._set_value(value)
            self.time = time

    def get_update_expression(self, old):
        return f"{self.value}"

    def reset(self, value=None):
        self.time = -math.inf
        if value is None:
            self.value = self.default_value
        else:
            self.value = value

    def aggregate_lua_script(self, vl1, vl2):
        return f"{vl2}"


class FirstValue(AggregationValue):
    name = "first"

    def __init__(self, max_value=None, set_data=None, set_time=None):
        if set_time is None:
            set_time = math.inf
        super().__init__(max_value, set_data, set_time)

    def aggregate(self, time, value):
        if time is not None and not math.isnan(value) and (self.time is None or time < self.time):
            if math.isnan(self.value) and math.isnan(self.default_value):
                self._set_value(value)
            self.time = time

    def get_update_expression(self, old):
        return f"if_else(isnan({old}), {self.value}, {old})"

    def reset(self, value=None):
        self.time = math.inf
        if value is None:
            self.value = self.default_value
        else:
            self.value = value

    def aggregate_lua_script(self, vl1, vl2):
        return f"{vl1}~={vl1} and {vl2} or {vl1}"


class AggregatedStoreElement:
    def __init__(
        self,
        key,
        aggregates,
        base_time,
        initial_data=None,
        options=None,
        persist_func=None,
    ):
        self.aggregation_buckets = {}
        self.key = key
        self.aggregates = aggregates
        self.storage_specific_cache = {}
        self.persist_func = persist_func

        # Group init data by feature name
        initial_data_by_feature = {}
        if initial_data:
            for key, value in initial_data.items():
                separator_index = key.rindex("_")
                feature_name = key[:separator_index]
                aggr = key[separator_index + 1 :]
                if feature_name not in initial_data_by_feature:
                    initial_data_by_feature[feature_name] = {}
                initial_data_by_feature[feature_name][aggr] = value

        for aggregation_metadata in aggregates:
            explicit_raw_aggregates = []
            hidden_raw_aggregates = []
            virtual_aggregates = []
            for aggregation in aggregation_metadata.aggregations:
                if is_raw_aggregate(aggregation):
                    explicit_raw_aggregates.append(aggregation)
                else:
                    dependant_aggregate_names = get_implied_aggregates(aggregation)
                    hidden_raw_aggregates.extend(dependant_aggregate_names)
                    virtual_aggregates.append(VirtualAggregation(aggregation, dependant_aggregate_names))
            initial_column_data = None
            if initial_data_by_feature and aggregation_metadata.name in initial_data_by_feature:
                initial_column_data = initial_data_by_feature[aggregation_metadata.name]
            self.aggregation_buckets[aggregation_metadata.name] = AggregationBuckets(
                aggregation_metadata.name,
                explicit_raw_aggregates,
                hidden_raw_aggregates,
                virtual_aggregates,
                aggregation_metadata.windows,
                base_time,
                aggregation_metadata.max_value,
                key,
                initial_column_data,
                persist_func,
            )

    def __deepcopy__(self, memo):  # memo is a dict of id's to copies
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k == "persist_func":
                setattr(result, k, v)
            else:
                setattr(result, k, copy.deepcopy(v, memo))
        return result

    async def aggregate(self, data, timestamp):
        # add a new point and aggregate
        for aggregation_metadata in self.aggregates:
            if aggregation_metadata.should_aggregate(data):
                curr_value = aggregation_metadata.value_extractor(data)
                if curr_value is None:
                    continue
                # for aggr in aggregation_metadata.get_all_raw_aggregates():
                await self.aggregation_buckets[f"{aggregation_metadata.name}"].aggregate(timestamp, curr_value)

    def get_features(self, timestamp):
        result = {}
        for aggregation_bucket in self.aggregation_buckets.values():
            result.update(aggregation_bucket.get_features(timestamp))

        return result


class AggregationBuckets:
    def __init__(
        self,
        name,
        explicit_raw_aggregations,
        hidden_raw_aggregations,
        virtual_aggregations,
        explicit_windows,
        base_time,
        max_value,
        key,
        initial_data=None,
        persist_func=None,
    ):
        self.key = key
        self.name = name
        self._explicit_raw_aggregations = explicit_raw_aggregations
        self._hidden_raw_aggregations = hidden_raw_aggregations
        self._hidden_raw_aggregations.append("count")
        self._all_raw_aggregates = self._explicit_raw_aggregations.copy()
        for hidden_aggr in self._hidden_raw_aggregations:
            if hidden_aggr not in self._all_raw_aggregates:
                self._all_raw_aggregates.append(hidden_aggr)
        self._virtual_aggregations = virtual_aggregations
        self.explicit_windows = explicit_windows
        self.max_value = max_value
        self.buckets = []
        self.should_persist = True
        self.pending_aggr = {}
        self.storage_specific_cache = {}
        self.max_window_millis = self.explicit_windows.max_window_millis
        self.total_number_of_buckets = self.explicit_windows.total_number_of_buckets

        self._need_to_recalculate_pre_aggregates = False
        self._last_data_point_timestamp = base_time
        self._current_aggregate_values = {}
        self._intermediate_aggregation_values = {}
        self.persist_func = persist_func
        for aggregation_name in self._all_raw_aggregates:
            aggregation_value = AggregationValue.new_from_name(
                self.get_aggregation_for_aggregation(aggregation_name), self.max_value
            )
            self._intermediate_aggregation_values[aggregation_name] = aggregation_value

        # If a user specified a max_value we need to recalculated features on every event
        self._precalculated_aggregations = max_value is None

        self.is_fixed_window = isinstance(self.explicit_windows, FixedWindows)
        if self.is_fixed_window:
            self._round_time_func = self.explicit_windows.round_up_time_to_window
        self.period_millis = explicit_windows.period_millis
        self._window_start_time = explicit_windows.get_window_start_time_by_time(base_time)
        if self._precalculated_aggregations:
            for (window_millis, _) in explicit_windows.windows:
                for aggr in self._all_raw_aggregates:
                    self._current_aggregate_values[(aggr, window_millis)] = AggregationValue.new_from_name(
                        aggr, max_value
                    )

        if initial_data:
            self.last_bucket_start_time = None

            # Initializing the buckets from the stored data and calculating the initial pre aggregates
            self.initialize_from_data(initial_data, base_time)
            self._need_to_recalculate_pre_aggregates = True
            self.calculate_features(base_time)
        else:
            self.first_bucket_start_time = self._window_start_time
            self.last_bucket_start_time = (
                self.first_bucket_start_time + (self.total_number_of_buckets - 1) * self.period_millis
            )

            self.initialize_column()

    def __deepcopy__(self, memo):  # memo is a dict of id's to copies
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k == "persist_func" or k == "_round_time_func":
                setattr(result, k, v)
            else:
                setattr(result, k, copy.deepcopy(v, memo))
        return result

    def initialize_column(self):
        self.buckets = []

        for _ in range(self.total_number_of_buckets):
            self.buckets.append(self.new_aggregation_value())

    async def get_or_advance_bucket_index_by_timestamp(self, timestamp):
        if timestamp < self.last_bucket_start_time + self.period_millis:
            bucket_index = int((timestamp - self.first_bucket_start_time) / self.period_millis)

            if bucket_index > self.get_bucket_index_by_timestamp(self._last_data_point_timestamp):
                self.remove_old_values_from_pre_aggregations(timestamp)
            return bucket_index
        else:
            await self.advance_window_period(timestamp)
            return self.get_bucket_index_by_timestamp(timestamp)

    #  Get the index of the bucket corresponding to the requested timestamp
    #  Note: This method can return indexes outside the 'buckets' array
    def get_bucket_index_by_timestamp(self, timestamp):
        return int((timestamp - self.first_bucket_start_time) / self.period_millis)

    def get_nearest_window_index_by_timestamp(self, timestamp, window_millis):
        return int((timestamp - self.first_bucket_start_time) / window_millis)

    def remove_old_values_from_pre_aggregations(self, timestamp):
        if self._precalculated_aggregations:
            for (
                aggr_name,
                current_window_millis,
            ), aggr in self._current_aggregate_values.items():
                if self.is_fixed_window:
                    previous_window_start_time = self.get_window_start_time_from_timestamp(
                        self._last_data_point_timestamp, current_window_millis
                    )
                    previous_window_start = self.get_bucket_index_by_timestamp(previous_window_start_time)
                    current_window_start_time = self.get_window_start_time_from_timestamp(
                        timestamp, current_window_millis
                    )
                    current_window_start = self.get_bucket_index_by_timestamp(current_window_start_time)
                else:
                    previous_window_start = self.get_window_range(
                        self.get_end_bucket(self._last_data_point_timestamp),
                        current_window_millis,
                    )
                    current_window_start = self.get_window_range(self.get_end_bucket(timestamp), current_window_millis)

                    previous_window_start = max(0, previous_window_start)
                    current_window_start = max(0, current_window_start)
                    previous_window_start = min(len(self.buckets) - 1, previous_window_start)
                    current_window_start = min(len(self.buckets), current_window_start)

                for bucket_id in range(previous_window_start, current_window_start):
                    current_pre_aggregated_value = aggr.value
                    if bucket_id >= self.total_number_of_buckets:
                        break
                    bucket_aggregated_value = self.buckets[bucket_id][aggr_name].value
                    if aggr_name == "min" or aggr_name == "max" or aggr_name == "first" or aggr_name == "last":
                        if current_pre_aggregated_value == bucket_aggregated_value:
                            self._need_to_recalculate_pre_aggregates = True
                            return
                    else:
                        aggr._set_value(current_pre_aggregated_value - bucket_aggregated_value)

    async def advance_window_period(self, advance_to):
        desired_bucket_index = int((advance_to - self.first_bucket_start_time) / self.period_millis)
        buckets_to_advance = desired_bucket_index - (self.total_number_of_buckets - 1)
        if self.is_fixed_window:
            buckets_to_advance = max(self.total_number_of_buckets, buckets_to_advance)

        if buckets_to_advance > 0:
            if self.is_fixed_window:
                await self.persist_func(_PersistJob(self.key, None, None))
            if buckets_to_advance > self.total_number_of_buckets:
                self.initialize_column()
                self._need_to_recalculate_pre_aggregates = True
            else:
                # Updating the pre-aggregated data per window
                self.remove_old_values_from_pre_aggregations(advance_to)
                buckets_to_reuse = self.buckets[:buckets_to_advance]
                self.buckets = self.buckets[buckets_to_advance:]
                for bucket_to_reuse in buckets_to_reuse:
                    for _, aggr_value in bucket_to_reuse.items():
                        aggr_value.reset()
                    self.buckets.append(bucket_to_reuse)

            # fixed windows are advancing in integral window size
            if self.is_fixed_window:
                window_millis = self.last_bucket_start_time + self.period_millis - self.first_bucket_start_time
                windows_to_advance = int((advance_to - self.first_bucket_start_time) / window_millis)
                self.first_bucket_start_time = self.first_bucket_start_time + windows_to_advance * window_millis
                self.last_bucket_start_time = self.last_bucket_start_time + windows_to_advance * window_millis
            else:
                self.first_bucket_start_time = self.first_bucket_start_time + buckets_to_advance * self.period_millis
                self.last_bucket_start_time = self.last_bucket_start_time + buckets_to_advance * self.period_millis

    def get_end_bucket(self, timestamp):
        if self.is_fixed_window:
            return self.get_bucket_index_by_timestamp(self._round_time_func(timestamp) - 1)
        else:
            return self.get_bucket_index_by_timestamp(timestamp)

    def get_window_range(self, end_bucket, windows_millis):
        num_of_buckets_in_window = int(windows_millis / self.period_millis)
        return end_bucket - num_of_buckets_in_window + 1

    def get_window_start_time_from_timestamp(self, timestamp, window_millis):
        return (
            int((timestamp - self.first_bucket_start_time) / window_millis) * window_millis
            + self.first_bucket_start_time
        )

    async def aggregate(self, timestamp, value):
        index = await self.get_or_advance_bucket_index_by_timestamp(timestamp)

        # Only aggregate points that are in range
        if index >= 0:
            for aggr in self.buckets[index].values():
                aggr.aggregate(timestamp, value)
            self.add_to_pending(timestamp, value)

            if self._precalculated_aggregations:
                for (
                    _,
                    current_window_millis,
                ), aggr in self._current_aggregate_values.items():
                    current_window_start_time = self.get_window_start_time_from_timestamp(
                        timestamp, current_window_millis
                    )
                    current_window_start_index = self.get_bucket_index_by_timestamp(current_window_start_time)

                    current_window_end_time = current_window_start_time + current_window_millis
                    current_window_end_index = self.get_bucket_index_by_timestamp(current_window_end_time) - 1

                    if timestamp >= self._last_data_point_timestamp and index in range(
                        current_window_start_index, current_window_end_index + 1
                    ):
                        aggr.aggregate(timestamp, value)
                if timestamp > self._last_data_point_timestamp:
                    self._last_data_point_timestamp = timestamp

    def add_to_pending(self, timestamp, value):
        bucket_start_time = int(timestamp / self.period_millis) * self.period_millis
        if bucket_start_time not in self.pending_aggr:
            self.pending_aggr[bucket_start_time] = self.new_aggregation_value()

        for aggr in self.pending_aggr[bucket_start_time].values():
            aggr.aggregate(timestamp, value)

    def new_aggregation_value(self):
        return {
            aggr_name: AggregationValue.new_from_name(aggr_name, self.max_value)
            for aggr_name in self._all_raw_aggregates
        }

    def get_aggregation_for_aggregation(self, aggregation):
        if aggregation == "count" or aggregation == "sqr":
            return "sum"
        return aggregation

    def get_features(self, timestamp):
        result = {}

        # In case we need to completely recalculate the aggregations
        # Either a) we were signaled b) the requested timestamp is prior to our pre aggregates
        current_time_bucket_index = self.get_bucket_index_by_timestamp(timestamp)
        if current_time_bucket_index < 0:
            self._need_to_recalculate_pre_aggregates = False
            return result

        if (
            self._need_to_recalculate_pre_aggregates
            or current_time_bucket_index < self.get_bucket_index_by_timestamp(self._last_data_point_timestamp)
            or not self._precalculated_aggregations
        ):
            result = self.calculate_features(timestamp)
        else:
            # In case our pre aggregates already have the answer
            for aggregation_name in self._explicit_raw_aggregations:
                for (window_millis, window_str) in self.explicit_windows.windows:
                    value = self._current_aggregate_values[(aggregation_name, window_millis)].value
                    count_value = self._current_aggregate_values[("count", window_millis)].value
                    if value == math.inf or value == -math.inf:
                        value = math.nan
                    if count_value == 0 and aggregation_name != "count":
                        value = math.nan
                    result[f"{self.name}_{aggregation_name}_{window_str}"] = value

        self.augment_virtual_features(result)
        return result

    def augment_virtual_features(self, features):
        if not self._virtual_aggregations:
            return

        args = [None, None, None]  # Avoid in-loop allocation
        for aggregate in self._virtual_aggregations:
            for (window_millis, window_str) in self.explicit_windows.windows:
                for i, aggr in enumerate(aggregate.dependant_aggregates):
                    args[i] = self._current_aggregate_values[(aggr, window_millis)].value
                features[f"{self.name}_{aggregate.name}_{window_str}"] = aggregate.aggregation_func(args)

    def calculate_features(self, timestamp):
        result = {}

        current_time_bucket_index = self.get_bucket_index_by_timestamp(timestamp)
        if current_time_bucket_index < 0:
            self._need_to_recalculate_pre_aggregates = False
            return result

        if self.is_fixed_window:
            current_time_bucket_index = self.get_bucket_index_by_timestamp(self._round_time_func(timestamp) - 1)

        for aggregation_name in self._all_raw_aggregates:
            self._intermediate_aggregation_values[aggregation_name].reset()
        prev_windows_millis = 0
        for (window_millis, window_string) in self.explicit_windows.windows:
            # In case the current bucket is outside our time range just create a feature with the current aggregated
            # value
            if current_time_bucket_index < 0:
                for aggregation_name in self._explicit_raw_aggregations:
                    result[f"{self.name}_{aggregation_name}_{window_string}"] = self._intermediate_aggregation_values[
                        aggregation_name
                    ].value

            number_of_buckets_backwards = int((window_millis - prev_windows_millis) / self.period_millis)
            last_bucket_to_aggregate = current_time_bucket_index - number_of_buckets_backwards + 1

            if last_bucket_to_aggregate < 0:
                last_bucket_to_aggregate = 0

            for bucket_index in range(current_time_bucket_index, last_bucket_to_aggregate - 1, -1):
                if bucket_index < len(self.buckets):
                    for aggregation_name in self._all_raw_aggregates:
                        bucket = self.buckets[bucket_index][aggregation_name]
                        self._intermediate_aggregation_values[aggregation_name].aggregate(bucket.time, bucket.value)

            # create a feature for the current time window
            for aggregation_name in self._explicit_raw_aggregations:
                current_aggregation_value = self._intermediate_aggregation_values[aggregation_name].value
                result[f"{self.name}_{aggregation_name}_{window_string}"] = current_aggregation_value

                if self._precalculated_aggregations and self._need_to_recalculate_pre_aggregates:
                    self._current_aggregate_values[(aggregation_name, window_millis)].reset(
                        value=current_aggregation_value
                    )

            # Update the corresponding pre aggregate
            if self._precalculated_aggregations and self._need_to_recalculate_pre_aggregates:
                for aggregation_name in self._hidden_raw_aggregations:
                    value = self._intermediate_aggregation_values[aggregation_name].value
                    key = (aggregation_name, window_millis)
                    self._current_aggregate_values[key].reset(value=value)

            # advance the time bucket, so that next iteration won't calculate the same buckets again
            current_time_bucket_index = last_bucket_to_aggregate - 1
            prev_windows_millis = window_millis

        self._need_to_recalculate_pre_aggregates = False
        return result

    def initialize_from_data(self, data, base_time):
        period = self.period_millis
        self.buckets = [self.new_aggregation_value() for _ in range(self.total_number_of_buckets)]

        aggregation_bucket_initial_data = {}

        # Assuming all aggregates have the same time so just checking the first
        for key, value in data[next(iter(self._all_raw_aggregates))].items():
            if isinstance(key, int):
                aggregation_bucket_initial_data[key] = value
            else:
                self.storage_specific_cache[key] = value

        first_time, last_time = None, next(iter(aggregation_bucket_initial_data))
        if len(aggregation_bucket_initial_data.keys()) == 2:
            timestamp1, timestamp2 = aggregation_bucket_initial_data.keys()
            first_time, last_time = min(timestamp1, timestamp2), max(timestamp1, timestamp2)

        bucket_index = self.total_number_of_buckets - 1
        self.last_bucket_start_time = self._window_start_time
        self.first_bucket_start_time = self.last_bucket_start_time - (self.total_number_of_buckets - 1) * period

        start_index = int((base_time - last_time) / period)

        # In case base_time is newer than what is stored in the storage initialize
        # the buckets until reaching the stored data
        if start_index >= len(aggregation_bucket_initial_data[last_time]):
            # If the requested data is so new that the stored data is obsolete just
            # initialize the buckets regardless of the stored data.
            if start_index >= len(aggregation_bucket_initial_data[last_time]) + self.total_number_of_buckets:
                self.initialize_column()
                return
            for _ in range(start_index, len(aggregation_bucket_initial_data[last_time]) - 1, -1):
                if bucket_index < 0:
                    return
                for aggregation in self._all_raw_aggregates:
                    self.buckets[bucket_index][aggregation].reset()
                bucket_index = bucket_index - 1
            start_index = len(aggregation_bucket_initial_data[last_time]) - 1

        # Initializing the buckets based in the stored data starting with the latest bucket
        for i in range(start_index, -1, -1):
            if bucket_index < 0:
                return
            for aggregation in self._all_raw_aggregates:
                curr_value = data[aggregation][last_time][i]
                self.buckets[bucket_index][aggregation] = AggregationValue.new_from_name(
                    aggregation, self.max_value, curr_value
                )
            bucket_index = bucket_index - 1

        # In case we still haven't finished initializing all buckets and there is
        # another stored bucket, initialize from there
        if first_time and bucket_index >= 0 and base_time > first_time:
            for i in range(len(aggregation_bucket_initial_data[first_time]) - 1, -1, -1):
                for aggregation in self._all_raw_aggregates:
                    curr_value = data[aggregation][first_time][i]
                    self.buckets[bucket_index][aggregation] = AggregationValue.new_from_name(
                        aggregation, self.max_value, curr_value
                    )
                bucket_index = bucket_index - 1

                if bucket_index < 0:
                    return

        # Initialize every remaining buckets
        for i in range(bucket_index + 1):
            for aggregation in self._all_raw_aggregates:
                self.buckets[i][aggregation] = AggregationValue.new_from_name(aggregation, self.max_value)

    def get_and_flush_pending(self):
        pending = self.pending_aggr
        self.pending_aggr = {}
        return pending


class VirtualAggregation:
    def __init__(self, aggregation, dependant_aggregates):
        self.name = aggregation
        self.dependant_aggregates = dependant_aggregates
        self.aggregation_func = get_virtual_aggregation_func(aggregation)


class _PendingEvent:
    def __init__(self):
        self.in_flight = []
        self.pending = []


class _PersistJob:
    def __init__(self, key, data, callback, extra_data=None):
        self.key = key
        self.data = copy.deepcopy(data)
        self.callback = callback
        self.extra_data = extra_data
        self.aggr_by_key = None
        self.additional_data_persist = None

    def save_additional_data_from_table(self, table):
        self.additional_data_persist = copy.deepcopy(table._get_static_attrs(self.key))
        self.aggr_by_key = copy.deepcopy(table._get_aggregations_attrs(self.key))
