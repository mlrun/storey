import asyncio
import copy
from datetime import datetime
import re
from typing import Optional, Union, Callable, List, Dict

from .aggregation_utils import is_raw_aggregate, get_virtual_aggregation_func, get_implied_aggregates, get_all_raw_aggregates, \
    get_all_raw_aggregates_with_hidden
from .dtypes import EmitEveryEvent, FixedWindows, SlidingWindows, EmitAfterPeriod, EmitAfterWindow, EmitAfterMaxEvent, \
    _dict_to_emit_policy, FieldAggregator
from .table import Table
from .flow import Flow, _termination_obj, Event

_default_emit_policy = EmitEveryEvent()


class AggregateByKey(Flow):
    """
    Aggregates the data into the table object provided for later persistence, and outputs an event enriched with the requested aggregation
    features.
    Persistence is done via the `WriteToTable` step and based on the Cache object persistence settings.

    :param aggregates: List of aggregates to apply for each event.
    :param table: A Table object or name for persistence of aggregations. If a table name is provided, it will be looked up in the context.
    :param key: Key field to aggregate by, accepts either a string representing the key field or a key extracting function.
     Defaults to the key in the event's metadata. (Optional)
    :param emit_policy: Policy indicating when the data will be emitted. Defaults to EmitEveryEvent. (Optional)
    :param augmentation_fn: Function that augments the features into the event's body. Defaults to updating a dict. (Optional)
    :param enrich_with: List of attributes names from the associated storage object to be fetched and added to every event. (Optional)
    :param aliases: Dictionary specifying aliases to the enriched columns, of the format `{'col_name': 'new_col_name'}`. (Optional)
    :param context: Context object that holds global configurations and secrets.
    """

    def __init__(self, aggregates: Union[List[FieldAggregator], List[Dict[str, object]]], table: Union[Table, str],
                 key: Union[str, Callable[[Event], object], None] = None,
                 emit_policy: Union[EmitEveryEvent, FixedWindows, SlidingWindows, EmitAfterPeriod, EmitAfterWindow,
                                    EmitAfterMaxEvent, Dict[str, object]] = _default_emit_policy,
                 augmentation_fn: Optional[Callable[[Event, Dict[str, object]], Event]] = None, enrich_with: Optional[List[str]] = None,
                 aliases: Optional[Dict[str, str]] = None, use_windows_from_schema: bool = False, **kwargs):
        Flow.__init__(self, **kwargs)
        aggregates = self._parse_aggregates(aggregates)
        self._aggregates_store = AggregateStore(aggregates, use_windows_from_schema=use_windows_from_schema)

        self._table = table
        if isinstance(table, str):
            if not self.context:
                raise TypeError("Table can not be string if no context was provided to the step")
            self._table = self.context.get_table(table)
        self._table._set_aggregation_store(self._aggregates_store)
        self._closeables = [self._table]

        self._aggregates_metadata = aggregates

        self._enrich_with = enrich_with or []
        self._aliases = aliases or {}
        self._emit_policy = emit_policy
        if isinstance(self._emit_policy, dict):
            self._emit_policy = _dict_to_emit_policy(self._emit_policy)
        self._events_in_batch = {}
        self._emit_worker_running = False
        self._terminate_worker = False

        self._augmentation_fn = augmentation_fn
        if not augmentation_fn:
            def f(element, features):
                features.update(element)
                return features

            self._augmentation_fn = f

        self.key_extractor = None
        if key:
            if callable(key):
                self.key_extractor = key
            elif isinstance(key, str):
                self.key_extractor = lambda element: element[key]
            else:
                raise TypeError(f'key is expected to be either a callable or string but got {type(key)}')

    @staticmethod
    def _parse_aggregates(aggregates):
        if not isinstance(aggregates, list):
            raise TypeError('aggregates should be a list of FieldAggregator/dictionaries')

        if not aggregates or isinstance(aggregates[0], FieldAggregator):
            return aggregates

        if isinstance(aggregates[0], dict):
            new_aggregates = []
            for aggregate_dict in aggregates:
                if 'period' in aggregate_dict:
                    window = SlidingWindows(aggregate_dict['windows'], aggregate_dict['period'])
                else:
                    window = FixedWindows(aggregate_dict['windows'])
                new_aggregates.append(FieldAggregator(aggregate_dict['name'], aggregate_dict['column'], aggregate_dict['operations'],
                                                      window, aggregate_dict.get('aggregation_filter', None),
                                                      aggregate_dict.get('max_value', None)))
            return new_aggregates

        raise TypeError('aggregates should be a list of FieldAggregator/dictionaries')

    async def _do(self, event):
        if event == _termination_obj:
            self._terminate_worker = True
            return await self._do_downstream(_termination_obj)

        try:
            # check whether a background loop is needed, if so create start one
            if (not self._emit_worker_running) and \
                    (isinstance(self._emit_policy, EmitAfterPeriod) or isinstance(self._emit_policy, EmitAfterWindow)):
                asyncio.get_running_loop().create_task(self._emit_worker())
                self._emit_worker_running = True

            element = event.body
            key = event.key
            if self.key_extractor:
                key = self.key_extractor(element)

            event_timestamp = event.time
            if isinstance(event_timestamp, datetime):
                event_timestamp = event_timestamp.timestamp() * 1000

            await self._table.lazy_load_key_with_aggregates(key, event_timestamp)
            await self._aggregates_store.aggregate(key, element, event_timestamp)

            if isinstance(self._emit_policy, EmitEveryEvent):
                await self._emit_event(key, event)
            elif isinstance(self._emit_policy, EmitAfterMaxEvent):
                self._events_in_batch[key] = self._events_in_batch.get(key, 0) + 1
                if self._events_in_batch[key] == self._emit_policy.max_events:
                    await self._emit_event(key, event)
                    self._events_in_batch[key] = 0
        except Exception as ex:
            raise ex

    # Emit a single event for the requested key
    async def _emit_event(self, key, event):
        event_timestamp = event.time
        if isinstance(event_timestamp, datetime):
            event_timestamp = event_timestamp.timestamp() * 1000

        await self._table.lazy_load_key_with_aggregates(key, event_timestamp)
        features = await self._aggregates_store.get_features(key, event_timestamp)
        features = self._augmentation_fn(event.body, features)

        for col in self._enrich_with:
            emitted_attr_name = self._aliases.get(col, None) or col
            if col in self._table[key]:
                features[emitted_attr_name] = self._table[key][col]
        new_event = copy.copy(event)
        new_event.key = key
        new_event.body = features
        await self._do_downstream(new_event)

    # Emit multiple events for every key in the store with the current time
    async def _emit_all_events(self, timestamp):
        for key in self._aggregates_store.get_keys():
            await self._emit_event(key, Event({'key': key, 'time': timestamp}, key, timestamp, None))

    async def _emit_worker(self):
        if isinstance(self._emit_policy, EmitAfterPeriod):
            seconds_to_sleep_between_emits = self._aggregates_metadata[0].windows.period_millis / 1000
        elif isinstance(self._emit_policy, EmitAfterWindow):
            seconds_to_sleep_between_emits = self._aggregates_metadata[0].windows.windows[0][0] / 1000
        else:
            raise TypeError(f'Emit policy "{type(self._emit_policy)}" is not supported')

        current_time = datetime.now().timestamp()
        next_emit_time = int(
            current_time / seconds_to_sleep_between_emits) * seconds_to_sleep_between_emits + seconds_to_sleep_between_emits

        while not self._terminate_worker:
            current_time = datetime.now().timestamp()
            next_sleep_interval = next_emit_time - current_time + self._emit_policy.delay_in_seconds
            if next_sleep_interval > 0:
                await asyncio.sleep(next_sleep_interval)
            await self._emit_all_events(next_emit_time * 1000)
            next_emit_time = next_emit_time + seconds_to_sleep_between_emits


class QueryByKey(AggregateByKey):
    """
    Query features by name

    :param features: List of features to get.
    :param table: A Table object or name for persistence of aggregations. If a table name is provided, it will be looked up in the context.
    :param key: Key field to aggregate by, accepts either a string representing the key field or a key extracting function.
     Defaults to the key in the event's metadata. (Optional)
    :param augmentation_fn: Function that augments the features into the event's body. Defaults to updating a dict. (Optional)
    :param aliases: Dictionary specifying aliases to the enriched columns, of the format `{'col_name': 'new_col_name'}`. (Optional)
    :param context: Context object that holds global configurations and secrets.
    """

    def __init__(self, features: List[str], table: Union[Table, str], key: Union[str, Callable[[Event], object], None] = None,
                 augmentation_fn: Optional[Callable[[Event, Dict[str, object]], Event]] = None,
                 aliases: Optional[Dict[str, str]] = None, **kwargs):
        self._aggrs = []
        self._enrich_cols = []
        resolved_aggrs = {}
        for feature in features:
            if re.match(r".*_[a-z]+_[0-9]+[smhd]", feature):
                name, window = feature.rsplit('_', 1)
                if name in resolved_aggrs:
                    resolved_aggrs[name].append(window)
                else:
                    resolved_aggrs[name] = [window]
            else:
                self._enrich_cols.append(feature)
        for name, windows in resolved_aggrs.items():
            feature, aggr = name.rsplit('_', 1)
            # setting as SlidingWindow temporarily until actual window type will be read from schema
            self._aggrs.append(FieldAggregator(name=feature, field=None, aggr=[aggr], windows=SlidingWindows(windows, '10m')))

        AggregateByKey.__init__(self, self._aggrs, table, key, augmentation_fn=augmentation_fn,
                                enrich_with=self._enrich_cols, aliases=aliases, use_windows_from_schema=True, **kwargs)
        self._aggregates_store._read_only = True

    async def _do(self, event):
        if event == _termination_obj:
            self._terminate_worker = True
            return await self._do_downstream(_termination_obj)

        try:
            element = event.body
            key = event.key
            if self.key_extractor:
                key = self.key_extractor(element)
            await self._emit_event(key, event)

        except Exception as ex:
            raise ex


class AggregatedStoreElement:
    def __init__(self, key, aggregates, base_time, initial_data=None):
        self.aggregation_buckets = {}
        self.key = key
        self.aggregates = aggregates
        self.storage_specific_cache = {}

        # Add all raw aggregates, including aggregates not explicitly requested.
        for aggregation_metadata in aggregates:
            for aggr, is_hidden in get_all_raw_aggregates_with_hidden(aggregation_metadata.aggregations).items():
                column_name = f'{aggregation_metadata.name}_{aggr}'
                if column_name in self.aggregation_buckets:
                    self.aggregation_buckets[column_name].is_hidden &= is_hidden
                else:
                    initial_column_data = None
                    if initial_data and column_name in initial_data:
                        initial_column_data = initial_data[column_name]
                    self.aggregation_buckets[column_name] = \
                        AggregationBuckets(aggregation_metadata.name, aggr, aggregation_metadata.windows, base_time,
                                           aggregation_metadata.max_value, is_hidden, initial_column_data)

        # Add all virtual aggregates
        for aggregation_metadata in aggregates:
            for aggr in aggregation_metadata.aggregations:
                if not is_raw_aggregate(aggr):
                    dependant_aggregate_names = get_implied_aggregates(aggr)
                    dependant_buckets = []
                    for dep in dependant_aggregate_names:
                        dependant_buckets.append(self.aggregation_buckets[f'{aggregation_metadata.name}_{dep}'])
                    self.aggregation_buckets[f'{aggregation_metadata.name}_{aggr}'] = \
                        VirtualAggregationBuckets(aggregation_metadata.name, aggr, aggregation_metadata.windows,
                                                  base_time, dependant_buckets)

    def aggregate(self, data, timestamp):
        # add a new point and aggregate
        for aggregation_metadata in self.aggregates:
            if aggregation_metadata.should_aggregate(data):
                curr_value = aggregation_metadata.value_extractor(data)
                for aggr in aggregation_metadata.get_all_raw_aggregates():
                    self.aggregation_buckets[f'{aggregation_metadata.name}_{aggr}'].aggregate(timestamp, curr_value)

    def get_features(self, timestamp):
        result = {}
        for aggregation_bucket in self.aggregation_buckets.values():
            if not aggregation_bucket.is_hidden:
                result.update(aggregation_bucket.get_features(timestamp))

        return result


class AggregateStore:
    def __init__(self, aggregates, use_windows_from_schema=False):
        self._cache = {}
        self._aggregates = aggregates
        self._storage = None
        self._container = None
        self._table_path = None
        self._schema = None
        self._read_only = False
        self._use_windows_from_schema = use_windows_from_schema

    def __iter__(self):
        return iter(self._cache.items())

    async def aggregate(self, key, data, timestamp):
        if not self._schema:
            await self.get_or_save_schema()

        self._cache[key].aggregate(data, timestamp)

    async def get_features(self, key, timestamp):
        if not self._schema:
            await self.get_or_save_schema()

        return self._cache[key].get_features(timestamp)

    async def _get_or_load_key(self, key, timestamp=None):
        if self._read_only or key not in self._cache:
            # Try load from the store, and create a new one only if the key really is new
            initial_data = await self._storage._load_aggregates_by_key(self._container, self._table_path, key)
            self._cache[key] = AggregatedStoreElement(key, self._aggregates, timestamp, initial_data)

        return self._cache[key]

    def __contains__(self, key):
        return key in self._cache

    def get_keys(self):
        return self._cache.keys()

    async def add_key(self, key, base_timestamp, initial_data):
        if not self._schema:
            await self.get_or_save_schema()
        self._cache[key] = AggregatedStoreElement(key, self._aggregates, base_timestamp, initial_data)

    async def get_or_save_schema(self):
        self._schema = await self._storage._load_schema(self._container, self._table_path)

        should_update = True
        if self._schema:
            if self._use_windows_from_schema:
                for aggr in self._aggregates:
                    schema_aggr = self._schema[aggr.name]
                    window_type = schema_aggr['window_type']
                    period_secs = str(int(schema_aggr['period_millis'] / 1000)) + 's'
                    if window_type == "SlidingWindow":
                        aggr.windows = SlidingWindows(aggr.windows.windows, period_secs)
                    elif window_type == "FixedWindow":
                        aggr.windows = FixedWindows(aggr.windows.windows)
                        aggr.windows.period_millis = schema_aggr['period_millis']
                        aggr.windows.total_number_of_buckets = int(aggr.windows.max_window_millis / aggr.windows.period_millis)
                    else:
                        raise TypeError(f'"{window_type}" unknown window type')
            should_update = self._validate_schema_fit_aggregations(self._schema)

        if should_update and not self._read_only:
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
                new_aggregates = get_all_raw_aggregates(schema_aggr['aggregates'])
                old_aggregates = get_all_raw_aggregates(old[name]['aggregates'])
                old[name] = {'period_millis': schema_aggr['period_millis'], 'aggregates': list(new_aggregates.union(old_aggregates))}

        return old

    # Validate if schema corresponds to the requested aggregates, and return whether the schema needs to be updated
    def _validate_schema_fit_aggregations(self, schema):
        should_update = False
        for aggr in self._aggregates:
            if aggr.name not in schema:
                if self._read_only:
                    raise ValueError(f'Requested aggregate {aggr.name}, does not exist in existing feature store at {self._table_path}')
                else:
                    should_update = True
                    continue
            schema_aggr = schema[aggr.name]
            if not aggr.windows.period_millis == schema_aggr['period_millis']:
                raise ValueError(f'Requested period for aggregate {aggr.name} does not match existing period at {self._table_path}. '
                                 f"Requested: {aggr.windows.period_millis}, existing: {schema_aggr['period_millis']}")
            requested_raw_aggregates = aggr.get_all_raw_aggregates()
            existing_raw_aggregates = get_all_raw_aggregates(schema_aggr['aggregates'])
            # validate if current feature store contains all aggregates needed for the requested calculations
            if self._read_only and not requested_raw_aggregates.issubset(existing_raw_aggregates):
                raise ValueError(
                    f'Requested aggregates for feature {aggr.name} do not match with existing aggregates at {self._table_path}. '
                    f"Requested: {aggr.aggregations}, existing: {schema_aggr['aggregates']}")
            # Check if more raw aggregates are requested, in which case a schema update is required
            if not self._read_only and requested_raw_aggregates != existing_raw_aggregates:
                should_update = True

        return should_update

    async def _save_key(self, key):
        await self._storage._save_key(self._container, self._table_path, key, self._cache[key])

    def _aggregates_to_schema(self):
        schema = {}
        for aggr in self._aggregates:
            if isinstance(aggr.windows, SlidingWindows):
                window_type = "SlidingWindow"
            else:
                window_type = "FixedWindow"
            schema[aggr.name] = {'period_millis': aggr.windows.period_millis,
                                 'aggregates': list(get_all_raw_aggregates(aggr.aggregations)),
                                 'window_type': window_type, 'max_window_millis': aggr.windows.max_window_millis}

        return schema

    def __getitem__(self, key):
        return self._cache[key]


class AggregationBuckets:
    def __init__(self, name, aggregation, window, base_time, max_value, is_hidden=False, initial_data=None):
        self.name = name
        self.aggregation = aggregation
        self.window = window
        self.max_value = max_value
        self.buckets = []
        self.is_hidden = is_hidden
        self.should_persist = True
        self.pending_aggr = {}
        self.storage_specific_cache = {}

        if initial_data:
            self.last_bucket_start_time = None
            self.initialize_from_data(initial_data, base_time)
        else:
            self.first_bucket_start_time = self.window.get_window_start_time_by_time(base_time)
            self.last_bucket_start_time = \
                self.first_bucket_start_time + (window.total_number_of_buckets - 1) * window.period_millis

            self.initialize_column()

    def initialize_column(self):
        self.buckets = []

        for _ in range(self.window.total_number_of_buckets):
            self.buckets.append(self.new_aggregation_value())

    def get_or_advance_bucket_index_by_timestamp(self, timestamp):
        if timestamp < self.last_bucket_start_time + self.window.period_millis:
            bucket_index = int((timestamp - self.first_bucket_start_time) / self.window.period_millis)
            return bucket_index
        else:
            self.advance_window_period(timestamp)
            return self.window.total_number_of_buckets - 1  # return last index

    #  Get the index of the bucket corresponding to the requested timestamp
    #  Note: This method can return indexes outside the 'buckets' array
    def get_bucket_index_by_timestamp(self, timestamp):
        bucket_index = int((timestamp - self.first_bucket_start_time) / self.window.period_millis)
        return bucket_index

    def get_nearest_window_index_by_timestamp(self, timestamp, window_millis):
        bucket_index = int((timestamp - self.first_bucket_start_time) / window_millis)
        return bucket_index

    def advance_window_period(self, advance_to):
        desired_bucket_index = int((advance_to - self.first_bucket_start_time) / self.window.period_millis)
        buckets_to_advance = desired_bucket_index - (self.window.total_number_of_buckets - 1)

        if buckets_to_advance > 0:
            if buckets_to_advance > self.window.total_number_of_buckets:
                self.initialize_column()
            else:
                self.buckets = self.buckets[buckets_to_advance:]
                for _ in range(buckets_to_advance):
                    self.buckets.extend([self.new_aggregation_value()])

            self.first_bucket_start_time = \
                self.first_bucket_start_time + buckets_to_advance * self.window.period_millis
            self.last_bucket_start_time = \
                self.last_bucket_start_time + buckets_to_advance * self.window.period_millis

    def aggregate(self, timestamp, value):
        index = self.get_or_advance_bucket_index_by_timestamp(timestamp)

        # Only aggregate points that are in range
        if index >= 0:
            self.buckets[index].aggregate(timestamp, value)
            self.add_to_pending(timestamp, value)

    def add_to_pending(self, timestamp, value):
        bucket_start_time = int(timestamp / self.window.period_millis) * self.window.period_millis
        if bucket_start_time not in self.pending_aggr:
            self.pending_aggr[bucket_start_time] = self.new_aggregation_value()

        self.pending_aggr[bucket_start_time].aggregate(timestamp, value)

    def new_aggregation_value(self):
        return AggregationValue(self.aggregation, self.max_value)

    def get_aggregation_for_aggregation(self):
        if self.aggregation == 'count' or self.aggregation == "sqr":
            return 'sum'
        return self.aggregation

    def get_features(self, timestamp):
        result = {}

        current_time_bucket_index = self.get_bucket_index_by_timestamp(timestamp)
        if current_time_bucket_index < 0:
            return result

        if isinstance(self.window, FixedWindows):
            current_time_bucket_index = self.get_bucket_index_by_timestamp(self.window.round_up_time_to_window(timestamp) - 1)

        aggregated_value = AggregationValue(self.get_aggregation_for_aggregation())
        prev_windows_millis = 0
        for i in range(len(self.window.windows)):
            window_string = self.window.windows[i][1]
            window_millis = self.window.windows[i][0]

            # In case the current bucket is outside our time range just create a feature with the current aggregated
            # value
            if current_time_bucket_index < 0:
                result[f'{self.name}_{self.aggregation}_{window_string}'] = aggregated_value.get_value()

            number_of_buckets_backwards = int((window_millis - prev_windows_millis) / self.window.period_millis)
            last_bucket_to_aggregate = current_time_bucket_index - number_of_buckets_backwards + 1

            if last_bucket_to_aggregate < 0:
                last_bucket_to_aggregate = 0

            for bucket_index in range(current_time_bucket_index, last_bucket_to_aggregate - 1, -1):
                if bucket_index < len(self.buckets):
                    t, v = self.buckets[bucket_index].get_value()
                    aggregated_value.aggregate(t, v)

            # advance the time bucket, so that next iteration won't calculate the same buckets again
            current_time_bucket_index = last_bucket_to_aggregate - 1

            # create a feature for the current time window
            result[f'{self.name}_{self.aggregation}_{window_string}'] = aggregated_value.get_value()[1]
            prev_windows_millis = window_millis

        return result

    def initialize_from_data(self, data, base_time):
        self.buckets = [None] * self.window.total_number_of_buckets
        aggregation_bucket_initial_data = {}

        for key, value in data.items():
            if isinstance(key, int):
                aggregation_bucket_initial_data[key] = value
            else:
                self.storage_specific_cache[key] = value

        first_time, last_time = None, next(iter(aggregation_bucket_initial_data))
        if len(aggregation_bucket_initial_data.keys()) == 2:
            timestamp1, timestamp2 = aggregation_bucket_initial_data.keys()
            first_time, last_time = min(timestamp1, timestamp2), max(timestamp1, timestamp2)

        bucket_index = self.window.total_number_of_buckets - 1
        self.last_bucket_start_time = self.window.get_window_start_time_by_time(base_time)
        self.first_bucket_start_time = \
            self.last_bucket_start_time - (self.window.total_number_of_buckets - 1) * self.window.period_millis

        start_index = int((base_time - last_time) / self.window.period_millis)

        # In case base_time is newer than what is stored in the storage initialize the buckets until reaching the stored data
        if start_index >= len(aggregation_bucket_initial_data[last_time]):
            # If the requested data is so new that the stored data is obsolete just initialize the buckets regardless of the stored data.
            if start_index >= len(aggregation_bucket_initial_data[last_time]) + self.window.total_number_of_buckets:
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
            self.buckets[bucket_index] = AggregationValue(self.aggregation, self.max_value, curr_value)
            bucket_index = bucket_index - 1

        # In case we still haven't finished initializing all buckets and there is another stored bucket, initialize from there
        if first_time and bucket_index >= 0 and base_time > first_time:
            for i in range(len(aggregation_bucket_initial_data[first_time]) - 1, -1, -1):
                curr_value = aggregation_bucket_initial_data[first_time][i]
                self.buckets[bucket_index] = AggregationValue(self.aggregation, self.max_value, curr_value)
                bucket_index = bucket_index - 1

                if bucket_index < 0:
                    return

        # Initialize every remaining buckets
        if bucket_index >= 0:
            for i in range(bucket_index + 1):
                self.buckets[i] = self.new_aggregation_value()

    def get_and_flush_pending(self):
        pending = self.pending_aggr
        self.pending_aggr = {}
        return pending


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
        self.last_bucket_start_time = \
            self.first_bucket_start_time + (window.total_number_of_buckets - 1) * window.period_millis

    def aggregate(self, timestamp, value):
        pass

    def get_features(self, timestamp):
        result = {}

        args_results = [list(bucket.get_features(timestamp).values()) for bucket in self.args]

        for i in range(len(args_results[0])):
            window_string = self.window.windows[i][1]
            current_args = []
            for window_result in args_results:
                current_args.append(window_result[i])

            result[f'{self.name}_{self.aggregation}_{window_string}'] = self.aggregation_func(current_args)
        return result


class AggregationValue:
    def __init__(self, aggregation, max_value=None, set_data=None):
        self.aggregation = aggregation

        self._value = self.get_default_value()
        self._first_time = datetime.max
        self._last_time = datetime.min
        self._max_value = max_value

        # In case we initialize the object from v3io data
        if set_data is not None:
            self._value = set_data

    def aggregate(self, time, value):
        if self.aggregation == 'min':
            self._set_value(min(self._value, value))
        elif self.aggregation == 'max':
            self._set_value(max(self._value, value))
        elif self.aggregation == 'sum':
            self._set_value(self._value + value)
        elif self.aggregation == 'count':
            self._set_value(self._value + 1)
        elif self.aggregation == 'sqr':
            self._set_value(self._value + value * value)
        elif self.aggregation == 'last' and time > self._last_time:
            self._set_value(value)
            self._last_time = time
        elif self.aggregation == 'first' and time < self._first_time:
            self._set_value(value)
            self._first_time = time

    def _set_value(self, value):
        if self._max_value:
            self._value = min(self._max_value, value)
        else:
            self._value = value

    def get_default_value(self):
        return _get_aggregation_default_value(self.aggregation)

    def get_value(self):
        value_time = self._last_time
        if self.aggregation == 'first':
            value_time = self._first_time
        return value_time, self._value


def _get_aggregation_default_value(aggregation):
    if aggregation == 'max':
        return float('-inf')
    elif aggregation == 'min':
        return float('inf')
    elif aggregation == 'first' or aggregation == 'last':
        return None
    else:
        return 0
