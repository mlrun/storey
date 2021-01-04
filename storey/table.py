from typing import List
import copy
from datetime import datetime
from .drivers import Driver
from .utils import _split_path
from .dtypes import FieldAggregator, SlidingWindows, FixedWindows
from .aggregation_utils import is_raw_aggregate, get_virtual_aggregation_func, get_implied_aggregates, get_all_raw_aggregates, \
    get_all_raw_aggregates_with_hidden


class Table:
    """
        Table object, represents a single table in a specific storage.

        :param table_path: Path to the table in the storage.
        :param storage: Storage driver
        :param partitioned_by_key: Whether that data is partitioned by the key or not, based on this indication storage drivers
         can optimize writes. Defaults to True.
        """

    def __init__(self, table_path: str, storage: Driver, partitioned_by_key: bool = True, use_windows_from_schema: bool = False):
        self._container, self._table_path = _split_path(table_path)
        self._storage = storage
        self._attrs_cache = {}
        self._partitioned_by_key = partitioned_by_key
        self._aggregates = None
        self._schema = None
        self._read_only = False
        self._use_windows_from_schema = use_windows_from_schema

    def update_static_attrs(self, key, data):
        attrs = self.get_static_attrs(key)
        if attrs:
            for name, value in data.items():
                attrs[name] = value
        else:
            self.set_static_attrs(key, data)

    async def lazy_load_key_with_aggregates(self, key, timestamp=None):
        if self._read_only or not self.get_aggregations_attrs(key):
            # Try load from the store, and create a new one only if the key really is new
            aggregate_initial_data, additional_data = await self._storage._load_aggregates_by_key(self._container, self._table_path, key)

            # Create new aggregation element
            await self.add_key(key, timestamp, aggregate_initial_data)

            if additional_data:
                # Add additional data to simple cache
                self.update_static_attrs(key, additional_data)

    async def get_or_load_key(self, key, attributes='*'):
        attrs = self.get_static_attrs(key)
        if not attrs:
            res = await self._storage._load_by_key(self._container, self._table_path, key, attributes)
            if res:
                self.set_static_attrs(key, res)
            else:
                self.set_static_attrs(key, {})
        return self.get_static_attrs(key)

    def _set_aggregation_metadata(self, aggregates: List[FieldAggregator], use_windows_from_schema: bool = False):
        self._use_windows_from_schema = use_windows_from_schema
        self._aggregates = aggregates

    async def persist_key(self, key, event_data_to_persist):
        aggr_by_key = self.get_aggregations_attrs(key)
        additional_data_persist = self.get_static_attrs(key)
        if event_data_to_persist:
            if not additional_data_persist:
                additional_data_persist = event_data_to_persist
            else:
                additional_data_persist.update(event_data_to_persist)
        await self._storage._save_key(self._container, self._table_path, key, aggr_by_key, self._partitioned_by_key,
                                      additional_data_persist)

    async def close(self):
        await self._storage.close()

    async def aggregate(self, key, data, timestamp):
        if not self._schema:
            await self.get_or_save_schema()

        self.get_aggregations_attrs(key).aggregate(data, timestamp)

    async def get_features(self, key, timestamp):
        if not self._schema:
            await self.get_or_save_schema()

        return self.get_aggregations_attrs(key).get_features(timestamp)

    async def _get_or_load_key(self, key, timestamp=None):
        if self._read_only or not self.get_aggregations_attrs(key):
            # Try load from the store, and create a new one only if the key really is new
            initial_data = await self._storage._load_aggregates_by_key(self._container, self._table_path, key)
            self.set_aggregations_attrs(key, AggregatedStoreElement(key, self._aggregates, timestamp, initial_data))

        return self.get_aggregations_attrs(key)

    async def add_key(self, key, base_timestamp, initial_data):
        if not self._schema:
            await self.get_or_save_schema()
        self.set_aggregations_attrs(key, AggregatedStoreElement(key, self._aggregates, base_timestamp, initial_data))

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
        await self._storage._save_key(self._container, self._table_path, key, self.get_aggregations_attrs(key))

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

    def get_aggregations_attrs(self, key):
        if key in self._attrs_cache:
            return self._attrs_cache[key].aggregations
        else:
            return None

    def set_aggregations_attrs(self, key, element):
        if key in self._attrs_cache:
            self._attrs_cache[key].aggregations = element
        else:
            self._attrs_cache[key] = _CacheElement({}, element)

    def get_static_attrs(self, key):
        if key in self._attrs_cache:
            return self._attrs_cache[key].static_attrs
        else:
            return None

    def set_static_attrs(self, key, value):
        if key in self._attrs_cache:
            self._attrs_cache[key].static_attrs = value
        else:
            self._attrs_cache[key] = _CacheElement(value, None)

    def get_keys(self):
        return self._attrs_cache.keys()


class _CacheElement:
    def __init__(self, static_attrs, aggregations):
        self.static_attrs = static_attrs
        self.aggregations = aggregations


class AggregatedStoreElement:
    def __init__(self, key, aggregates, base_time, initial_data=None):
        self.aggregation_buckets = {}
        self.key = key
        self.aggregates = aggregates
        self.storage_specific_cache = {}

        # Add all raw aggregates, including aggregates not explicitly requested.
        windows = {}
        for aggregation_metadata in aggregates:
            for meta in aggregation_metadata.aggregations:
                for aggr, is_hidden in get_all_raw_aggregates_with_hidden([meta]).items():
                    if (aggregation_metadata.name, aggr, aggregation_metadata.max_value) in windows:
                        aggr_windows = windows[(aggregation_metadata.name, aggr, aggregation_metadata.max_value)]
                        if is_hidden in aggr_windows:
                            aggr_windows[is_hidden].merge(aggregation_metadata.windows)
                        else:
                            aggr_windows[is_hidden] = copy.deepcopy(aggregation_metadata.windows)
                    else:
                        windows[(aggregation_metadata.name, aggr, aggregation_metadata.max_value)] =\
                            {is_hidden: copy.deepcopy(aggregation_metadata.windows)}

        for (name, aggr, max_value), calculated_windows in windows.items():
            column_name = f'{name}_{aggr}'
            initial_column_data = None
            if initial_data and column_name in initial_data:
                initial_column_data = initial_data[column_name]
            explicit_windows = None
            hidden_windows = None
            if False in calculated_windows:
                explicit_windows = calculated_windows[False]
            if True in calculated_windows:
                hidden_windows = calculated_windows[True]
            self.aggregation_buckets[column_name] =\
                AggregationBuckets(name, aggr, explicit_windows, hidden_windows, base_time, max_value, initial_column_data)

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
            if isinstance(aggregation_bucket, VirtualAggregationBuckets) or aggregation_bucket.explicit_windows:
                result.update(aggregation_bucket.get_features(timestamp))

        return result


class AggregationBuckets:
    def __init__(self, name, aggregation, explicit_windows, hidden_windows, base_time, max_value, initial_data=None):
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
            self.period_millis = explicit_windows.period_millis
            self._window_start_time = explicit_windows.get_window_start_time_by_time(base_time)
            if self._precalculated_aggregations:
                for win in explicit_windows.windows:
                    self._current_aggregate_values[win] = AggregationValue(aggregation)
        if hidden_windows:
            if not explicit_windows:
                self.is_fixed_window = isinstance(self.explicit_windows, FixedWindows)
                if self.is_fixed_window:
                    self._round_time_func = self.explicit_windows.round_up_time_to_window
                self.period_millis = hidden_windows.period_millis
                self._window_start_time = hidden_windows.get_window_start_time_by_time(base_time)
            if self._precalculated_aggregations:
                for win in hidden_windows.windows:
                    if win not in self._current_aggregate_values:
                        self._current_aggregate_values[win] = AggregationValue(aggregation)

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
            self.last_bucket_start_time = \
                self.first_bucket_start_time + (self.total_number_of_buckets - 1) * self.period_millis

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
                    current_pre_aggregated_value = aggr.get_value()[1]
                    bucket_aggregated_value = self.buckets[bucket_id].get_value()[1]
                    if self.aggregation == "min" or self.aggregation == "max":
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
                # Updating the pre aggreagted data per window
                self.remove_old_values_from_pre_aggregations(advance_to)
                self.buckets = self.buckets[buckets_to_advance:]
                for _ in range(buckets_to_advance):
                    self.buckets.append(self.new_aggregation_value())

            self.first_bucket_start_time = \
                self.first_bucket_start_time + buckets_to_advance * self.period_millis
            self.last_bucket_start_time = \
                self.last_bucket_start_time + buckets_to_advance * self.period_millis

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
        return AggregationValue(self.aggregation, self.max_value)

    def get_aggregation_for_aggregation(self):
        if self.aggregation == 'count' or self.aggregation == "sqr":
            return 'sum'
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
        if self._need_to_recalculate_pre_aggregates or \
                self.get_bucket_index_by_timestamp(timestamp) < self.get_bucket_index_by_timestamp(self._last_data_point_timestamp) or \
                not self._precalculated_aggregations:
            return self.calculate_features(timestamp, windows)

        # In case our pre aggregates already have the answer
        for win in windows:
            result[f'{self.name}_{self.aggregation}_{win[1]}'] = self._current_aggregate_values[win].get_value()[1]

        return result

    def calculate_features(self, timestamp, windows):
        result = {}

        current_time_bucket_index = self.get_bucket_index_by_timestamp(timestamp)
        if current_time_bucket_index < 0:
            self._need_to_recalculate_pre_aggregates = False
            return result

        if self.is_fixed_window:
            current_time_bucket_index = self.get_bucket_index_by_timestamp(self._round_time_func(timestamp) - 1)

        aggregated_value = AggregationValue(self.get_aggregation_for_aggregation())
        prev_windows_millis = 0
        for win in windows:
            window_string = win[1]
            window_millis = win[0]

            # In case the current bucket is outside our time range just create a feature with the current aggregated
            # value
            if current_time_bucket_index < 0:
                result[f'{self.name}_{self.aggregation}_{window_string}'] = aggregated_value.get_value()[1]

            number_of_buckets_backwards = int((window_millis - prev_windows_millis) / self.period_millis)
            last_bucket_to_aggregate = current_time_bucket_index - number_of_buckets_backwards + 1

            if last_bucket_to_aggregate < 0:
                last_bucket_to_aggregate = 0

            for bucket_index in range(current_time_bucket_index, last_bucket_to_aggregate - 1, -1):
                if bucket_index < len(self.buckets):
                    t, v = self.buckets[bucket_index].get_value()
                    aggregated_value.aggregate(t, v)

            # advance the time bucket, so that next iteration won't calculate the same buckets again
            current_time_bucket_index = last_bucket_to_aggregate - 1
            current_aggregations_value = aggregated_value.get_value()[1]

            # create a feature for the current time window
            result[f'{self.name}_{self.aggregation}_{window_string}'] = current_aggregations_value
            prev_windows_millis = window_millis

            # Update the corresponding pre aggregate
            if self._precalculated_aggregations and self._need_to_recalculate_pre_aggregates:
                self._current_aggregate_values[win] = AggregationValue(self.aggregation, set_data=current_aggregations_value)
        self._need_to_recalculate_pre_aggregates = False
        return result

    def initialize_from_data(self, data, base_time):
        period = self.period_millis
        self.buckets = [None] * self.total_number_of_buckets
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

        bucket_index = self.total_number_of_buckets - 1
        self.last_bucket_start_time = self._window_start_time
        self.first_bucket_start_time = \
            self.last_bucket_start_time - (self.total_number_of_buckets - 1) * period

        start_index = int((base_time - last_time) / period)

        # In case base_time is newer than what is stored in the storage initialize the buckets until reaching the stored data
        if start_index >= len(aggregation_bucket_initial_data[last_time]):
            # If the requested data is so new that the stored data is obsolete just initialize the buckets regardless of the stored data.
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
        for i in range(bucket_index + 1):
            self.buckets[i] = self.new_aggregation_value()

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
        self.last_bucket_start_time = \
            self.first_bucket_start_time + (window.total_number_of_buckets - 1) * window.period_millis

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
