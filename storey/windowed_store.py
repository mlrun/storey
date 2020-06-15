import asyncio
from datetime import datetime

from .dtypes import EmitAfterMaxEvent, LateDataHandling, EmitAfterPeriod, EmitAfterWindow, EmitAfterDelay, EmitEveryEvent, EmissionType
from .flow import Flow, NeedsV3ioAccess


class Window(Flow, NeedsV3ioAccess):
    def __init__(self, window, key_column, time_column, emit_policy=EmitAfterMaxEvent(10),
                 late_data_handling=LateDataHandling.Nothing, webapi=None, access_key=None):
        Flow.__init__(self)
        NeedsV3ioAccess.__init__(self, webapi, access_key)
        self._windowed_store = WindowedStore(window, late_data_handling)
        self._window = window
        self._key_column = key_column
        self._time_column = time_column
        self._emit_policy = emit_policy
        self._late_data_handling = late_data_handling
        self._events_in_batch = 0
        self._emit_worker_running = False

    async def _emit_worker(self):
        while True:
            if isinstance(self._emit_policy, EmitAfterPeriod):
                await asyncio.sleep(self._window.period_millis / 1000)
            elif isinstance(self._emit_policy, EmitAfterWindow):
                await asyncio.sleep(self._window.window_millis / 1000)
            elif isinstance(self._emit_policy, EmitAfterDelay):
                await asyncio.sleep(self._emit_policy.delay_in_seconds)

            await self.emit_window()

    async def _do(self, element):
        if (not self._emit_worker_running) and \
                (isinstance(self._emit_policy, EmitAfterPeriod) or isinstance(self._emit_policy, EmitAfterWindow)):
            asyncio.get_running_loop().create_task(self._emit_worker())
            self._emit_worker_running = True
        key = element.pop(self._key_column)
        timestamp = element.pop(self._time_column)
        self._windowed_store.add(key, element, timestamp)
        self._events_in_batch = self._events_in_batch + 1

        if isinstance(self._emit_policy, EmitEveryEvent) or \
                isinstance(self._emit_policy,
                           EmitAfterMaxEvent) and self._events_in_batch == self._emit_policy.max_events:
            await self.emit_window()
            self._events_in_batch = 0

    async def emit_window(self):
        await self._do_downstream(self._windowed_store)

        # when emission type is incremental we need to flush the window after every emit
        if self._emit_policy.emission_type == EmissionType.Incremental:
            self._windowed_store.flush()


class WindowBucket:
    def __init__(self, late_data_handling):
        self.data = []
        self.max_time = 0
        self.late_data_handling = late_data_handling

    def add(self, t, v):
        if t < self.max_time and self.late_data_handling == LateDataHandling.Sort_before_emit:
            index = 0
            for data_point in self.data:
                if t < data_point[0]:
                    self.data.insert(index, (t, v))
                    break
                index = index + 1
        else:
            self.data.append((t, v))
            if t > self.max_time:
                self.max_time = t

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f'{self.data} - {self.max_time}'


# a class that accepts - window, (data, key, timestamp)
class WindowedStoreElement:
    def __init__(self, key, window, late_data_handling):
        self.key = key
        self.late_data_handling = late_data_handling
        self.window = window
        self.features = {}
        self.first_bucket_start_time = self.window.get_window_start_time()
        self.last_bucket_start_time = \
            self.first_bucket_start_time + (window.get_total_number_of_buckets() - 1) * window.period_millis

        # for feature in aggregations:
        #     for aggr in aggregations[feature]:
        #         self.features[self.get_column_name(feature, aggr)] = \
        #             [0.0] * window.get_total_number_of_buckets()

    def add(self, data, timestamp):
        # add a new point and aggregate
        for column_name in data:
            if column_name not in self.features:
                self.initialize_column(column_name)
            index = self.get_or_advance_bucket_index_by_timestamp(timestamp)
            self.features[column_name][index].add(timestamp, data[column_name])

    def get_column_name(self, column, aggregation):
        return f'{column}_{aggregation}_{self.window.window_str}'

    def initialize_column(self, column):
        self.features[column] = []
        for i in range(self.window.get_total_number_of_buckets()):
            self.features[column].append(WindowBucket(self.late_data_handling))

    def get_or_advance_bucket_index_by_timestamp(self, timestamp):
        if timestamp < self.last_bucket_start_time + self.window.period_millis:
            bucket_index = int((timestamp - self.first_bucket_start_time) / self.window.period_millis)
            return bucket_index
        else:
            self.advance_window_period(timestamp)
            return self.window.get_total_number_of_buckets() - 1  # return last index

    def advance_window_period(self, advance_to=None):
        if not advance_to:
            advance_to = datetime.now().timestamp() * 1000
        desired_bucket_index = int((advance_to - self.first_bucket_start_time) / self.window.period_millis)
        buckets_to_advnace = desired_bucket_index - (self.window.get_total_number_of_buckets() - 1)

        if buckets_to_advnace > 0:
            if buckets_to_advnace > self.window.get_total_number_of_buckets():
                for column in self.features:
                    self.initialize_column(column)
            else:
                for column in self.features:
                    self.features[column] = self.features[column][buckets_to_advnace:]
                    for i in range(buckets_to_advnace):
                        self.features[column].extend([WindowBucket(self.late_data_handling)])

            self.first_bucket_start_time = \
                self.first_bucket_start_time + buckets_to_advnace * self.window.period_millis
            self.last_bucket_start_time = \
                self.last_bucket_start_time + buckets_to_advnace * self.window.period_millis

    def flush(self):
        for column in self.features:
            self.initialize_column(column)


def aggregate(self, aggregation, old_value, new_value):
    if aggregation == 'min':
        return min(old_value, new_value)
    elif aggregation == 'max':
        return max(old_value, new_value)
    elif aggregation == 'sum':
        return old_value + new_value
    elif aggregation == 'count':
        return old_value + 1
    elif aggregation == 'last':
        return new_value
    elif aggregation == 'first':
        return old_value


class WindowedStore:
    def __init__(self, window, late_data_handling):
        self.window = window
        self.late_data_handling = late_data_handling
        self.cache = {}

    def __iter__(self):
        return iter(self.cache.items())

    def add(self, key, data, timestamp):
        if key not in self.cache:
            self.cache[key] = WindowedStoreElement(key, self.window, self.late_data_handling)

        if isinstance(timestamp, datetime):
            timestamp = timestamp.timestamp() * 1000
        self.cache[key].add(data, timestamp)

    def flush(self):
        for key in self.cache:
            self.cache[key].flush()
