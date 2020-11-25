import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Union, Optional, Callable, List, Dict

from .utils import parse_duration, bucketPerWindow, get_one_unit_of_duration, _split_path
from .aggregation_utils import get_all_raw_aggregates

_termination_obj = object()


class Event:
    """The basic unit of data in storey. All steps receive and emit events.

    :param body: the event payload, or data
    :type body: object
    :param key: Event key. Used by steps that aggregate events by key, such as AggregateByKey.
    :type key: string
    :param time: Event time. Defaults to the time the event was created, UTC.
    :type time: datetime
    :param id: Event identifier. Usually a unique identifier. Defaults to random (version 4) UUID.
    :type id: string
    :param headers: Request headers (HTTP only)
    :type headers: dict
    :param method: Request method (HTTP only)
    :type method: string
    :param path: Request path (HTTP only)
    :type path: string
    :param content_type: Request content type (HTTP only)
    :param awaitable_result: Generally not passed directly.
    :type awaitable_result: AwaitableResult
    """

    def __init__(self, body, key=None, time=None, id=None, headers=None, method=None, path='/', content_type=None, awaitable_result=None):
        self.body = body
        self.key = key
        self.time = time or datetime.now(timezone.utc)
        self.id = id or uuid.uuid4().hex
        self.headers = headers
        self.method = method
        self.path = path
        self.content_type = content_type
        self._awaitable_result = awaitable_result

    def __eq__(self, other):
        if not isinstance(other, Event):
            return False

        return self.body == other.body and self.time == other.time and self.id == other.id and self.headers == other.headers and \
               self.method == other.method and self.path == other.path and self.content_type == other.content_type  # noqa: E127


class V3ioError(Exception):
    pass


class FlowError(Exception):
    pass


class WindowBase:
    def __init__(self, window, period, window_str):
        self.window_millis = window
        self.period_millis = period
        self.window_str = window_str


class FixedWindow(WindowBase):
    def __init__(self, window):
        window_millis = parse_duration(window)
        WindowBase.__init__(self, window_millis, window_millis / bucketPerWindow, window)

    def get_total_number_of_buckets(self):
        return bucketPerWindow * 2

    def get_window_start_time(self):
        return self.get_current_window()

    def get_current_window(self):
        return int((datetime.now().timestamp() * 1000) / self.window_millis) * self.window_millis

    def get_current_period(self):
        return int((datetime.now().timestamp() * 1000) / self.period_millis) * self.period_millis


class SlidingWindow(WindowBase):
    def __init__(self, window, period):
        window_millis, period_millis = parse_duration(window), parse_duration(period)
        if not window_millis % period_millis == 0:
            raise ValueError('period must be a divider of the window')

        WindowBase.__init__(self, window_millis, period_millis, window)

    def get_total_number_of_buckets(self):
        return int(self.window_millis / self.period_millis)

    def get_window_start_time(self):
        return datetime.now().timestamp() * 1000


class WindowsBase:
    def __init__(self, period, windows):
        self.max_window_millis = windows[-1][0]
        self.smallest_window_millis = windows[0][0]
        self.period_millis = period
        self.windows = windows  # list of tuples of the form (3600000, '1h')
        self.total_number_of_buckets = int(self.max_window_millis / self.period_millis)


def sort_windows_and_convert_to_millis(windows):
    if len(windows) == 0:
        raise ValueError('Windows list can not be empty')

    # Validate windows order
    windows_tuples = [(parse_duration(window), window) for window in windows]
    windows_tuples.sort(key=lambda tup: tup[0])
    return windows_tuples


class FixedWindows(WindowsBase):
    """
    List of time windows representing fixed time intervals.
    For example: 1h will represent 1h windows starting every round hour.

    :param windows: List of time windows in the format [0-9]+[smhd]
    :type windows: list of string
    """

    def __init__(self, windows):
        windows_tuples = sort_windows_and_convert_to_millis(windows)
        # The period should be a divisor of the unit of the smallest window,
        # for example if the smallest request window is 2h, the period will be 1h / `bucketPerWindow`
        self.smallest_window_unit_millis = get_one_unit_of_duration(windows_tuples[0][1])
        WindowsBase.__init__(self, self.smallest_window_unit_millis / bucketPerWindow, windows_tuples)

    def round_up_time_to_window(self, timestamp):
        return int(timestamp / self.smallest_window_unit_millis) * self.smallest_window_unit_millis + self.smallest_window_unit_millis

    def get_period_by_time(self, timestamp):
        return int(timestamp / self.period_millis) * self.period_millis

    def get_window_start_time_by_time(self, reference_timestamp):
        return self.get_period_by_time(reference_timestamp)


class SlidingWindows(WindowsBase):
    """
    List of time windows representing sliding time intervals.
    For example: 1h will represent 1h windows starting from the current time.

    :param windows: List of time windows in the format [0-9]+[smhd]
    :type windows: list of string
    :param period: Period in the format [0-9]+[smhd]
    :type period: string
    """

    def __init__(self, windows, period=None):
        windows_tuples = sort_windows_and_convert_to_millis(windows)

        if period:
            period_millis = parse_duration(period)

            # Verify the given period is a divisor of the windows
            for window in windows:
                if not parse_duration(window) % period_millis == 0:
                    raise ValueError(
                        f'Period must be a divisor of every window, but period {period} does not divide {window}')
        else:
            # The period should be a divisor of the unit of the smallest window,
            # for example if the smallest request window is 2h, the period will be 1h / `bucketPerWindow`
            smallest_window_unit_millis = get_one_unit_of_duration(windows_tuples[0][1])
            period_millis = smallest_window_unit_millis / bucketPerWindow

        WindowsBase.__init__(self, period_millis, windows_tuples)

    def get_window_start_time_by_time(self, timestamp):
        return timestamp


class EmissionType(Enum):
    All = 1
    Incremental = 2


class EmitBase:
    def __init__(self, emission_type=EmissionType.All):
        self.emission_type = emission_type


class EmitAfterPeriod(EmitBase):
    def __init__(self, delay_in_seconds=0, emission_type=EmissionType.All):
        self.delay_in_seconds = delay_in_seconds
        EmitBase.__init__(self, emission_type)

    @staticmethod
    def name():
        return 'afterPeriod'


class EmitAfterWindow(EmitBase):
    def __init__(self, delay_in_seconds=0, emission_type=EmissionType.All):
        self.delay_in_seconds = delay_in_seconds
        EmitBase.__init__(self, emission_type)

    @staticmethod
    def name():
        return 'afterWindow'


class EmitAfterMaxEvent(EmitBase):
    def __init__(self, max_events, emission_type=EmissionType.All):
        self.max_events = max_events
        EmitBase.__init__(self, emission_type)

    @staticmethod
    def name():
        return 'maxEvents'


class EmitAfterDelay(EmitBase):
    def __init__(self, delay_in_seconds, emission_type=EmissionType.All):
        self.delay_in_seconds = delay_in_seconds
        EmitBase.__init__(self, emission_type)

    @staticmethod
    def name():
        return 'afterDelay'


class EmitEveryEvent(EmitBase):
    @staticmethod
    def name():
        return 'everyEvent'

    pass


def _dict_to_emit_policy(policy_dict):
    mode = policy_dict.pop('mode')
    if mode == EmitEveryEvent.name():
        policy = EmitEveryEvent()
    elif mode == EmitAfterMaxEvent.name():
        if 'maxEvents' not in policy_dict:
            raise ValueError('maxEvents parameter must be specified for maxEvents emit policy')
        policy = EmitAfterMaxEvent(policy_dict.pop('maxEvents'))
    elif mode == EmitAfterDelay.name():
        if 'delay' not in policy_dict:
            raise ValueError('delay parameter must be specified for afterDelay emit policy')

        policy = EmitAfterDelay(policy_dict.pop('delay'))
    elif mode == EmitAfterWindow.name():
        policy = EmitAfterWindow(delay_in_seconds=policy_dict.pop('delay', 0))
    elif mode == EmitAfterPeriod.name():
        policy = EmitAfterPeriod(delay_in_seconds=policy_dict.pop('delay', 0))
    else:
        raise TypeError(f'unsupported emit policy type: {mode}')

    if policy_dict:
        raise ValueError(f'got unexpected arguments for emit policy: {policy_dict}')

    return policy


class LateDataHandling(Enum):
    Nothing = 1
    Sort_before_emit = 2


class FieldAggregator:
    """
    Field Aggregator represents an set of aggregation features.

    :param name: Name for the feature.
    :param field: Field in the event body to aggregate.
    :param aggr: List of aggregates to apply. Valid values are: [count, sum, sqr, avg, max, min, last, first, sttdev, stdvar]
    :param windows: Time windows to aggregate the data by.
    :param aggr_filter: Filter specifying which events to aggregate. (Optional)
    :param max_value: Maximum value for the aggregation (Optional)
    """

    def __init__(self, name: str, field: Union[str, Callable[[Event], object]], aggr: List[str],
                 windows: Union[FixedWindows, SlidingWindows], aggr_filter: Optional[Callable[[Event], bool]] = None,
                 max_value: Optional[float] = None):
        if aggr_filter is not None and not callable(aggr_filter):
            raise TypeError(f'aggr_filter expected to be callable, got {type(aggr_filter)}')

        if callable(field):
            self.value_extractor = field
        elif isinstance(field, str):
            self.value_extractor = lambda element: element[field]
        else:
            raise TypeError(f'field is expected to be either a callable or string but got {type(field)}')

        self.name = name
        self.aggregations = aggr
        self.windows = windows
        self.aggr_filter = aggr_filter
        self.max_value = max_value

    def get_all_raw_aggregates(self):
        return get_all_raw_aggregates(self.aggregations)

    def should_aggregate(self, element):
        if not self.aggr_filter:
            return True

        return self.aggr_filter(element)


class Table:
    """
        Table object, represents a single table in a specific storage.

        :param table_path: Path to the table in the storage.
        :type table_path: string
        :param storage: Storage driver
        :type storage: {V3ioDriver, NoopDriver}
        :param partitioned_by_key: Whether that data is partitioned by the key or not, based on this indication storage drivers
         can optimize writes. Defaults to True.
        :type partitioned_by_key: boolean
        """

    def __init__(self, table_path: str, storage, partitioned_by_key=True):
        self._container, self._table_path = _split_path(table_path)
        self._storage = storage
        self._cache = {}
        self._aggregation_store = None
        self._partitioned_by_key = partitioned_by_key

    def __getitem__(self, key):
        return self._cache[key]

    def __setitem__(self, key, value):
        self._cache[key] = value

    def update_key(self, key, data):
        if key in self._cache:
            for name, value in data.items():
                self._cache[key][name] = value
        else:
            self._cache[key] = data

    async def lazy_load_key_with_aggregates(self, key, timestamp=None):
        if self._aggregation_store._read_only or key not in self._aggregation_store:
            # Try load from the store, and create a new one only if the key really is new
            aggregate_initial_data, additional_data = await self._storage._load_aggregates_by_key(self._container, self._table_path, key)

            # Create new aggregation element
            self._aggregation_store.add_key(key, timestamp, aggregate_initial_data)

            if additional_data:
                # Add additional data to simple cache
                self.update_key(key, additional_data)

    async def get_or_load_key(self, key, attributes='*'):
        if key not in self._cache:
            res = await self._storage._load_by_key(self._container, self._table_path, key, attributes)
            if res:
                self._cache[key] = res
            else:
                self._cache[key] = {}
        return self._cache[key]

    def _set_aggregation_store(self, store):
        store._container = self._container
        store._table_path = self._table_path
        store._storage = self._storage
        self._aggregation_store = store

    async def persist_key(self, key, event_data_to_persist):
        aggr_by_key = None
        if self._aggregation_store:
            aggr_by_key = self._aggregation_store[key]
        additional_data_persist = self._cache.get(key, None)
        if event_data_to_persist:
            if not additional_data_persist:
                additional_data_persist = event_data_to_persist
            else:
                additional_data_persist.update(event_data_to_persist)
        await self._storage._save_key(self._container, self._table_path, key, aggr_by_key, self._partitioned_by_key,
                                      additional_data_persist)

    async def close(self):
        await self._storage.close()


class Context:
    """
    Context object that holds global secrets and configurations to be passed to relevant steps.

    :param initial_secrets: Initial dict of secrets.
    :param initial_parameters: Initial dict of parameters.
    :param initial_tables: Initial dict of tables.
    """

    def __init__(self, initial_secrets: Optional[Dict[str, str]] = None, initial_parameters: Optional[Dict[str, object]] = None,
                 initial_tables: Optional[Dict[str, Table]] = None):
        self._secrets = initial_secrets or {}
        self._parameters = initial_parameters or {}
        self._tables = initial_tables or {}

    def get_param(self, key, default):
        return self._parameters.get(key, default)

    def set_param(self, key, value):
        self._parameters[key] = value

    def get_secret(self, key):
        return self._secrets.get(key, None)

    def set_secret(self, key, secret):
        self._secrets[key] = secret

    def get_table(self, key):
        return self._tables[key]

    def set_table(self, key, table):
        self._tables[key] = table
