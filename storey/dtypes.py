from datetime import datetime, timezone
from enum import Enum
from typing import Union, Optional, Callable, List
from copy import deepcopy

from .utils import parse_duration, bucketPerWindow, get_one_unit_of_duration
from .aggregation_utils import get_all_raw_aggregates

_termination_obj = object()


class Event:
    """The basic unit of data in storey. All steps receive and emit events.

    :param body: the event payload, or data
    :param key: Event key. Used by steps that aggregate events by key, such as AggregateByKey. (Optional)
    :param time: Event time. Defaults to the time the event was created, UTC. (Optional)
    :param id: Event identifier. Usually a unique identifier. (Optional)
    :param headers: Request headers (HTTP only) (Optional)
    :param method: Request method (HTTP only) (Optional)
    :param path: Request path (HTTP only) (Optional)
    :param content_type: Request content type (HTTP only) (Optional)
    :param awaitable_result: Generally not passed directly. (Optional)
    :type awaitable_result: AwaitableResult (Optional)
    """

    def __init__(self, body: object, key: Optional[str] = None, time: Optional[datetime] = None, id: Optional[str] = None,
                 headers: Optional[dict] = None, method: Optional[str] = None, path: Optional[str] = '/',
                 content_type=None, awaitable_result=None):
        self.body = body
        self.key = key
        self.time = time or datetime.now(timezone.utc)
        self.id = id
        self.headers = headers
        self.method = method
        self.path = path
        self.content_type = content_type
        self._awaitable_result = awaitable_result
        self.error = None

    def __eq__(self, other):
        if not isinstance(other, Event):
            return False

        return self.body == other.body and self.time == other.time and self.id == other.id and self.headers == other.headers and \
               self.method == other.method and self.path == other.path and self.content_type == other.content_type  # noqa: E127

    def __str__(self):
        return f'Event(id={self.id}, key={self.key}, time={self.time}, body={self.body})'

    def copy(self, body=None, key=None, time=None, id=None, headers=None, method=None, path=None, content_type=None,
             awaitable_result=None,
             deep_copy=False) -> 'Event':
        if deep_copy and body is None and self.body is not None:
            body = deepcopy(self.body)

        return Event(
            body=body or self.body,
            key=key or self.key,
            time=time or self.time,
            id=id or self.id,
            headers=headers or self.headers,
            method=method or self.method,
            path=path or self.path,
            content_type=content_type or self.content_type,
            awaitable_result=awaitable_result or self._awaitable_result
        )


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
    """
    Time window representing fixed time interval. The interval will be divided to 10 periods

    :param window: Time window in the format [0-9]+[smhd]
    """
    def __init__(self, window: str):
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
    """
    Time window representing sliding time interval divided to periods.

    :param window: Time window in the format [0-9]+[smhd]
    :param period: Number of buckets to use for the window [0-9]+[smhd]
    """
    def __init__(self, window: str, period: str):
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

    def merge(self, new):
        if self.period_millis != new.period_millis:
            raise ValueError('Cannot use different periods for same aggregation')
        found_new_window = False
        for window in new.windows:
            if window not in self.windows:
                self.windows.append(window)
                found_new_window = True
        if found_new_window:
            if self.max_window_millis < new.max_window_millis:
                self.max_window_millis = new.max_window_millis
            if self.smallest_window_millis > new.smallest_window_millis:
                self.smallest_window_millis = new.smallest_window_millis
            if self.total_number_of_buckets < new.total_number_of_buckets:
                self.total_number_of_buckets = new.total_number_of_buckets
            sorted(set(self.windows), key=lambda tup: tup[0])


def sort_windows_and_convert_to_millis(windows):
    if len(windows) == 0:
        raise ValueError('Windows list can not be empty')

    if isinstance(windows[0], str):
        # Validate windows order
        windows_tuples = [(parse_duration(window), window) for window in windows]
        windows_tuples.sort(key=lambda tup: tup[0])
    else:
        # Internally windows can be passed as tuples
        windows_tuples = windows
    return windows_tuples


class FixedWindows(WindowsBase):
    """
    List of time windows representing fixed time intervals.
    For example: 1h will represent 1h windows starting every round hour.

    :param windows: List of time windows in the format [0-9]+[smhd]
    """

    def __init__(self, windows: List[str]):
        windows_tuples = sort_windows_and_convert_to_millis(windows)
        # The period should be a divisor of the unit of the smallest window,
        # for example if the smallest request window is 2h, the period will be 1h / `bucketPerWindow`
        self.smallest_window_unit_millis = get_one_unit_of_duration(windows_tuples[0][1])
        WindowsBase.__init__(self, self.smallest_window_unit_millis / bucketPerWindow, windows_tuples)

    def round_up_time_to_window(self, timestamp):
        return int(
            timestamp / self.smallest_window_unit_millis) * self.smallest_window_unit_millis + self.smallest_window_unit_millis

    def get_period_by_time(self, timestamp):
        return int(timestamp / self.period_millis) * self.period_millis

    def get_window_start_time_by_time(self, reference_timestamp):
        return self.get_period_by_time(reference_timestamp)


class SlidingWindows(WindowsBase):
    """
    List of time windows representing sliding time intervals.
    For example: 1h will represent 1h windows starting from the current time.

    :param windows: List of time windows in the format [0-9]+[smhd]
    :param period: Period in the format [0-9]+[smhd]
    """

    def __init__(self, windows: List[str], period: Optional[str] = None):
        windows_tuples = sort_windows_and_convert_to_millis(windows)

        if period:
            period_millis = parse_duration(period)

            # Verify the given period is a divisor of the windows
            for window in windows_tuples:
                if not window[0] % period_millis == 0:
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
    """
    Emit event for next step after each period ends

    :param delay_in_seconds: Delay event emission by seconds (Optional)
    """
    def __init__(self, delay_in_seconds: Optional[int] = 0, emission_type=EmissionType.All):
        self.delay_in_seconds = delay_in_seconds
        EmitBase.__init__(self, emission_type)

    @staticmethod
    def name():
        return 'afterPeriod'


class EmitAfterWindow(EmitBase):
    """
    Emit event for next step after each window ends

    :param delay_in_seconds: Delay event emission by seconds (Optional)
    """
    def __init__(self, delay_in_seconds: Optional[int] = 0, emission_type=EmissionType.All):
        self.delay_in_seconds = delay_in_seconds
        EmitBase.__init__(self, emission_type)

    @staticmethod
    def name():
        return 'afterWindow'


class EmitAfterMaxEvent(EmitBase):
    """
    Emit the Nth event

    :param max_events: Which number of event to emit
    :param timeout_secs: Emit event after timeout expires even if it didn't reach max_events event (Optional)
    """
    def __init__(self, max_events: int, timeout_secs: Optional[int] = None, emission_type=EmissionType.All):
        self.max_events = max_events
        self.timeout_secs = timeout_secs
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
    """
    Emit every event
    """
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

    def __init__(self, name: str, field: Union[str, Callable[[Event], object], None], aggr: List[str],
                 windows: Union[FixedWindows, SlidingWindows], aggr_filter: Optional[Callable[[Event], bool]] = None,
                 max_value: Optional[float] = None):
        if aggr_filter is not None and not callable(aggr_filter):
            raise TypeError(f'aggr_filter expected to be callable, got {type(aggr_filter)}')

        if callable(field):
            self.value_extractor = field
        elif isinstance(field, str):
            self.value_extractor = lambda element: element[field]

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
