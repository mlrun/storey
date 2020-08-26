from datetime import datetime
from enum import Enum

from .utils import parse_duration, bucketPerWindow, get_one_unit_of_duration


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


class EmitAfterWindow(EmitBase):
    def __init__(self, delay_in_seconds=0, emission_type=EmissionType.All):
        self.delay_in_seconds = delay_in_seconds
        EmitBase.__init__(self, emission_type)


class EmitAfterMaxEvent(EmitBase):
    def __init__(self, max_events, emission_type=EmissionType.All):
        self.max_events = max_events
        EmitBase.__init__(self, emission_type)


class EmitAfterDelay(EmitBase):
    def __init__(self, delay_in_seconds, emission_type=EmissionType.All):
        self.delay_in_seconds = delay_in_seconds
        EmitBase.__init__(self, emission_type)


class EmitEveryEvent(EmitBase):
    pass


class LateDataHandling(Enum):
    Nothing = 1
    Sort_before_emit = 2
