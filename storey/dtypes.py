from datetime import datetime
from enum import Enum

from .utils import parse_duration

bucketPerWindow = 10


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


class EmissionType(Enum):
    All = 1
    Incremental = 2


class EmitBase:
    def __init__(self, emission_type=EmissionType.All):
        self.emission_type = emission_type


class EmitAfterPeriod(EmitBase):
    pass


class EmitAfterWindow(EmitBase):
    pass


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
