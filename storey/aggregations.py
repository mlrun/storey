import asyncio
import re
import time
from datetime import datetime
from typing import Optional, Union, Callable, List, Dict

import pandas as pd

from .dtypes import EmitEveryEvent, FixedWindows, SlidingWindows, EmitAfterPeriod, EmitAfterWindow, EmitAfterMaxEvent, \
    _dict_to_emit_policy, FieldAggregator, EmitPolicy, FixedWindowType
from .flow import Flow, _termination_obj, Event
from .table import Table
from .utils import stringify_key

_default_emit_policy = EmitEveryEvent()


class AggregateByKey(Flow):
    """
    Aggregates the data into the table object provided for later persistence, and outputs an event enriched with the requested aggregation
    features.
    Persistence is done via the `NoSqlTarget` step and based on the Cache object persistence settings.

    :param aggregates: List of aggregates to apply for each event.
        accepts either list of FieldAggregators or a dictionary describing FieldAggregators.
    :param table: A Table object or name for persistence of aggregations.
        If a table name is provided, it will be looked up in the context object passed in kwargs.
    :param key: Key field to aggregate by, accepts either a string representing the key field or a key extracting function.
     Defaults to the key in the event's metadata. (Optional)
    :param emit_policy: Policy indicating when the data will be emitted. Defaults to EmitEveryEvent
    :param augmentation_fn: Function that augments the features into the event's body. Defaults to updating a dict. (Optional)
    :param enrich_with: List of attributes names from the associated storage object to be fetched and added to every event. (Optional)
    :param aliases: Dictionary specifying aliases for enriched or aggregate columns, of the
     format `{'col_name': 'new_col_name'}`. (Optional)
    """

    def __init__(self, aggregates: Union[List[FieldAggregator], List[Dict[str, object]]], table: Union[Table, str],
                 key: Union[str, Callable[[Event], object], None] = None,
                 emit_policy: Union[EmitPolicy, Dict[str, object]] = _default_emit_policy,
                 augmentation_fn: Optional[Callable[[Event, Dict[str, object]], Event]] = None, enrich_with: Optional[List[str]] = None,
                 aliases: Optional[Dict[str, str]] = None, use_windows_from_schema: bool = False, **kwargs):
        Flow.__init__(self, **kwargs)
        aggregates = self._parse_aggregates(aggregates)
        self._check_unique_names(aggregates)

        self._table = table
        if isinstance(table, str):
            if not self.context:
                raise TypeError("Table can not be string if no context was provided to the step")
            self._table = self.context.get_table(table)
        self._table._set_aggregation_metadata(aggregates, use_windows_from_schema=use_windows_from_schema)
        self._closeables = [self._table]

        self._aggregates_metadata = aggregates

        self._enrich_with = enrich_with or []
        self._aliases = aliases or {}

        if not isinstance(emit_policy, EmitPolicy) and not isinstance(emit_policy, Dict):
            raise TypeError(f'emit_policy parameter must be of type EmitPolicy, or dict. Found {type(emit_policy)} instead.')

        self._emit_policy = emit_policy
        if isinstance(self._emit_policy, dict):
            self._emit_policy = _dict_to_emit_policy(self._emit_policy)

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
                self.key_extractor = lambda element: element.get(key)
            elif isinstance(key, list):
                self.key_extractor = lambda element: [element.get(single_key) for single_key in key]
            else:
                raise TypeError(f'key is expected to be either a callable or string but got {type(key)}')

    def _init(self):
        super()._init()
        self._events_in_batch = {}
        self._emit_worker_running = False
        self._terminate_worker = False
        self._timeout_task: Optional[asyncio.Task] = None

    def _check_unique_names(self, aggregates):
        unique_aggr_names = set()
        for aggr in aggregates:
            if aggr.name in unique_aggr_names:
                raise TypeError(f'Aggregates should have unique names. {aggr.name} already exists')
            unique_aggr_names.add(aggr.name)

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

    def _get_timestamp(self, event):
        event_timestamp = event.time
        if isinstance(event_timestamp, datetime):
            if isinstance(event_timestamp, pd.Timestamp) and event_timestamp.tzinfo is None:
                # timestamp for pandas timestamp gives the wrong result in case there is no timezone (ML-313)
                local_time_zone = datetime.now().astimezone().tzinfo
                event_timestamp = event_timestamp.replace(tzinfo=local_time_zone)
            event_timestamp = event_timestamp.timestamp() * 1000
        return event_timestamp

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

            event_timestamp = self._get_timestamp(event)

            safe_key = stringify_key(key)
            await self._table._lazy_load_key_with_aggregates(safe_key, event_timestamp)
            await self._table._aggregate(safe_key, element, event_timestamp)

            if isinstance(self._emit_policy, EmitEveryEvent):
                await self._emit_event(key, event)
            elif isinstance(self._emit_policy, EmitAfterMaxEvent):
                if safe_key in self._events_in_batch:
                    self._events_in_batch[safe_key]['counter'] += 1
                else:
                    event_dict = {'counter': 1, 'time': time.monotonic()}
                    self._events_in_batch[safe_key] = event_dict
                self._events_in_batch[safe_key]['event'] = event
                if self._emit_policy.timeout_secs and self._timeout_task is None:
                    self._timeout_task = asyncio.get_running_loop().create_task(self._sleep_and_emit())
                if self._events_in_batch[safe_key]['counter'] == self._emit_policy.max_events:
                    event_from_batch = self._events_in_batch.pop(safe_key, None)
                    if event_from_batch is not None:
                        await self._emit_event(key, event_from_batch['event'])
        except Exception as ex:
            raise ex

    async def _sleep_and_emit(self):
        while self._events_in_batch:
            key = next(iter(self._events_in_batch.keys()))
            delta_seconds = time.monotonic() - self._events_in_batch[key]['time']
            if delta_seconds < self._emit_policy.timeout_secs:
                await asyncio.sleep(self._emit_policy.timeout_secs - delta_seconds)
            event = self._events_in_batch.pop(key, None)
            if event is not None:
                await self._emit_event(key, event['event'])

        self._timeout_task = None

    # Emit a single event for the requested key
    async def _emit_event(self, key, event):
        event_timestamp = self._get_timestamp(event)

        safe_key = stringify_key(key)
        await self._table._lazy_load_key_with_aggregates(safe_key, event_timestamp)
        features = await self._table._get_features(safe_key, event_timestamp)
        for feature_name in list(features.keys()):
            if feature_name in self._aliases:
                new_feature_name = self._aliases[feature_name]
                if feature_name != new_feature_name:
                    features[new_feature_name] = features[feature_name]
                    del features[feature_name]
        features = self._augmentation_fn(event.body, features)

        for col in self._enrich_with:
            emitted_attr_name = self._aliases.get(col, None) or col
            if col in self._table._get_static_attrs(safe_key):
                features[emitted_attr_name] = self._table._get_static_attrs(safe_key)[col]
        event.key = key
        event.body = features
        await self._do_downstream(event)

    # Emit multiple events for every key in the store with the current time
    async def _emit_all_events(self, timestamp):
        for key in self._table._get_keys():
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
    :param table: A Table object or name for persistence of aggregations.
        If a table name is provided, it will be looked up in the context object passed in kwargs.
    :param key: Key field to aggregate by, accepts either a string representing the key field or a key extracting function.
     Defaults to the key in the event's metadata. (Optional). Can be list of keys
    :param augmentation_fn: Function that augments the features into the event's body. Defaults to updating a dict. (Optional)
    :param aliases: Dictionary specifying aliases for enriched or aggregate columns, of the
     format `{'col_name': 'new_col_name'}`. (Optional)
    :param options: Enum flags specifying query options. (Optional)
    """

    def __init__(self, features: List[str], table: Union[Table, str], key: Union[str, List[str], Callable[[Event], object], None] = None,
                 augmentation_fn: Optional[Callable[[Event, Dict[str, object]], Event]] = None,
                 aliases: Optional[Dict[str, str]] = None,
                 fixed_window_type: Optional[FixedWindowType] = FixedWindowType.CurrentOpenWindow, **kwargs):
        self._aggrs = []
        self._enrich_cols = []
        resolved_aggrs = {}
        for feature in features:
            if table.supports_aggregations() and re.match(r'.*_[a-z]+_[0-9]+[smhd]$', feature):
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
            self._aggrs.append(FieldAggregator(name=feature, field=None, aggr=[aggr], windows=SlidingWindows(windows)))
        if isinstance(table, Table):
            other_table = table._clone() if table._aggregates is not None else table
        else:
            other_table = table  # str - pass table string along with the context object
        AggregateByKey.__init__(self, self._aggrs, other_table, key, augmentation_fn=augmentation_fn,
                                enrich_with=self._enrich_cols, aliases=aliases, use_windows_from_schema=True, **kwargs)
        self._table._aggregations_read_only = True
        self._table.fixed_window_type = fixed_window_type

    async def _do(self, event):
        if event == _termination_obj:
            self._terminate_worker = True
            return await self._do_downstream(_termination_obj)

        element = event.body
        key = event.key
        if self.key_extractor:
            if element:
                key = self.key_extractor(element)
            if key is None or key == [None] or element is None:
                event.body = None
                await self._do_downstream(event)
                return
        await self._emit_event(key, event)

    def _check_unique_names(self, aggregates):
        pass
