import asyncio
import copy
from typing import Optional

import aiohttp

from .dtypes import _termination_obj, Event, FlowError, V3ioError


class Flow:
    def __init__(self, name=None, full_event=False, termination_result_fn=lambda x, y: None, context=None, **kwargs):
        self._outlets = []
        self._full_event = full_event
        self._termination_result_fn = termination_result_fn
        self.context = context
        self._closeables = []
        if name:
            self.name = name
        else:
            self.name = type(self).__name__

    def to(self, outlet):
        self._outlets.append(outlet)
        return outlet

    def run(self):
        for outlet in self._outlets:
            self._closeables.extend(outlet.run())
        return self._closeables

    async def run_async(self):
        raise NotImplementedError

    async def _do(self, event):
        raise NotImplementedError

    async def _do_downstream(self, event):
        if not self._outlets:
            return
        if event is _termination_obj:
            termination_result = await self._outlets[0]._do(_termination_obj)
            for i in range(1, len(self._outlets)):
                termination_result = self._termination_result_fn(termination_result, await self._outlets[i]._do(_termination_obj))
            return termination_result
        tasks = []
        for i in range(len(self._outlets)):
            tasks.append(asyncio.get_running_loop().create_task(self._outlets[i]._do(event)))
        for task in tasks:
            await task

    def _get_safe_event_or_body(self, event):
        if self._full_event:
            new_event = copy.copy(event)
            return new_event
        else:
            return event.body

    def _user_fn_output_to_event(self, event, fn_result):
        if self._full_event:
            return fn_result
        else:
            mapped_event = copy.copy(event)
            mapped_event.body = fn_result
            return mapped_event


class Choice(Flow):
    """Redirects each input element into at most one of multiple downstreams.

    :param choice_array: a list of (downstream, condition) tuples, where downstream is a step and condition is a function. The first
    condition in the list to evaluate as true for an input element causes that element to be redirected to that downstream step.
    :type choice_array: tuple of (Flow, Function (Event=>boolean))

    :param default: a default step for events that did not match any condition in choice_array. If not set, elements that don't match any
    condition will be discarded.
    :type default: Flow
    :param name: Name of this step, as it should appear in logs. Defaults to class name (Choice).
    :type name: string
    :param full_event: Whether user functions should receive and/or return Event objects (when True), or only the payload (when False).
    Defaults to False.
    :type full_event: boolean
    """

    def __init__(self, choice_array, default=None, **kwargs):
        Flow.__init__(self, **kwargs)

        self._choice_array = choice_array
        for outlet, _ in choice_array:
            self._outlets.append(outlet)

        if default:
            self._outlets.append(default)
        self._default = default

    async def _do(self, event):
        if not self._outlets or event is _termination_obj:
            return await super()._do_downstream(event)
        chosen_outlet = None
        element = self._get_safe_event_or_body(event)
        for outlet, condition in self._choice_array:
            if condition(element):
                chosen_outlet = outlet
                break
        if chosen_outlet:
            await chosen_outlet._do(event)
        elif self._default:
            await self._default._do(event)


class UnaryFunctionFlow(Flow):
    def __init__(self, fn, **kwargs):
        super().__init__(**kwargs)
        if not callable(fn):
            raise TypeError(f'Expected a callable, got {type(fn)}')
        self._is_async = asyncio.iscoroutinefunction(fn)
        self._fn = fn

    async def _call(self, element):
        res = self._fn(element)
        if self._is_async:
            res = await res
        return res

    async def _do_internal(self, element, fn_result):
        raise NotImplementedError()

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            element = self._get_safe_event_or_body(event)
            fn_result = await self._call(element)
            await self._do_internal(event, fn_result)


class Map(UnaryFunctionFlow):
    """
    Maps, or transforms, incoming events using a user-provided function.
    :param fn: Function to apply to each event
    :type fn: Function (Event=>Event)
    :param name: Name of this step, as it should appear in logs. Defaults to class name (Map).
    :type name: string
    :param full_event: Whether user functions should receive and/or return Event objects (when True), or only the payload (when False).
    Defaults to False.
    :type full_event: boolean
    """

    async def _do_internal(self, event, fn_result):
        mapped_event = self._user_fn_output_to_event(event, fn_result)
        await self._do_downstream(mapped_event)


class Filter(UnaryFunctionFlow):
    """
        Filters events based on a user-provided function.
        :param fn: Function to decide whether to keep each event.
        :type fn: Function (Event=>boolean)
        :param name: Name of this step, as it should appear in logs. Defaults to class name (Filter).
        :type name: string
        :param full_event: Whether user functions should receive and/or return Event objects (when True), or only the payload (when False).
        Defaults to False.
        :type full_event: boolean
    """

    async def _do_internal(self, event, keep):
        if keep:
            await self._do_downstream(event)


class FlatMap(UnaryFunctionFlow):
    """
        Maps, or transforms, each incoming event into any number of events.
        :param fn: Function to transform each event to a list of events.
        :type fn: Function (Event=>list of Event)
        :param name: Name of this step, as it should appear in logs. Defaults to class name (FlatMap).
        :type name: string
        :param full_event: Whether user functions should receive and/or return Event objects (when True), or only the payload (when False).
        Defaults to False.
        :type full_event: boolean
    """

    async def _do_internal(self, event, fn_result):
        for fn_result_element in fn_result:
            mapped_event = self._user_fn_output_to_event(event, fn_result_element)
            await self._do_downstream(mapped_event)


class FunctionWithStateFlow(Flow):
    def __init__(self, initial_state, fn, group_by_key=False, **kwargs):
        super().__init__(**kwargs)
        if not callable(fn):
            raise TypeError(f'Expected a callable, got {type(fn)}')
        self._is_async = asyncio.iscoroutinefunction(fn)
        self._state = initial_state
        self._fn = fn
        self._group_by_key = group_by_key

    async def _call(self, event):
        element = self._get_safe_event_or_body(event)
        if self._group_by_key:
            if isinstance(self._state, Table):
                key_data = await self._state.get_or_load_key(event.key)
            else:
                key_data = self._state[event.key]
            res, self._state[event.key] = self._fn(element, key_data)
        else:
            res, self._state = self._fn(element, self._state)
        if self._is_async:
            res = await res
        return res

    async def _do_internal(self, element, fn_result):
        raise NotImplementedError()

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:

            fn_result = await self._call(event)
            await self._do_internal(event, fn_result)


class MapWithState(FunctionWithStateFlow):
    async def _do_internal(self, event, mapped_element):
        mapped_event = self._user_fn_output_to_event(event, mapped_element)
        await self._do_downstream(mapped_event)


class MapClass(Flow):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._is_async = asyncio.iscoroutinefunction(self.do)
        self._filter = False

    def filter(self):
        # used in the .do() code to signal filtering
        self._filter = True

    def do(self, event):
        raise NotImplementedError()

    async def _call(self, event):
        res = self.do(event)
        if self._is_async:
            res = await res
        return res

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            element = self._get_safe_event_or_body(event)
            fn_result = await self._call(element)
            if not self._filter:
                mapped_event = self._user_fn_output_to_event(event, fn_result)
                await self._do_downstream(mapped_event)
            else:
                self._filter = False  # clear the flag for future runs


class Complete(Flow):
    """
        Completes the AwaitableResult associated with incoming events.
        :param name: Name of this step, as it should appear in logs. Defaults to class name (Complete).
        :type name: string
        :param full_event: Whether to complete with an Event object (when True) or only the payload (when False). Default to False.
        :type full_event: boolean
    """

    async def _do(self, event):
        termination_result = await self._do_downstream(event)
        if event is not _termination_obj:
            result = self._get_safe_event_or_body(event)
            res = event._awaitable_result._set_result(result)
            if res:
                await res
        return termination_result


class Reduce(Flow):
    """
        Reduces incoming events into a single value which is returned upon the successful termination of the flow.
        :param initial_value: Starting value. When the first event is received, fn will be appled to the initial_value and that event.
        :type initial_value: object
        :param fn: Function to apply to the current value and each event.
        :type fn: Function ((object, Event) => object)
        :param name: Name of this step, as it should appear in logs. Defaults to class name (Reduce).
        :type name: string
        :param full_event: Whether user functions should receive and/or return Event objects (when True), or only the payload (when False).
        Defaults to False.
        :type full_event: boolean
    """

    def __init__(self, initial_value, fn, **kwargs):
        super().__init__(**kwargs)
        if not callable(fn):
            raise TypeError(f'Expected a callable, got {type(fn)}')
        self._is_async = asyncio.iscoroutinefunction(fn)
        self._fn = fn
        self._result = initial_value

    def to(self, outlet):
        raise ValueError("Reduce is a terminal step. It cannot be piped further.")

    async def _do(self, event):
        if event is _termination_obj:
            return self._result
        else:
            if self._full_event:
                elem = event
            else:
                elem = event.body
            res = self._fn(self._result, elem)
            if self._is_async:
                res = await res
            self._result = res


class HttpRequest:
    def __init__(self, method, url, body, headers=None):
        self.method = method
        self.url = url
        self.body = body
        if headers is None:
            headers = {}
        self.headers = headers


class HttpResponse:
    def __init__(self, status, body):
        self.status = status
        self.body = body


class _ConcurrentJobExecution(Flow):
    def __init__(self, max_in_flight=8, **kwargs):
        Flow.__init__(self, **kwargs)
        self._max_in_flight = max_in_flight
        self._q = None

    async def _worker(self):
        try:
            while True:
                job = await self._q.get()
                if job is _termination_obj:
                    break
                event = job[0]
                completed = await job[1]
                await self._handle_completed(event, completed)
        except BaseException as ex:
            if not self._q.empty():
                await self._q.get()
            raise ex
        finally:
            await self._cleanup()

    async def _process_event(self, event):
        raise NotImplementedError()

    async def _handle_completed(self, event, response):
        raise NotImplementedError()

    async def _cleanup(self):
        pass

    async def _lazy_init(self):
        pass

    async def _do(self, event):
        if not self._q:
            await self._lazy_init()
            self._q = asyncio.queues.Queue(self._max_in_flight)
            self._worker_awaitable = asyncio.get_running_loop().create_task(self._worker())

        if self._worker_awaitable.done():
            await self._worker_awaitable
            raise FlowError("ConcurrentJobExecution worker has already terminated")

        if event is _termination_obj:
            await self._q.put(_termination_obj)
            await self._worker_awaitable
            return await self._do_downstream(_termination_obj)
        else:
            task = self._process_event(event)
            await self._q.put((event, asyncio.get_running_loop().create_task(task)))
            if self._worker_awaitable.done():
                await self._worker_awaitable


class _PendingEvent:
    def __init__(self):
        self.in_flight = []
        self.pending = []


class _ConcurrentByKeyJobExecution(Flow):
    def __init__(self, max_in_flight=8, **kwargs):
        Flow.__init__(self, **kwargs)
        self._max_in_flight = max_in_flight
        self._q = None
        self._pending_by_key = {}

    async def _worker(self):
        try:
            while True:
                job = await self._q.get()
                if job is _termination_obj:
                    for pending_event in self._pending_by_key.values():
                        if pending_event.pending and not pending_event.in_flight:
                            resp = await self._process_event(pending_event.pending[0])
                            for event in pending_event.pending:
                                await self._handle_completed(event, resp)
                    if self._q.empty():
                        break
                    else:
                        await self._q.put(_termination_obj)
                        continue

                event = job[0]
                completed = await job[1]

                for event in self._pending_by_key[event.key].in_flight:
                    await self._handle_completed(event, completed)
                self._pending_by_key[event.key].in_flight = []

                # If we got more pending events for the same key process them
                if self._pending_by_key[event.key].pending:
                    first_event = self._pending_by_key[event.key].pending[0]
                    self._pending_by_key[event.key].in_flight = self._pending_by_key[event.key].pending
                    self._pending_by_key[event.key].pending = []

                    task = self._process_event(first_event)
                    await self._q.put((event, asyncio.get_running_loop().create_task(task)))
                else:
                    del self._pending_by_key[event.key]
        except BaseException as ex:
            if not self._q.empty():
                await self._q.get()
            raise ex
        finally:
            await self._cleanup()

    async def _do(self, event):
        if not self._q:
            await self._lazy_init()
            self._q = asyncio.queues.Queue(self._max_in_flight)
            self._worker_awaitable = asyncio.get_running_loop().create_task(self._worker())

        if self._worker_awaitable.done():
            await self._worker_awaitable
            raise FlowError("ConcurrentByKeyJobExecution worker has already terminated")

        if event is _termination_obj:
            await self._q.put(_termination_obj)
            await self._worker_awaitable
            return await self._do_downstream(_termination_obj)
        else:
            # Initializing the key with 2 lists. One for pending requests and one for requests that an update request has been issued for.
            if event.key not in self._pending_by_key:
                self._pending_by_key[event.key] = _PendingEvent()

            # If there is a current update in flight for the key, add the event to the pending list. Otherwise update the key.
            self._pending_by_key[event.key].pending.append(event)
            if len(self._pending_by_key[event.key].in_flight) == 0:
                self._pending_by_key[event.key].in_flight = self._pending_by_key[event.key].pending
                self._pending_by_key[event.key].pending = []

                task = self._process_event(event)
                await self._q.put((event, asyncio.get_running_loop().create_task(task)))
                if self._worker_awaitable.done():
                    await self._worker_awaitable

    async def _process_event(self, event):
        raise NotImplementedError()

    async def _handle_completed(self, event, response):
        raise NotImplementedError()

    async def _cleanup(self):
        pass

    async def _lazy_init(self):
        pass


class JoinWithHttp(_ConcurrentJobExecution):
    """Joins each event with data from any HTTP source. Used for event augmentation.

    :param request_builder: Creates an HTTP request from the event. This request is then sent to its destination.
    :type request_builder: Function (Event=>HttpRequest)
    :param join_from_response: Joins the original event with the HTTP response into a new event.
    :type join_from_response: Function ((Event, HttpResponse)=>Event)
    :param name: Name of this step, as it should appear in logs. Defaults to class name (JoinWithHttp).
    :type name: string
    :param full_event: Whether user functions should receive and/or return Event objects (when True), or only the payload (when False).
    Defaults to False.
    :type full_event: boolean
    """

    def __init__(self, request_builder, join_from_response, **kwargs):
        super().__init__(**kwargs)
        self._request_builder = request_builder
        self._join_from_response = join_from_response

        self._client_session = None

    async def _lazy_init(self):
        connector = aiohttp.TCPConnector()
        self._client_session = aiohttp.ClientSession(connector=connector)

    async def _cleanup(self):
        await self._client_session.close()

    async def _process_event(self, event):
        req = self._request_builder(event)
        return await self._client_session.request(req.method, req.url, headers=req.headers, data=req.body, ssl=False)

    async def _handle_completed(self, event, response):
        response_body = await response.text()
        joined_element = self._join_from_response(event.body, HttpResponse(response.status, response_body))
        if joined_element is not None:
            new_event = self._user_fn_output_to_event(event, joined_element)
            await self._do_downstream(new_event)


class _Batching(Flow):
    def __init__(self, max_events: Optional[int] = None, timeout_secs=None, **kwargs):
        super().__init__(**kwargs)
        self._max_events = max_events
        self._event_count = 0
        self._batch = []
        self._batch_time = None
        self._timeout_task = None

        self._timeout_secs = timeout_secs
        if self._timeout_secs is not None and self._timeout_secs <= 0:
            raise ValueError('Batch timeout cannot be 0 or negative')

    async def _emit(self, batch, batch_time):
        raise NotImplementedError

    async def _terminate(self):
        raise NotImplementedError

    async def _sleep_and_emit(self):
        await asyncio.sleep(self._timeout_secs)
        await self._emit_batch()

    async def _do(self, event):
        if event is _termination_obj:
            if self._timeout_task and not self._timeout_task.cancelled():
                self._timeout_task.cancel()
            await self._emit_batch()
            return await self._terminate()
        else:
            if len(self._batch) == 0:
                self._batch_time = event.time
                if self._timeout_secs:
                    self._timeout_task = asyncio.get_running_loop().create_task(self._sleep_and_emit())

            self._event_count = self._event_count + 1
            self._batch.append(self._get_safe_event_or_body(event))

            if self._event_count == self._max_events:
                if self._timeout_task and not self._timeout_task.cancelled():
                    self._timeout_task.cancel()
                await self._emit_batch()

    async def _emit_batch(self):
        if len(self._batch) > 0:
            batch_to_emit = self._batch
            batch_time = self._batch_time
            self._batch = []
            self._batch_time = None
            self._event_count = 0

            await self._emit(batch_to_emit, batch_time)


class Batch(_Batching):
    """
    Batches events into lists of up to max_events events. Each emitted list contained max_events events, unless timeout_secs seconds
    have passed since the first event in the batch was received, at which the batch is emitted with potentially fewer than max_events
    event.
    :param max_events: Maximum number of events per emitted batch. Set to None to emit all events in one batch on flow termination.
    :type max_events: int or None
    :param timeout_secs: Maximum number of seconds to wait before a batch is emitted.
    :type timeout_secs: int
    """

    def __init__(self, max_events: Optional[int], timeout_secs=None, **kwargs):
        super().__init__(max_events, timeout_secs, **kwargs)

    async def _emit(self, batch, batch_time):
        event = Event(batch, time=batch_time)
        return await self._do_downstream(event)

    async def _terminate(self):
        return await self._do_downstream(_termination_obj)


class JoinWithV3IOTable(_ConcurrentJobExecution):
    """Joins each event with a V3IO table. Used for event augmentation.

    :param storage: V3IO driver.
    :type storage: V3ioDriver
    :param key_extractor: Function for extracting the key for table access from an event.
    :type key_extractor: Function (Event=>string)
    :param join_function: Joins the original event with relevant data received from V3IO.
    :type join_function: Function ((Event, dict)=>Event)
    :param table_path: Path to the table in V3IO.
    :type table_path: string
    :param attributes: A comma-separated list of attributes to be requested from V3IO. Defaults to '*' (all user attributes).
    :type attributes: string
    :param name: Name of this step, as it should appear in logs. Defaults to class name (JoinWithV3IOTable).
    :type name: string
    :param full_event: Whether user functions should receive and/or return Event objects (when True), or only the payload (when False).
    Defaults to False.
    :type full_event: boolean
    """

    def __init__(self, storage, key_extractor, join_function, table_path, attributes='*', **kwargs):
        super().__init__(**kwargs)

        self._storage = storage

        self._key_extractor = key_extractor
        self._join_function = join_function

        self._container, self._table_path = _split_path(table_path)
        self._attributes = attributes

    async def _process_event(self, event):
        key = str(self._key_extractor(self._get_safe_event_or_body(event)))
        return await self._storage._get_item(self._container, self._table_path, key, self._attributes)

    async def _handle_completed(self, event, response):
        if response.status_code == 200:
            response_object = response.output.item
            joined = self._join_function(self._get_safe_event_or_body(event), response_object)
            if joined is not None:
                new_event = self._user_fn_output_to_event(event, joined)
                await self._do_downstream(new_event)
        elif response.status_code == 404:
            return None
        else:
            raise V3ioError(f'Failed to get item. Response status code was {response.status_code}: {response.body}')

    async def _cleanup(self):
        await self._storage.close()


class JoinWithTable(_ConcurrentJobExecution):
    """Joins each event with data from the given table.

    :param table: Table to join with.
    :type table: Table
    :param key_extractor: Function for extracting the key for table access from an event.
    :type key_extractor: Function (Event=>string)
    :param attributes: A comma-separated list of attributes to be requested from V3IO. Defaults to '*' (all user attributes).
    :type attributes: list of string
    :param join_function: Joins the original event with relevant data received from V3IO.
    :type join_function: Function ((Event, dict)=>Event)
    :param name: Name of this step, as it should appear in logs. Defaults to class name (JoinWithV3IOTable).
    :type name: string
    :param full_event: Whether user functions should receive and/or return Event objects (when True), or only the payload (when False).
    Defaults to False.
    :type full_event: boolean
    """

    def __init__(self, table, key_extractor, attributes='*', join_function=None, **kwargs):
        super().__init__(**kwargs)

        self._table = table
        self._closeables = [table]

        self._key_extractor = key_extractor

        def _default_join_fn(event, join_res):
            event.update(join_res)
            return event
        self._join_function = join_function or _default_join_fn

        self._attributes = attributes

    async def _process_event(self, event):
        key = str(self._key_extractor(self._get_safe_event_or_body(event)))
        return await self._table.get_or_load_key(key, self._attributes)

    async def _handle_completed(self, event, response):
        joined = self._join_function(self._get_safe_event_or_body(event), response)
        if joined is not None:
            new_event = self._user_fn_output_to_event(event, joined)
            await self._do_downstream(new_event)


def build_flow(steps):
    """Builds a flow from a list of steps, by chaining the steps according to their order in the list.
    Nested lists are used to represent branches in the flow.

    Examples:
        build_flow([step1, step2, step3])
        is equivalent to
        step1.to(step2).to(step3)

        build_flow([step1, [step2a, step2b], step3])
        is equivalent to
        step1.to(step2a)
        step1.to(step3)
        step2a.to(step2b)

    :param steps: a potentially nested list of steps
    :returns: the first step
    :rtype: Flow
    """
    if len(steps) == 0:
        raise ValueError('Cannot build an empty flow')
    cur_step = steps[0]
    for next_step in steps[1:]:
        if isinstance(next_step, list):
            cur_step.to(build_flow(next_step))
        else:
            cur_step.to(next_step)
            cur_step = next_step
    return steps[0]


def _split_path(path):
    while path.startswith('/'):
        path = path[1:]

    parts = path.split('/', 1)
    if len(parts) == 1:
        return parts[0], '/'
    else:
        return parts[0], f'/{parts[1]}'


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

    def __init__(self, table_path, storage, partitioned_by_key=True):
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

    async def get_or_load_key(self, key, attributes=None):
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

    async def persist_key(self, key):
        aggr_by_key = None
        if self._aggregation_store:
            aggr_by_key = self._aggregation_store[key]
        additional_cache_data_by_key = self._cache.get(key, None)
        await self._storage._save_key(self._container, self._table_path, key, aggr_by_key, self._partitioned_by_key,
                                      additional_cache_data_by_key)

    async def close(self):
        await self._storage.close()
