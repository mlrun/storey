import asyncio
import base64
import copy
import csv
import io
import json
import os
import queue
import threading
from datetime import datetime, timezone
import uuid
import random
import pickle

import aiofiles
import aiohttp

from .utils import schema_file_name

import v3io.aio.dataplane

_termination_obj = object()


class FlowError(Exception):
    pass


class V3ioError(Exception):
    pass


class Flow:
    def __init__(self, full_event=False, termination_result_fn=lambda x, y: None):
        self._outlets = []
        self._full_event = full_event
        self._termination_result_fn = termination_result_fn

    def to(self, outlet):
        self._outlets.append(outlet)
        return outlet

    def run(self):
        for outlet in self._outlets:
            outlet.run()

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
        self.id = id or uuid.uuid4().hex
        self.key = key
        self.time = time or datetime.now(timezone.utc)
        self.headers = headers
        self.method = method
        self.path = path
        self.content_type = content_type
        self._awaitable_result = awaitable_result


class AwaitableResult:
    def __init__(self):
        self._q = queue.Queue(1)

    def await_result(self):
        return self._q.get()

    def _set_result(self, element):
        self._q.put(element)

    def _set_error(self, element):
        pass


class FlowController:
    """Used to emit events into the associated flow, terminate the flow, and await the flow's termination.
    To be used from a synchronous context.
    """

    def __init__(self, emit_fn, await_termination_fn):
        self._emit_fn = emit_fn
        self._await_termination_fn = await_termination_fn

    def emit(self, element, key=None, event_time=None, return_awaitable_result=False):
        """Emits an event into the associated flow.

        :param element: The event data, or payload. To set metadata as well, pass an Event object.
        :type element: object
        :param key: The event key (optional)
        :type key: string
        :param event_time: The event time (default to current time, UTC).
        :type event_time: datetime
        :param return_awaitable_result: Whether an AwaitableResult object should be returned. Defaults to False.
        :type return_awaitable_result: boolean

        :returns: AsyncAwaitableResult if return_awaitable_result is True. None otherwise.
        """
        if event_time is None:
            event_time = datetime.now(timezone.utc)
        if hasattr(element, 'id'):
            event = element
            if key:
                event.key = key
            if event_time:
                event.time = event_time
        else:
            event = Event(element, key=key, time=event_time)
        awaitable_result = None
        if return_awaitable_result:
            awaitable_result = AwaitableResult()
        event._awaitable_result = awaitable_result
        self._emit_fn(event)
        return awaitable_result

    def terminate(self):
        """Terminates the associated flow."""
        self._emit_fn(_termination_obj)

    def await_termination(self):
        """Awaits the termination of the flow. To be called after terminate. Returns the termination result of the flow (if any)."""
        return self._await_termination_fn()


class FlowAwaiter:
    def __init__(self, await_termination_fn):
        self._await_termination_fn = await_termination_fn

    def await_termination(self):
        return self._await_termination_fn()


class Source(Flow):
    """
    Synchronous entry point into a flow. Produces a FlowController when run, for use from inside a synchronous context. See AsyncSource
    for use from inside an async context.

    :param buffer_size: size of the incoming event buffer. Defaults to 1.
    :type buffer_size: int
    """

    def __init__(self, buffer_size=1, **kwargs):
        super().__init__(**kwargs)
        if buffer_size <= 0:
            raise ValueError('Buffer size must be positive')
        self._q = queue.Queue(buffer_size)
        self._termination_q = queue.Queue(1)
        self._ex = None

    async def _run_loop(self):
        loop = asyncio.get_running_loop()
        self._termination_future = asyncio.get_running_loop().create_future()

        while True:
            event = await loop.run_in_executor(None, self._q.get)
            try:
                termination_result = await self._do_downstream(event)
                if event is _termination_obj:
                    self._termination_future.set_result(termination_result)
            except BaseException as ex:
                self._ex = ex
                if not self._q.empty():
                    self._q.get()
                self._termination_future.set_result(None)
                break
            if event is _termination_obj:
                break

    def _loop_thread_main(self):
        asyncio.run(self._run_loop())
        self._termination_q.put(self._ex)

    def _raise_on_error(self, ex):
        if ex:
            raise FlowError('Flow execution terminated due to an error') from self._ex

    def _emit(self, event):
        self._raise_on_error(self._ex)
        self._q.put(event)
        self._raise_on_error(self._ex)

    def run(self):
        super().run()

        thread = threading.Thread(target=self._loop_thread_main)
        thread.start()

        def raise_error_or_return_termination_result():
            self._raise_on_error(self._termination_q.get())
            return self._termination_future.result()

        return FlowController(self._emit, raise_error_or_return_termination_result)


class AsyncAwaitableResult:
    def __init__(self):
        self._q = asyncio.Queue(1)

    async def await_result(self):
        return await self._q.get()

    async def _set_result(self, element):
        await self._q.put(element)

    async def _set_error(self, ex):
        await self._set_result(ex)


class AsyncFlowController:
    """
    Used to emit events into the associated flow, terminate the flow, and await the flow's termination. To be used from inside an async def.
    """

    def __init__(self, emit_fn, loop_task):
        self._emit_fn = emit_fn
        self._loop_task = loop_task

    async def emit(self, element, key=None, event_time=None, await_result=False):
        """Emits an event into the associated flow.

        :param element: The event data, or payload. To set metadata as well, pass an Event object.
        :type element: object
        :param key: The event key (optional)
        :type key: string
        :param event_time: The event time (default to current time, UTC).
        :type event_time: datetime
        :param await_result: Whether to await a result from the flow (as signaled by the Complete step). Defaults to False.
        :type await_result: boolean

        :returns: The result received from the flow if await_result is True. None otherwise.
        :rtype: object
        """
        if event_time is None:
            event_time = datetime.now(timezone.utc)
        if hasattr(element, 'id'):
            event = element
            if key:
                event.key = key
            if event_time:
                event = event_time
        else:
            event = Event(element, key=key, time=event_time)
        awaitable = None
        if await_result:
            awaitable = AsyncAwaitableResult()
        event._awaitable_result = awaitable
        await self._emit_fn(event)
        if await_result:
            result = await awaitable.await_result()
            if isinstance(result, BaseException):
                raise result
            return result

    async def terminate(self):
        """Terminates the associated flow."""
        await self._emit_fn(_termination_obj)

    async def await_termination(self):
        """Awaits the termination of the flow. To be called after terminate. Returns the termination result of the flow (if any)."""
        return await self._loop_task


class AsyncSource(Flow):
    """
    Asynchronous entry point into a flow. Produces an AsyncFlowController when run, for use from inside an async def.
    See Source for use from inside a synchronous context.

    :param buffer_size: size of the incoming event buffer. Defaults to 1.
    :type buffer_size: int
    """

    def __init__(self, buffer_size=1, **kwargs):
        super().__init__(**kwargs)
        if buffer_size <= 0:
            raise ValueError('Buffer size must be positive')
        self._q = asyncio.Queue(buffer_size, loop=asyncio.get_running_loop())
        self._ex = None

    async def _run_loop(self):
        while True:
            event = await self._q.get()
            try:
                termination_result = await self._do_downstream(event)
                if event is _termination_obj:
                    return termination_result
            except BaseException as ex:
                self._ex = ex
                if event._awaitable_result:
                    awaitable = event._awaitable_result._set_error(ex)
                    if awaitable:
                        await awaitable
                if not self._q.empty():
                    await self._q.get()
                return None

    def _raise_on_error(self):
        if self._ex:
            raise FlowError('Flow execution terminated due to an error') from self._ex

    async def _emit(self, event):
        self._raise_on_error()
        await self._q.put(event)
        self._raise_on_error()

    async def run(self):
        loop_task = asyncio.get_running_loop().create_task(self._run_loop())
        return AsyncFlowController(self._emit, loop_task)


class ReadCSV(Flow):
    """
    Reads CSV files as input source for a flow.

    :param paths: paths to CSV files
    :type paths: list of string
    :param with_header: whether CSV files have a header or not. Defaults to False.
    :type with_header: boolean
    :param build_dict: whether to format each record produced from the input file as a dictionary (as opposed to a list). Default to False.
    :type build_dict: boolean
    :param key_field: the CSV field to be use as the key for events. May be an int (field index) or string (field name) if with_header
    is True. Defaults to None (no key).
    :type key_field: int or string
    :param timestamp_field: the CSV field to be parsed as the timestamp for events. May be an int (field index) or string (field name) if
    with_header is True. Defaults to None (no timestamp field).
    :type timestamp_field: int or string
    :param timestamp_format: timestamp format as defined in datetime.strptime(). Default to ISO-8601 as defined in datetime.fromisoformat().
    :type timestamp_format: string
    """

    def __init__(self, paths, with_header=False, build_dict=False, key_field=None, timestamp_field=None, timestamp_format=None, **kwargs):
        super().__init__(**kwargs)
        if isinstance(paths, str):
            paths = [paths]
        self._paths = paths
        self._with_header = with_header
        self._build_dict = build_dict
        self._key_field = key_field
        self._timestamp_field = timestamp_field
        self._timestamp_format = timestamp_format

        self._termination_q = queue.Queue(1)
        self._ex = None

        if not with_header and isinstance(key_field, str):
            raise ValueError('key_field can only be set to an integer when with_header is false')
        if not with_header and isinstance(timestamp_field, str):
            raise ValueError('timestamp_field can only be set to an integer when with_header is false')

    async def _run_loop(self):
        self._termination_future = asyncio.get_running_loop().create_future()

        try:
            for path in self._paths:
                async with aiofiles.open(path, mode='r') as f:
                    header = None
                    field_name_to_index = None
                    if self._with_header:
                        line = await f.readline()
                        header = next(csv.reader([line]))
                        field_name_to_index = {}
                        for i in range(len(header)):
                            field_name_to_index[header[i]] = i
                    async for line in f:
                        parsed_line = next(csv.reader([line]))
                        element = parsed_line
                        key = None
                        if header:
                            if len(parsed_line) != len(header):
                                raise ValueError(f'CSV line with {len(parsed_line)} fields did not match header with {len(header)} fields')
                            if self._build_dict:
                                element = {}
                                for i in range(len(parsed_line)):
                                    element[header[i]] = parsed_line[i]
                        if self._key_field:
                            key_field = self._key_field
                            if self._with_header and isinstance(key_field, str):
                                key_field = field_name_to_index[key_field]
                            key = parsed_line[key_field]
                        if self._timestamp_field:
                            timestamp_field = self._timestamp_field
                            if self._with_header and isinstance(timestamp_field, str):
                                timestamp_field = field_name_to_index[timestamp_field]
                            timestamp_str = parsed_line[timestamp_field]
                            if self._timestamp_format:
                                timestamp = datetime.strptime(timestamp_str, self._timestamp_format)
                            else:
                                timestamp = datetime.fromisoformat(timestamp_str)
                        else:
                            timestamp = datetime.now()
                        await self._do_downstream(Event(element, key=key, time=timestamp))
            termination_result = await self._do_downstream(_termination_obj)
            self._termination_future.set_result(termination_result)
        except BaseException as ex:
            self._ex = ex
            self._termination_future.set_result(None)

    def _loop_thread_main(self):
        asyncio.run(self._run_loop())
        self._termination_q.put(self._ex)

    def _raise_on_error(self, ex):
        if ex:
            raise FlowError('Flow execution terminated due to an error') from self._ex

    def run(self):
        super().run()

        thread = threading.Thread(target=self._loop_thread_main)
        thread.start()

        def raise_error_or_return_termination_result():
            self._raise_on_error(self._termination_q.get())
            return self._termination_future.result()

        return FlowAwaiter(raise_error_or_return_termination_result)


class WriteCSV(Flow):
    """
    Writes events to a CSV file.

    :param path: path where CSV file will be written.
    :type path: string
    :param event_to_line: function to transform an event to a CSV line (represented as a list).
    :type event_to_line: Function (Event=>list of string)
    :param header: a header for the output file.
    :type header: list of string
    :param full_event: Whether user functions should receive and/or return Event objects (when True), or only the payload (when False).
    Defaults to False.
    :type full_event: boolean
    """

    def __init__(self, path, event_to_line, header=None, **kwargs):
        super().__init__(**kwargs)
        self._path = path
        self._header = header
        self._event_to_line = event_to_line
        self._open_file = None

    async def _do(self, event):
        if event is _termination_obj:
            if self._open_file:
                await self._open_file.close()
            return await self._do_downstream(_termination_obj)
        try:
            if not self._open_file:
                self._open_file = await aiofiles.open(self._path, mode='w')
                linebuf = io.StringIO()
                csv_writer = csv.writer(linebuf)
                if self._header:
                    csv_writer.writerow(self._header)
                line = linebuf.getvalue()
                await self._open_file.write(line)
            line_arr = self._event_to_line(self._get_safe_event_or_body(event))
            linebuf = io.StringIO()
            csv_writer = csv.writer(linebuf)
            csv_writer.writerow(line_arr)
            line = linebuf.getvalue()
            await self._open_file.write(line)
        except BaseException as ex:
            if self._open_file:
                await self._open_file.close()
            raise ex


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
            if isinstance(self._state, Cache):
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


class Complete(Flow):
    """
        Completes the AwaitableResult associated with incoming events.
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


class NeedsV3ioAccess:
    def __init__(self, webapi=None, access_key=None):
        webapi = webapi or os.getenv('V3IO_API')
        if not webapi:
            raise ValueError('Missing webapi parameter or V3IO_API environment variable')

        if not webapi.startswith('http://') and not webapi.startswith('https://'):
            webapi = f'http://{webapi}'

        self._webapi_url = webapi

        access_key = access_key or os.getenv('V3IO_ACCESS_KEY')
        if not access_key:
            raise ValueError('Missing access_key parameter or V3IO_ACCESS_KEY environment variable')

        self._access_key = access_key


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
            raise FlowError("JoinWithHttp worker has already terminated")

        if event is _termination_obj:
            await self._q.put(_termination_obj)
            await self._worker_awaitable
            return await self._do_downstream(_termination_obj)
        else:
            task = self._process_event(event)
            await self._q.put((event, asyncio.get_running_loop().create_task(task)))
            if self._worker_awaitable.done():
                await self._worker_awaitable


class JoinWithHttp(_ConcurrentJobExecution):
    """Joins each event with data from any HTTP source. Used for event augmentation.

    :param request_builder: Creates an HTTP request from the event. This request is then sent to its destination.
    :type request_builder: Function (Event=>HttpRequest)
    :param join_from_response: Joins the original event with the HTTP response into a new event.
    :type join_from_response: Function ((Event, HttpResponse)=>Event)
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


class Batch(Flow):
    """
    Batches events into lists of up to max_events events. Each emitted list contained max_events events, unless timeout_secs seconds
    have passed since the first event in the batch was received, at which the batch is emitted with potentially fewer than max_events
    event.
    :param max_events: Maximum number of events per emitted batch.
    :type max_events: int
    :param timeout_secs: Maximum number of seconds to wait before a batch is emitted.
    :type timeout_secs: int
    """

    def __init__(self, max_events, timeout_secs=None, **kwargs):
        Flow.__init__(self, **kwargs)
        self._max_events = max_events
        self._event_count = 0
        self._batch = []
        self._timeout_task = None

        self._timeout_secs = timeout_secs
        if self._timeout_secs <= 0:
            raise ValueError('Batch timeout cannot be 0 or negative')

    async def sleep_and_emit(self):
        await asyncio.sleep(self._timeout_secs)
        await self.emit_batch()

    async def _do(self, event):
        if event is _termination_obj:
            if self._timeout_task and not self._timeout_task.cancelled():
                self._timeout_task.cancel()
            await self.emit_batch()
            return await self._do_downstream(_termination_obj)
        else:
            if len(self._batch) == 0 and self._timeout_secs:
                self._timeout_task = asyncio.get_running_loop().create_task(self.sleep_and_emit())

            self._event_count = self._event_count + 1
            self._batch.append(event)

            if self._event_count == self._max_events:
                if self._timeout_task and not self._timeout_task.cancelled():
                    self._timeout_task.cancel()
                await self.emit_batch()

    async def emit_batch(self):
        if len(self._batch) > 0:
            batch_to_emit = self._batch
            self._batch = []
            self._event_count = 0

            await self._do_downstream(Event([self._get_safe_event_or_body(event) for event in batch_to_emit], time=batch_to_emit[0].time))


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
        key = str(self._key_extractor(event))
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


class WriteToV3IOStream(Flow):
    """Writes all incoming events into a V3IO stream.

    :param storage: V3IO driver.
    :type storage: V3ioDriver
    :param stream_path: Path to the V3IO stream.
    :type stream_path: string
    :param sharding_func: Function for determining the shard ID to which to write each event.
    :type sharding_func: Function (Event=>int)
    :param batch_size: Batch size for each write request.
    :type batch_size: int
    """

    def __init__(self, storage, stream_path, sharding_func=None, batch_size=8, **kwargs):
        Flow.__init__(self, **kwargs)

        self._storage = storage

        if sharding_func is not None and not callable(sharding_func):
            raise TypeError(f'Expected a callable, got {type(sharding_func)}')

        self._container, self._stream_path = _split_path(stream_path)

        self._sharding_func = sharding_func

        self._batch_size = batch_size

        self._shard_count = None

    @staticmethod
    async def _handle_response(request):
        if request:
            response = await request
            if response.output.failed_record_count == 0:
                return
            raise V3ioError(f'Failed to put records to V3IO. Got {response.status_code} response: {response.body}')

    def _build_request_put_records(self, shard_id, events):
        record_list_for_json = []
        for event in events:
            record = event.body
            if isinstance(record, dict):
                record = json.dumps(record).encode("utf-8")
            record_list_for_json.append({'shard_id': shard_id, 'data': record})

        return record_list_for_json

    def _send_batch(self, buffers, in_flight_reqs, shard_id):
        buffer = buffers[shard_id]
        buffers[shard_id] = []
        request_body = self._build_request_put_records(shard_id, buffer)
        request = self._storage._put_records(self._container, self._stream_path, request_body)
        in_flight_reqs[shard_id] = asyncio.get_running_loop().create_task(request)

    async def _worker(self):
        try:
            buffers = []
            in_flight_reqs = []
            for _ in range(self._shard_count):
                buffers.append([])
                in_flight_reqs.append(None)
            while True:
                for shard_id in range(self._shard_count):
                    if self._q.empty():
                        req = in_flight_reqs[shard_id]
                        in_flight_reqs[shard_id] = None
                        await self._handle_response(req)
                        if len(buffers[shard_id]) >= self._batch_size:
                            self._send_batch(buffers, in_flight_reqs, shard_id)
                event = await self._q.get()
                if event is _termination_obj:  # handle outstanding batches and in flight requests on termination
                    for req in in_flight_reqs:
                        await self._handle_response(req)
                    for shard_id in range(self._shard_count):
                        if buffers[shard_id]:
                            self._send_batch(buffers, in_flight_reqs, shard_id)
                    for req in in_flight_reqs:
                        await self._handle_response(req)
                    break
                shard_id = self._sharding_func(event) % self._shard_count
                buffers[shard_id].append(event)
                if len(buffers[shard_id]) >= self._batch_size:
                    if in_flight_reqs[shard_id]:
                        req = in_flight_reqs[shard_id]
                        in_flight_reqs[shard_id] = None
                        await self._handle_response(req)
                    self._send_batch(buffers, in_flight_reqs, shard_id)
        except BaseException as ex:
            if not self._q.empty():
                await self._q.get()
            raise ex
        finally:
            await self._storage.close()

    async def _lazy_init(self):
        if not self._shard_count:
            response = await self._storage._describe(self._container, self._stream_path)

            self._shard_count = response.shard_count
            if self._sharding_func is None:
                def f(_):
                    return random.randint(0, self._shard_count - 1)

                self._sharding_func = f

            self._q = asyncio.queues.Queue(self._batch_size * self._shard_count)
            self._worker_awaitable = asyncio.get_running_loop().create_task(self._worker())

    async def _do(self, event):
        await self._lazy_init()

        if self._worker_awaitable.done():
            await self._worker_awaitable
            raise AssertionError("JoinWithHttp worker has already terminated")

        if event is _termination_obj:
            await self._q.put(_termination_obj)
            await self._worker_awaitable
            return await self._do_downstream(_termination_obj)
        else:
            await self._q.put(event)
            if self._worker_awaitable.done():
                await self._worker_awaitable


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


class V3ioDriver(NeedsV3ioAccess):
    """
    Database connection to V3IO.
    :param webapi: URL to the web API (https or http). If not set, the V3IO_API environment variable will be used.
    :type webapi: string
    :param access_key: V3IO access key. If not set, the V3IO_ACCESS_KEY environment variable will be used.
    :type access_key: string
    """

    def __init__(self, webapi=None, access_key=None):
        NeedsV3ioAccess.__init__(self, webapi, access_key)
        self._v3io_client = None
        self._closed = False
        self._aggregation_attribute_prefix = 'aggr_'

    def _lazy_init(self):
        if not self._v3io_client:
            self._v3io_client = v3io.aio.dataplane.Client(endpoint=self._webapi_url, access_key=self._access_key)

    async def _save_schema(self, container, table_path, schema):
        self._lazy_init()

        response = await self._v3io_client.object.put(container=container,
                                                      path=f'{table_path}/{schema_file_name}',
                                                      body=json.dumps(schema),
                                                      raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)

        if not response.status_code == 200:
            raise V3ioError(f'Failed to save schema file. Response status code was {response.status_code}: {response.body}')

    async def _load_schema(self, container, table_path):
        self._lazy_init()

        schema_path = f'{table_path}/{schema_file_name}'
        response = await self._v3io_client.object.get(container, schema_path,
                                                      raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
        if response.status_code == 404:
            schema = None
        elif response.status_code == 200:
            schema = json.loads(response.body)
        else:
            raise V3ioError(f'Failed to get schema at {schema_path}. Response status code was {response.status_code}: {response.body}')

        return schema

    async def _save_key(self, container, table_path, key, aggr_item, additional_data=None):
        self._lazy_init()

        update_expression = self._build_update_expression(aggr_item, additional_data)

        response = await self._v3io_client.kv.update(container, table_path, key, expression=update_expression,
                                                     raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
        if not response.status_code == 200:
            raise V3ioError(f'Failed to save aggregation for key: {key}. Response status code was {response.status_code}: {response.body}')

    @staticmethod
    def _convert_python_obj_to_expression_value(value):
        if isinstance(value, str):
            return f"'{value}'"
        if isinstance(value, bool) or isinstance(value, float) or isinstance(value, int):
            return str(value)
        elif isinstance(value, bytes):
            return f"blob('{base64.b64encode(value).decode('ascii')}')"
        elif isinstance(value, datetime):
            timestamp = value.timestamp()

            secs = int(timestamp)
            nanosecs = int((timestamp - secs) * 1e+9)
            return f'{secs}:{nanosecs}'
        else:
            raise V3ioError(f'Type {type(value)} in UpdateItem request is not supported')

    def _build_update_expression(self, aggregation_element, additional_data):
        expressions = []
        for name, bucket in aggregation_element.aggregation_buckets.items():
            # Only save raw aggregates, not virtual
            if bucket.should_persist:
                blob = pickle.dumps(bucket.to_dict())
                base64_blob = base64.b64encode(blob).decode('ascii')
                expressions.append(f"{self._aggregation_attribute_prefix}{name}=blob('{base64_blob}')")
        if additional_data:
            for name, value in additional_data.items():
                expressions.append(f'{name}={self._convert_python_obj_to_expression_value(value)}')
        return ';'.join(expressions)

    # Loads a specific key from the store, and returns it in the following format
    # {
    #   'feature_name_1': {'first_bucket_time': <time> 'values': []},
    #   'feature_name_2': {'first_bucket_time': <time> 'values': []}
    # }
    async def _load_aggregates_by_key(self, container, table_path, key):
        self._lazy_init()

        response = await self._v3io_client.kv.get(container, table_path, key, raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
        if response.status_code == 404:
            return None
        elif response.status_code == 200:
            res = {}
            for name, value in response.output.item.items():
                if name.startswith(self._aggregation_attribute_prefix):
                    res[name[len(self._aggregation_attribute_prefix):]] = pickle.loads(value)

            return res
        else:
            raise V3ioError(f'Failed to get item. Response status code was {response.status_code}: {response.body}')

    async def _load_by_key(self, container, table_path, key):
        self._lazy_init()

        response = await self._v3io_client.kv.get(container, table_path, key, raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
        if response.status_code == 404:
            return None
        elif response.status_code == 200:
            parsed_response = response.output.item
            for name in parsed_response.items():
                if name.startswith(self._aggregation_attribute_prefix):
                    del parsed_response[name]
            return parsed_response
        else:
            raise V3ioError(f'Failed to get item. Response status code was {response.status_code}: {response.body}')

    async def _describe(self, container, stream_path):
        self._lazy_init()

        response = await self._v3io_client.stream.describe(container, stream_path)
        if response.status_code != 200:
            raise V3ioError(f'Failed to get number of shards. Got {response.status_code} response: {response.body}')
        return response.output

    async def _put_records(self, container, stream_path, payload):
        self._lazy_init()

        return await self._v3io_client.stream.put_records(container, stream_path, payload)

    async def _get_item(self, container, table_path, key, attributes):
        self._lazy_init()

        return await self._v3io_client.kv.get(container, table_path, key, attribute_names=attributes,
                                              raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)

    async def close(self):
        if self._v3io_client and not self._closed:
            self._closed = True
            await self._v3io_client.close()


class NoopDriver:
    async def _save_schema(self, container, table_path, schema):
        pass

    async def _load_schema(self, container, table_path):
        pass

    async def _save_key(self, container, table_path, key, aggr_item, additional_data):
        pass

    async def _load_aggregates_by_key(self, container, table_path, key):
        pass

    async def _load_by_key(self, container, table_path, key):
        pass

    async def close(self):
        pass


class Cache:
    def __init__(self, table_path, storage):
        self._container, self._table_path = _split_path(table_path)
        self._storage = storage
        self._cache = {}
        self._aggregation_store = None

    def __getitem__(self, key):
        return self._cache[key]

    async def get_or_load_key(self, key):
        if key not in self._cache:
            res = await self._storage._load_by_key(self._container, self._table_path, key)
            if res:
                self._cache[key] = res
            else:
                self._cache[key] = {}
        return self._cache[key]

    def __setitem__(self, key, value):
        self._cache[key] = value

    def _set_aggregation_store(self, store):
        store._container = self._container
        store._table_path = self._table_path
        store._storage = self._storage
        self._aggregation_store = store

    async def _persist_key(self, key):
        aggr_by_key = self._aggregation_store[key]
        additional_cache_data_by_key = self._cache.get(key, None)
        await self._storage._save_key(self._container, self._table_path, key, aggr_by_key, additional_cache_data_by_key)

    async def close(self):
        await self._storage.close()
