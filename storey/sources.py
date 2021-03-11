import asyncio
import csv
import queue
import threading
import uuid
import warnings
from datetime import datetime, timezone
from typing import List, Optional, Union, Callable, Coroutine, Iterable

import pandas

from .dtypes import _termination_obj, Event
from .flow import Flow, Complete
from .utils import url_to_file_system, get_key_from_data


class AwaitableResult:
    """Future result of a computation. Calling await_result() will return with the result once the computation is completed."""

    def __init__(self, on_error: Optional[Callable[[], None]] = None):
        self._on_error = on_error
        self._q = queue.Queue(1)
        self._completed = False

    def await_result(self):
        """Returns the result, once the computation is completed"""
        result = self._q.get()
        if isinstance(result, BaseException):
            if self._on_error:
                self._on_error()
            raise result
        return result

    def _set_result(self, element):
        if not self._completed:
            self._completed = True
            self._q.put(element)

    def _set_error(self, ex):
        self._set_result(ex)


class FlowControllerBase:
    def __init__(self, key_field: Optional[str], time_field: Optional[str]):
        self._key_field = key_field
        self._time_field = time_field
        self._current_uuid_base = None
        self._current_uuid_count = 0

    def _get_uuid(self):
        if not self._current_uuid_base or self._current_uuid_count == 1024:
            self._current_uuid_base = uuid.uuid4().hex
            self._current_uuid_count = 0
        result = f'{self._current_uuid_base}-{self._current_uuid_count:04}'
        self._current_uuid_count += 1
        return result

    def _build_event(self, element, key, event_time):
        if event_time is None and self._time_field is None:
            event_time = datetime.now(timezone.utc)
        if hasattr(element, 'id'):
            event = element
            if key:
                event.key = key
            elif self._key_field:
                event.key = get_key_from_data(event.body, self._key_field)
            if event_time:
                event.time = event_time
            elif self._time_field:
                event.time = event.body[self._time_field]
        else:
            if not key and self._key_field:
                key = get_key_from_data(element, self._key_field)
            if not event_time and self._time_field:
                event_time = element[self._time_field]
            event = Event(element, id=self._get_uuid(), key=key, time=event_time)
        return event


class FlowController(FlowControllerBase):
    """Used to emit events into the associated flow, terminate the flow, and await the flow's termination.
    To be used from a synchronous context.

    """

    def __init__(self, emit_fn, await_termination_fn, return_awaitable_result, key_field: Optional[str] = None,
                 time_field: Optional[str] = None):
        super().__init__(key_field, time_field)
        self._emit_fn = emit_fn
        self._await_termination_fn = await_termination_fn
        self._return_awaitable_result = return_awaitable_result

    def emit(self, element: object, key: Optional[str] = None, event_time: Optional[datetime] = None,
             return_awaitable_result: Optional[bool] = None):
        """Emits an event into the associated flow.

        :param element: The event data, or payload. To set metadata as well, pass an Event object.
        :param key: The event key (optional)
        :param event_time: The event time (default to current time, UTC).
        :param return_awaitable_result: Deprecated! An awaitable result object will be returned if a Complete step appears in the flow.

        :returns: AsyncAwaitableResult if a Complete appears in the flow. None otherwise.
        """
        if return_awaitable_result is not None:
            warnings.warn('return_awaitable_result is deprecated. An awaitable result object will be returned if a Complete step appears '
                          'in the flow.',
                          DeprecationWarning)

        event = self._build_event(element, key, event_time)
        awaitable_result = None
        if self._return_awaitable_result:
            awaitable_result = AwaitableResult(self.terminate)
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
    """Future termination result of a flow. Calling await_termination() will wait for the flow to terminate and return its
    termination result."""

    def __init__(self, await_termination_fn):
        self._await_termination_fn = await_termination_fn

    def await_termination(self):
        """"waits for the flow to terminate and returns the result"""
        return self._await_termination_fn()


class Source(Flow):
    """Synchronous entry point into a flow. Produces a FlowController when run, for use from inside a synchronous context. See AsyncSource
    for use from inside an async context.

    :param buffer_size: size of the incoming event buffer. Defaults to 1024.
    :param key_field: Field to extract and use as the key. Optional. Can be list of keys
    :param time_field: Field to extract and use as the time. Optional.
    :param name: Name of this step, as it should appear in logs. Defaults to class name (Source).
    :type name: string

    for additional params, see documentation of  :class:`storey.flow.Flow`
    """
    _legal_first_step = True

    def __init__(self, buffer_size: Optional[int] = None, key_field: Optional[Union[str, List[str]]] = None,
                 time_field: Optional[str] = None, **kwargs):
        if buffer_size is None:
            buffer_size = 1024
        else:
            kwargs['buffer_size'] = buffer_size
        if key_field is not None:
            kwargs['key_field'] = key_field
        super().__init__(**kwargs)
        if buffer_size <= 0:
            raise ValueError('Buffer size must be positive')
        self._q = queue.Queue(buffer_size)
        self._key_field = key_field
        self._time_field = time_field
        self._termination_q = queue.Queue(1)
        self._ex = None
        self._closeables = []

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
                if event is not _termination_obj and event._awaitable_result:
                    event._awaitable_result._set_error(ex)
                self._ex = ex
                if not self._q.empty():
                    event = self._q.get()
                    if event is not _termination_obj and event._awaitable_result:
                        event._awaitable_result._set_error(ex)
                self._termination_future.set_result(None)
                break
            if event is _termination_obj:
                break

        for closeable in self._closeables:
            await closeable.close()

    def _loop_thread_main(self):
        asyncio.run(self._run_loop())
        self._termination_q.put(self._ex)

    def _raise_on_error(self, ex):
        if ex:
            if self.verbose:
                raise type(self._ex)('Flow execution terminated') from self._ex
            raise self._ex

    def _emit(self, event):
        if event is not _termination_obj:
            self._raise_on_error(self._ex)
        self._q.put(event)
        if event is not _termination_obj:
            self._raise_on_error(self._ex)

    def run(self):
        """Starts the flow"""
        self._closeables = super().run()

        thread = threading.Thread(target=self._loop_thread_main)
        thread.start()

        def raise_error_or_return_termination_result():
            self._raise_on_error(self._termination_q.get())
            return self._termination_future.result()

        has_complete = self._check_step_in_flow(Complete)

        return FlowController(self._emit, raise_error_or_return_termination_result, has_complete, self._key_field, self._time_field)


class AsyncAwaitableResult:
    """Future result of a computation. Calling await_result() will return with the result once the computation is completed.
    Same as AwaitableResult but for an async context."""

    def __init__(self, on_error: Optional[Callable[[BaseException], Coroutine]] = None):
        self._on_error = on_error
        self._q = asyncio.Queue(1)
        self._completed = False

    async def await_result(self):
        """returns the result of the computation, once the computation is complete"""
        result = await self._q.get()
        if isinstance(result, BaseException):
            if self._on_error:
                await self._on_error()
            raise result
        return result

    async def _set_result(self, element):
        if not self._completed:
            self._completed = True
            await self._q.put(element)

    async def _set_error(self, ex):
        await self._set_result(ex)


class AsyncFlowController(FlowControllerBase):
    """
    Used to emit events into the associated flow, terminate the flow, and await the flow's termination. To be used from inside an async def.
    """

    def __init__(self, emit_fn, loop_task, await_result, key_field: Optional[str] = None, time_field: Optional[str] = None):
        super().__init__(key_field, time_field)
        self._emit_fn = emit_fn
        self._loop_task = loop_task
        self._key_field = key_field
        self._time_field = time_field
        self._await_result = await_result

    async def emit(self, element: object, key: Optional[str] = None, event_time: Optional[datetime] = None,
                   await_result: Optional[bool] = None) -> object:
        """Emits an event into the associated flow.

        :param element: The event data, or payload. To set metadata as well, pass an Event object.
        :param key: The event key (optional)
        :param event_time: The event time (default to current time, UTC).
        :param await_result: Deprecated. Will await a result if a Complete step appears in the flow.

        :returns: The result received from the flow if a Complete step appears in the flow. None otherwise.
        """
        if await_result is not None:
            warnings.warn('await_result is deprecated. An awaitable result object will be returned if a Complete step appears '
                          'in the flow.',
                          DeprecationWarning)

        event = self._build_event(element, key, event_time)
        awaitable = None
        if self._await_result:
            awaitable = AsyncAwaitableResult(self.terminate)
        event._awaitable_result = awaitable
        await self._emit_fn(event)
        if self._await_result:
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

    :param buffer_size: size of the incoming event buffer. Defaults to 1024.
    :param name: Name of this step, as it should appear in logs. Defaults to class name (AsyncSource).
    :type name: string

    for additional params, see documentation of  :class:`~storey.flow.Flow`
    """
    _legal_first_step = True

    def __init__(self, buffer_size: int = 1024, key_field: Optional[str] = None, time_field: Optional[str] = None,
                 **kwargs):
        super().__init__(**kwargs)
        if buffer_size <= 0:
            raise ValueError('Buffer size must be positive')
        self._q = asyncio.Queue(buffer_size)
        self._key_field = key_field
        self._time_field = time_field
        self._ex = None
        self._closeables = []

    async def _run_loop(self):
        while True:
            event = await self._q.get()
            try:
                termination_result = await self._do_downstream(event)
                if event is _termination_obj:
                    return termination_result
            except BaseException as ex:
                self._ex = ex
                if event is not _termination_obj and event._awaitable_result:
                    awaitable = event._awaitable_result._set_error(ex)
                    if awaitable:
                        await awaitable
                if not self._q.empty():
                    await self._q.get()
                self._raise_on_error()
            finally:
                if event is _termination_obj or self._ex:
                    for closeable in self._closeables:
                        await closeable.close()

    def _raise_on_error(self):
        if self._ex:
            if self.verbose:
                raise type(self._ex)('Flow execution terminated') from self._ex
            raise self._ex

    async def _emit(self, event):
        if event is not _termination_obj:
            self._raise_on_error()
        await self._q.put(event)
        if event is not _termination_obj:
            self._raise_on_error()

    async def run(self):
        """Starts the flow"""
        self._closeables = super().run()
        loop_task = asyncio.get_running_loop().create_task(self._run_loop())
        has_complete = self._check_step_in_flow(Complete)
        return AsyncFlowController(self._emit, loop_task, has_complete, self._key_field, self._time_field)


class _IterableSource(Flow):
    _legal_first_step = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._termination_q = queue.Queue(1)
        self._ex = None
        self._closeables = []

    async def _run_loop(self):
        raise NotImplementedError()

    async def _async_loop_thread_main(self):
        try:
            self._termination_future = asyncio.get_running_loop().create_future()
            termination_result = await self._run_loop()
            self._termination_future.set_result(termination_result)
        except BaseException as ex:
            self._ex = ex
            self._termination_future.set_result(None)
        finally:
            for closeable in self._closeables:
                await closeable.close()

    def _loop_thread_main(self):
        asyncio.run(self._async_loop_thread_main())
        self._termination_q.put(self._ex)

    def _raise_on_error(self, ex):
        if ex:
            if self.verbose:
                raise type(self._ex)('Flow execution terminated') from self._ex
            raise self._ex

    def run(self):
        self._closeables = super().run()

        thread = threading.Thread(target=self._loop_thread_main)
        thread.start()

        def raise_error_or_return_termination_result():
            self._raise_on_error(self._termination_q.get())
            return self._termination_future.result()

        return FlowAwaiter(raise_error_or_return_termination_result)

    async def run_async(self):
        self._closeables = super().run()
        return await self._run_loop()


class ReadCSV(_IterableSource):
    """
    Reads CSV files as input source for a flow.

    :parameter paths: paths to CSV files
    :parameter header: whether CSV files have a header or not. Defaults to False.
    :parameter build_dict: whether to format each record produced from the input file as a dictionary (as opposed to a list).
        Default to False.
    :parameter key_field: the CSV field to be use as the key for events. May be an int (field index) or string (field name) or a tuple
        if with_header is True. Defaults to None (no key).
    :parameter time_field: the CSV field to be parsed as the timestamp for events. May be an int (field index) or string (field name)
        if with_header is True. Defaults to None (no timestamp field).
    :parameter timestamp_format: timestamp format as defined in datetime.strptime(). Default to ISO-8601 as defined in
        datetime.fromisoformat().
    :parameter type_inference: Whether to infer data types from the data (when True), or read all fields in as strings (when False).
        Defaults to True.

    for additional params, see documentation of  :class:`~storey.flow.Flow`
    """

    def __init__(self, paths: Union[List[str], str], header: bool = False, build_dict: bool = False,
                 key_field: Optional[Union[str, int, tuple]] = None, time_field: Union[int, str, None] = None,
                 timestamp_format: Optional[str] = None, type_inference: bool = True, **kwargs):
        kwargs['paths'] = paths
        kwargs['header'] = header
        kwargs['build_dict'] = build_dict
        if key_field is not None:
            kwargs['key_field'] = key_field
        if time_field is not None:
            kwargs['time_field'] = time_field
        if timestamp_format is not None:
            kwargs['timestamp_format'] = timestamp_format
        kwargs['type_inference'] = type_inference
        super().__init__(**kwargs)
        if isinstance(paths, str):
            paths = [paths]
        self._paths = paths
        self._with_header = header
        self._build_dict = build_dict
        self._key_field = key_field
        self._time_field = time_field
        self._timestamp_format = timestamp_format
        self._type_inference = type_inference
        self._storage_options = kwargs.get('storage_options')

        if not header and isinstance(key_field, str):
            raise ValueError('key_field can only be set to an integer when with_header is false')
        if not header and isinstance(time_field, str):
            raise ValueError('time_field can only be set to an integer when with_header is false')

    def _init(self):
        self._event_buffer = queue.Queue(1024)
        self._types = []

    def _infer_type(self, value):
        lowercase = value.lower()
        if lowercase == 'true' or lowercase == 'false':
            return 'b'

        try:
            int(value)
            return 'i'
        except ValueError:
            pass

        try:
            float(value)
            return 'f'
        except ValueError:
            pass

        try:
            self._datetime_from_timestamp(value)
            return 't'
        except ValueError:
            pass

        return 's'

    def _parse_field(self, field, index):
        typ = self._types[index]
        if typ == 's':
            return field
        if typ == 'f':
            return float(field)
        if typ == 'i':
            return int(field)
        if typ == 'b':
            lowercase = field.lower()
            if lowercase == 'true':
                return True
            if lowercase == 'false':
                return False
            raise TypeError(f'Expected boolean, got {field}')
        if typ == 't':
            return self._datetime_from_timestamp(field)
        raise TypeError(f'Unknown type: {typ}')

    def _datetime_from_timestamp(self, timestamp):
        if self._timestamp_format:
            return datetime.strptime(timestamp, self._timestamp_format)
        else:
            return datetime.fromisoformat(timestamp)

    def _blocking_io_loop(self):
        try:

            for path in self._paths:
                fs, file_path = url_to_file_system(path, self._storage_options)
                with fs.open(file_path, mode='r') as f:
                    header = None
                    field_name_to_index = None
                    if self._with_header:
                        line = f.readline()
                        header = next(csv.reader([line]))
                        field_name_to_index = {}
                        for i in range(len(header)):
                            field_name_to_index[header[i]] = i
                    for line in f:
                        parsed_line = next(csv.reader([line]))
                        if self._type_inference:
                            if not self._types:
                                for field in parsed_line:
                                    self._types.append(self._infer_type(field))
                            for i in range(len(parsed_line)):
                                parsed_line[i] = self._parse_field(parsed_line[i], i)
                        element = parsed_line
                        key = None
                        if header:
                            if len(parsed_line) != len(header):
                                raise ValueError(
                                    f'CSV line with {len(parsed_line)} fields did not match header with {len(header)} fields')
                            if self._build_dict:
                                element = {}
                                for i in range(len(parsed_line)):
                                    element[header[i]] = parsed_line[i]
                        if self._key_field:
                            key_field = self._key_field
                            if self._with_header:
                                if isinstance(self._key_field, str):
                                    key_field = field_name_to_index[key_field]
                                if isinstance(self._key_field, list):
                                    key_field = []
                                    for key_f in self._key_field:
                                        if isinstance(key_f, str):
                                            key_field.append(field_name_to_index[key_f])
                                        else:
                                            key_field.append(key_f)
                            key = get_key_from_data(parsed_line, key_field)
                        if self._time_field:
                            time_field = self._time_field
                            if self._with_header and isinstance(time_field, str):
                                time_field = field_name_to_index[time_field]
                            time_as_datetime = parsed_line[time_field]
                        else:
                            time_as_datetime = datetime.now()
                        self._event_buffer.put(Event(element, key=key, time=time_as_datetime))
        except BaseException as ex:
            self._event_buffer.put(ex)
        self._event_buffer.put(_termination_obj)

    def _get_event(self):
        event = self._event_buffer.get()
        if isinstance(event, BaseException):
            raise event
        return event

    async def _run_loop(self):
        asyncio.get_running_loop().run_in_executor(None, self._blocking_io_loop)

        def get_multiple():
            events = [self._get_event()]
            while not self._event_buffer.empty() and len(events) < 128:
                events.append(self._get_event())
            return events

        while True:
            events = await asyncio.get_running_loop().run_in_executor(None, get_multiple)
            for event in events:
                res = await self._do_downstream(event)
                if event is _termination_obj:
                    return res


class DataframeSource(_IterableSource):
    """Use pandas dataframe as input source for a flow.

    :param dfs: A pandas dataframe, or dataframes, to be used as input source for the flow.
    :param key_field: column to be used as key for events. can be list of columns
    :param time_field: column to be used as time for events.
    :param id_field: column to be used as ID for events.

    for additional params, see documentation of  :class:`~storey.flow.Flow`
    """

    def __init__(self, dfs: Union[pandas.DataFrame, Iterable[pandas.DataFrame]], key_field: Optional[Union[str, List[str]]] = None,
                 time_field: Optional[str] = None, id_field: Optional[str] = None, **kwargs):
        if key_field is not None:
            kwargs['key_field'] = key_field
        if time_field is not None:
            kwargs['time_field'] = time_field
        if id_field is not None:
            kwargs['id_field'] = id_field
        super().__init__(**kwargs)
        if isinstance(dfs, pandas.DataFrame):
            dfs = [dfs]
        self._dfs = dfs
        self._key_field = key_field
        self._time_field = time_field
        self._id_field = id_field

    async def _run_loop(self):
        for df in self._dfs:
            for namedtuple in df.itertuples():
                body = namedtuple._asdict()
                index = body.pop('Index')
                if len(df.index.names) > 1:
                    for i, index_column in enumerate(df.index.names):
                        body[index_column] = index[i]
                elif df.index.names[0] is not None:
                    body[df.index.names[0]] = index

                key = get_key_from_data(body, self._key_field)
                time = None
                if self._time_field:
                    time = body[self._time_field]
                id = None
                if self._id_field:
                    id = body[self._id_field]
                event = Event(body, key=key, time=time, id=id)
                await self._do_downstream(event)
        return await self._do_downstream(_termination_obj)


class ReadParquet(DataframeSource):
    """Reads Parquet files as input source for a flow.
    :parameter paths: paths to Parquet files
    :parameter columns : list, default=None. If not None, only these columns will be read from the file.
    """

    def __init__(self, paths: Union[str, Iterable[str]], columns=None, **kwargs):
        if isinstance(paths, str):
            paths = [paths]
        dfs = map(lambda path: pandas.read_parquet(path, columns=columns,
                                                   storage_options=kwargs.get('storage_options')), paths)
        super().__init__(dfs, **kwargs)
