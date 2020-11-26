import asyncio
import csv
import queue
import threading
from datetime import datetime, timezone
from typing import List, Optional, Union

import pandas

from .dtypes import _termination_obj, Event, FlowError
from .flow import Flow


class AwaitableResult:
    """Future result of a computation. Calling await_result() will return with the result once the computation is completed."""

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

    def emit(self, element: object, key: Optional[str] = None, event_time: Optional[datetime] = None,
             return_awaitable_result: bool = False):
        """Emits an event into the associated flow.

        :param element: The event data, or payload. To set metadata as well, pass an Event object.
        :param key: The event key (optional)
        :param event_time: The event time (default to current time, UTC).
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
    """Future termination result of a flow. Calling await_termination() will wait for the flow to terminate and return its
    termination result."""

    def __init__(self, await_termination_fn):
        self._await_termination_fn = await_termination_fn

    def await_termination(self):
        return self._await_termination_fn()


class Source(Flow):
    """Synchronous entry point into a flow. Produces a FlowController when run, for use from inside a synchronous context. See AsyncSource
    for use from inside an async context.

    :param buffer_size: size of the incoming event buffer. Defaults to 1024.
    :param name: Name of this step, as it should appear in logs. Defaults to class name (Source).
    :type name: string
    """

    def __init__(self, buffer_size: int = 1024, **kwargs):
        super().__init__(**kwargs)
        if buffer_size <= 0:
            raise ValueError('Buffer size must be positive')
        self._q = queue.Queue(buffer_size)
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
                self._ex = ex
                if not self._q.empty():
                    self._q.get()
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
            raise FlowError('Flow execution terminated due to an error') from self._ex

    def _emit(self, event):
        self._raise_on_error(self._ex)
        self._q.put(event)
        self._raise_on_error(self._ex)

    def run(self):
        self._closeables = super().run()

        thread = threading.Thread(target=self._loop_thread_main)
        thread.start()

        def raise_error_or_return_termination_result():
            self._raise_on_error(self._termination_q.get())
            return self._termination_future.result()

        return FlowController(self._emit, raise_error_or_return_termination_result)


class AsyncAwaitableResult:
    """Future result of a computation. Calling await_result() will return with the result once the computation is completed.
    Same as AwaitableResult but for an async context."""

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

    async def emit(self, element: object, key: Optional[str] = None, event_time: Optional[datetime] = None,
                   await_result: bool = False) -> object:
        """Emits an event into the associated flow.

        :param element: The event data, or payload. To set metadata as well, pass an Event object.
        :param key: The event key (optional)
        :param event_time: The event time (default to current time, UTC).
        :param await_result: Whether to await a result from the flow (as signaled by the Complete step). Defaults to False.

        :returns: The result received from the flow if await_result is True. None otherwise.
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

    :param buffer_size: size of the incoming event buffer. Defaults to 1024.
    :param name: Name of this step, as it should appear in logs. Defaults to class name (AsyncSource).
    :type name: string
    """

    def __init__(self, buffer_size: int = 1024, **kwargs):
        super().__init__(**kwargs)
        if buffer_size <= 0:
            raise ValueError('Buffer size must be positive')
        self._q = asyncio.Queue(buffer_size, loop=asyncio.get_running_loop())
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
                if event._awaitable_result:
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
            raise FlowError('Flow execution terminated due to an error') from self._ex

    async def _emit(self, event):
        self._raise_on_error()
        await self._q.put(event)
        self._raise_on_error()

    async def run(self):
        self._closeables = super().run()
        loop_task = asyncio.get_running_loop().create_task(self._run_loop())
        return AsyncFlowController(self._emit, loop_task)


class _IterableSource(Flow):

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
            raise FlowError('Flow execution terminated due to an error') from self._ex

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

    :param paths: paths to CSV files
    :param header: whether CSV files have a header or not. Defaults to False.
    :param build_dict: whether to format each record produced from the input file as a dictionary (as opposed to a list). Default to False.
    :param key_field: the CSV field to be use as the key for events. May be an int (field index) or string (field name) if with_header
    is True. Defaults to None (no key).
    :param timestamp_field: the CSV field to be parsed as the timestamp for events. May be an int (field index) or string (field name) if
    with_header is True. Defaults to None (no timestamp field).
    :param timestamp_format: timestamp format as defined in datetime.strptime(). Default to ISO-8601 as defined in datetime.fromisoformat().
    """

    def __init__(self, paths: Union[List[str], str], header: bool = False, build_dict: bool = False,
                 key_field: Union[int, str, None] = None, timestamp_field: Union[int, str, None] = None,
                 timestamp_format: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        if isinstance(paths, str):
            paths = [paths]
        self._paths = paths
        self._with_header = header
        self._build_dict = build_dict
        self._key_field = key_field
        self._timestamp_field = timestamp_field
        self._timestamp_format = timestamp_format
        self._event_buffer = queue.Queue(1024)

        if not header and isinstance(key_field, str):
            raise ValueError('key_field can only be set to an integer when with_header is false')
        if not header and isinstance(timestamp_field, str):
            raise ValueError('timestamp_field can only be set to an integer when with_header is false')

    def _run_in_executor(self):
        for path in self._paths:
            with open(path, mode='r') as f:
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
                    self._event_buffer.put(Event(element, key=key, time=timestamp))
        self._event_buffer.put(_termination_obj)

    async def _run_loop(self):
        asyncio.get_running_loop().run_in_executor(None, self._run_in_executor)
        while True:
            event = await asyncio.get_running_loop().run_in_executor(None, self._event_buffer.get)
            res = await self._do_downstream(event)
            if event is _termination_obj:
                return res


async def _aiter(iterable):
    for x in iterable:
        yield x


class DataframeSource(_IterableSource):
    """Use pandas dataframe as input source for a flow.

    :param dfs: A pandas dataframe, or dataframes, to be used as input source for the flow.
    :param key_column: column to be used as key for events.
    :param time_column: column to be used as time for events.
    :param id_column: column to be used as ID for events.
    """

    def __init__(self, dfs: Union[pandas.DataFrame, List[pandas.DataFrame]], key_column: Optional[str] = None,
                 time_column: Optional[str] = None, id_column: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        if not isinstance(dfs, list):
            dfs = [dfs]
        self._dfs = dfs
        self._key_field = key_column
        self._time_field = time_column
        self._id_field = id_column

    async def _run_loop(self):
        async for df in _aiter(self._dfs):
            async for indexes, series in _aiter(df.iterrows()):
                body = series.to_dict()
                i = 0
                for index_name in df.index.names:
                    if index_name:
                        body[index_name] = indexes[i]
                    i += 1
                key = None
                if self._key_field:
                    key = body[self._key_field]
                time = None
                if self._time_field:
                    time = body[self._time_field]
                id = None
                if self._id_field:
                    id = body[self._id_field]
                event = Event(body, key=key, time=time, id=id)
                await self._do_downstream(event)
        return await self._do_downstream(_termination_obj)
