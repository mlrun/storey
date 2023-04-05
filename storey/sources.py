# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import asyncio
import copy
import queue
import threading
import uuid
import warnings
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Callable, Coroutine, Iterable, List, Optional, Union

import pandas
import pyarrow
import pytz

from .dtypes import Event, _termination_obj
from .flow import Complete, Flow
from .utils import find_filters, find_partitions, url_to_file_system


class AwaitableResult:
    """
    Future result of a computation. Calling await_result() will return with the result once the computation is
    completed.
    """

    def __init__(
        self,
        on_error: Optional[Callable[[], None]] = None,
        expected_number_of_results: int = 1,
    ):
        self._on_error = on_error
        self._expected_number_of_results = expected_number_of_results
        self._number_of_results = 0
        self._q = queue.Queue(expected_number_of_results)

    def await_result(self):
        """Returns the result, once the computation is completed"""
        results = []
        for _ in range(self._expected_number_of_results):
            result = self._q.get()
            if isinstance(result, BaseException):
                if self._on_error:
                    self._on_error()
                # Python appends trace frames to a raised exception, so we must copy
                # it before raising to prevent it from growing each time
                raise copy.copy(result)
            results.append(result)
        if len(results) == 1:
            results = results[0]
        return results

    def _set_result(self, element):
        if self._number_of_results < self._expected_number_of_results:
            self._number_of_results += 1
            self._q.put(element)

    def _set_error(self, ex):
        self._set_result(ex)


def _convert_to_datetime(obj, time_format: Optional[str] = None):
    if isinstance(obj, datetime):
        return obj
    elif isinstance(obj, float) or isinstance(obj, int):
        return datetime.fromtimestamp(obj, tz=pytz.utc)
    elif isinstance(obj, str):
        if time_format is None:
            return datetime.fromisoformat(obj)
        else:
            return datetime.strptime(obj, time_format)
    else:
        raise ValueError(f"Could not parse '{obj}' (of type {type(obj)}) as a time.")


class WithUUID:
    def __init__(self):
        self._current_uuid_base = None
        self._current_uuid_count = 0

    def _get_uuid(self):
        if not self._current_uuid_base or self._current_uuid_count == 1024:
            self._current_uuid_base = uuid.uuid4().hex
            self._current_uuid_count = 0
        result = f"{self._current_uuid_base}-{self._current_uuid_count:04}"
        self._current_uuid_count += 1
        return result


class FlowControllerBase(WithUUID):
    def __init__(
        self,
        key_field: Optional[Union[str, List[str]]],
        id_field: Optional[str],
    ):
        super().__init__()
        self._key_field = key_field
        self._id_field = id_field

    def _build_event(self, element, key):
        element_is_event = hasattr(element, "id")
        if element_is_event:
            if isinstance(element.body, dict) and element.body.get(Event._serialize_event_marker):
                serialized_event = element.body
                body = serialized_event.get("body")
                element.body = body
                for field in Event._serialize_fields:
                    val = serialized_event.get(field)
                    if val is not None:
                        val = serialized_event.get(field)
                        if val is not None:
                            if field == "time":
                                val = _convert_to_datetime(val)
                            setattr(element, field, val)
            else:
                body = element.body
            if not hasattr(element, "processing_time"):
                if hasattr(element, "timestamp"):
                    element.processing_time = element.timestamp
                else:
                    element.processing_time = datetime.now(timezone.utc)
        else:
            body = element

        if not key and self._key_field:
            if isinstance(self._key_field, str) or isinstance(self._key_field, int):
                key = body[self._key_field]
            else:
                key = []
                for field in self._key_field:
                    key.append(body[field])

        if element_is_event:
            if key or not hasattr(element, "key"):
                element.key = key
            return element
        else:
            return Event(body, id=self._get_uuid(), key=key)


class FlowController(FlowControllerBase):
    """Used to emit events into the associated flow, terminate the flow, and await the flow's termination.
    To be used from a synchronous context.
    """

    def __init__(
        self,
        emit_fn,
        await_termination_fn,
        return_awaitable_result,
        key_field: Optional[str] = None,
        id_field: Optional[str] = None,
    ):
        super().__init__(key_field, id_field)
        self._emit_fn = emit_fn
        self._await_termination_fn = await_termination_fn
        self._return_awaitable_result = return_awaitable_result

    def emit(
        self,
        element: object,
        key: Optional[Union[str, List[str]]] = None,
        return_awaitable_result: Optional[bool] = None,
        expected_number_of_results: Optional[int] = None,
    ):
        """Emits an event into the associated flow.

        :param element: The event data, or payload. To set metadata as well, pass an Event object.
        :param key: The event key(s) (optional) #add to async
        :param return_awaitable_result: Deprecated! An awaitable result object will be returned if a Complete step
            appears in the flow.
        :param expected_number_of_results: Number of times the event will have to pass through a Complete step to be
            completed (for graph
        flows).

        :returns: AsyncAwaitableResult if a Complete appears in the flow. None otherwise.
        """
        if return_awaitable_result is not None:
            warnings.warn(
                "return_awaitable_result is deprecated. An awaitable result object will be returned if a Complete step "
                "appears in the flow.",
                DeprecationWarning,
            )

        event = self._build_event(element, key)
        awaitable_result = None
        if self._return_awaitable_result:
            awaitable_result = AwaitableResult(expected_number_of_results=expected_number_of_results or 1)
        event._awaitable_result = awaitable_result
        self._emit_fn(event)
        return awaitable_result

    def terminate(self):
        """Terminates the associated flow."""
        self._emit_fn(_termination_obj)

    def await_termination(self):
        """Awaits the termination of the flow. To be called after terminate. Returns the termination result of the
        flow (if any).
        """
        return self._await_termination_fn()


class FlowAwaiter:
    """Future termination result of a flow. Calling await_termination() will wait for the flow to terminate and return
    its termination result."""

    def __init__(self, await_termination_fn):
        self._await_termination_fn = await_termination_fn

    def await_termination(self):
        """ "waits for the flow to terminate and returns the result"""
        return self._await_termination_fn()


class SyncEmitSource(Flow):
    """Synchronous entry point into a flow. Produces a FlowController when run, for use from inside a synchronous
    context. See AsyncEmitSource for use from inside an async context.

    :param buffer_size: size of the incoming event buffer. Defaults to 8.
    :param key_field: Field to extract and use as the key. Optional.
    :param name: Name of this step, as it should appear in logs. Defaults to class name (SyncEmitSource).
    :type name: string

    for additional params, see documentation of  :class:`storey.flow.Flow`
    """

    _legal_first_step = True

    def __init__(
        self,
        buffer_size: Optional[int] = None,
        key_field: Union[list, str, int, None] = None,
        **kwargs,
    ):
        if buffer_size is None:
            buffer_size = 8
        else:
            kwargs["buffer_size"] = buffer_size
        if key_field is not None:
            kwargs["key_field"] = key_field
        super().__init__(**kwargs)
        if buffer_size <= 0:
            raise ValueError("Buffer size must be positive")
        self._q = queue.Queue(buffer_size)
        self._key_field = key_field
        self._termination_q = queue.Queue(1)
        self._ex = None
        self._closeables = []

    def _init(self):
        super()._init()
        self._is_terminated = False

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
            try:
                maybe_coroutine = closeable.close()
                if asyncio.iscoroutine(maybe_coroutine):
                    await maybe_coroutine
            except Exception as ex:
                if self.context:
                    self.context.logger.error(f"Error trying to close {closeable}: {ex}")

    def _loop_thread_main(self):
        asyncio.run(self._run_loop())
        self._termination_q.put(self._ex)

    def _raise_on_error(self, ex):
        if ex:
            # Python appends trace frames to a raised exception, so we must copy
            # it before raising to prevent it from growing each time
            ex_copy = copy.copy(self._ex)
            if self.verbose:
                raise type(ex_copy)("Flow execution terminated") from ex_copy
            raise ex_copy

    def _emit(self, event):
        if event is not _termination_obj:
            self._raise_on_error(self._ex)
            if self._is_terminated:
                raise ValueError("Cannot emit to a terminated flow")
        else:
            self._is_terminated = True
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

        return FlowController(
            self._emit,
            raise_error_or_return_termination_result,
            has_complete,
            self._key_field,
        )


class AsyncAwaitableResult:
    """Future result of a computation. Calling await_result() will return with the result once the computation is
    completed. Same as AwaitableResult but for an async context.
    """

    def __init__(
        self,
        on_error: Optional[Callable[[BaseException], Coroutine]] = None,
        expected_number_of_results: int = 1,
    ):
        self._on_error = on_error
        self._expected_number_of_results = expected_number_of_results
        self._number_of_results = 0
        self._q = asyncio.Queue(expected_number_of_results)

    async def await_result(self):
        """returns the result of the computation, once the computation is complete"""
        results = []
        for _ in range(self._expected_number_of_results):
            result = await self._q.get()
            if isinstance(result, BaseException):
                if self._on_error:
                    await self._on_error()
                # Python appends trace frames to a raised exception, so we must copy
                # it before raising to prevent it from growing each time
                raise copy.copy(result)
            results.append(result)
        if len(results) == 1:
            results = results[0]
        return results

    async def _set_result(self, element):
        if self._number_of_results < self._expected_number_of_results:
            self._number_of_results += 1
            await self._q.put(element)

    async def _set_error(self, ex):
        await self._set_result(ex)


class AsyncFlowController(FlowControllerBase):
    """
    Used to emit events into the associated flow, terminate the flow, and await the flow's termination. To be used from
    inside an async def.
    """

    def __init__(
        self,
        emit_fn,
        loop_task,
        await_result,
        key_field: Optional[str] = None,
        id_field: Optional[str] = None,
    ):
        super().__init__(key_field, id_field)
        self._emit_fn = emit_fn
        self._loop_task = loop_task
        self._key_field = key_field
        self._await_result = await_result

    async def emit(
        self,
        element: object,
        key: Optional[Union[str, List[str]]] = None,
        await_result: Optional[bool] = None,
        expected_number_of_results: Optional[int] = None,
    ) -> object:
        """Emits an event into the associated flow.

        :param element: The event data, or payload. To set metadata as well, pass an Event object.
        :param key: The event key(s) (optional)
        :param await_result: Deprecated. Will await a result if a Complete step appears in the flow.
        :param expected_number_of_results: Number of times the event will have to pass through a Complete step to be
        completed (for graph flows).

        :returns: The result received from the flow if a Complete step appears in the flow. None otherwise.
        """
        if await_result is not None:
            warnings.warn(
                "await_result is deprecated. An awaitable result object will be returned if a Complete step appears "
                "in the flow.",
                DeprecationWarning,
            )

        event = self._build_event(element, key)
        awaitable = None
        if self._await_result:
            awaitable = AsyncAwaitableResult()
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
        """
        Awaits the termination of the flow. To be called after terminate. Returns the termination result of the
        flow (if any).
        """
        return await self._loop_task


class AsyncEmitSource(Flow):
    """
    Asynchronous entry point into a flow. Produces an AsyncFlowController when run, for use from inside an async def.
    See SyncEmitSource for use from inside a synchronous context.

    :param buffer_size: size of the incoming event buffer. Defaults to 8.
    :param name: Name of this step, as it should appear in logs. Defaults to class name (AsyncEmitSource).
    :type name: string

    for additional params, see documentation of  :class:`~storey.flow.Flow`
    """

    _legal_first_step = True

    def __init__(
        self,
        buffer_size: int = None,
        key_field: Union[list, str, None] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if buffer_size is None:
            buffer_size = 8
        elif buffer_size <= 0:
            raise ValueError("Buffer size must be positive")
        else:
            kwargs["buffer_size"] = buffer_size
        self._q = asyncio.Queue(buffer_size)
        self._key_field = key_field
        self._ex = None
        self._closeables = []

    def _init(self):
        super()._init()
        self._is_terminated = False

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
                        try:
                            maybe_coroutine = closeable.close()
                            if asyncio.iscoroutine(maybe_coroutine):
                                await maybe_coroutine
                        except Exception as ex:
                            if self.context:
                                self.context.logger.error(f"Error trying to close {closeable}: {ex}")

    def _raise_on_error(self):
        if self._ex:
            # Python appends trace frames to a raised exception, so we must copy
            # it before raising to prevent it from growing each time
            ex_copy = copy.copy(self._ex)
            if self.verbose:
                raise type(ex_copy)("Flow execution terminated") from ex_copy
            raise ex_copy

    async def _emit(self, event):
        if event is not _termination_obj:
            self._raise_on_error()
            if self._is_terminated:
                raise ValueError("Cannot emit to a terminated flow")
        else:
            self._is_terminated = True
        await self._q.put(event)
        if event is not _termination_obj:
            self._raise_on_error()

    def run(self):
        """Starts the flow"""
        self._closeables = super().run()
        loop_task = asyncio.get_running_loop().create_task(self._run_loop())
        has_complete = self._check_step_in_flow(Complete)
        return AsyncFlowController(self._emit, loop_task, has_complete, self._key_field)


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
                raise type(self._ex)("Flow execution terminated") from self._ex
            raise self._ex

    def run(self):
        self._closeables = super().run()

        self._init()

        thread = threading.Thread(target=self._loop_thread_main)
        thread.start()

        def raise_error_or_return_termination_result():
            self._raise_on_error(self._termination_q.get())
            return self._termination_future.result()

        return FlowAwaiter(raise_error_or_return_termination_result)

    async def run_async(self):
        self._closeables = super().run()
        return await self._run_loop()


class DataframeSource(_IterableSource, WithUUID):
    """Use pandas dataframe as input source for a flow.

    :param dfs: A pandas dataframe, or dataframes, to be used as input source for the flow.
    :param key_field: column to be used as key for events. can be list of columns
    :param id_field: column to be used as ID for events.

    for additional params, see documentation of  :class:`~storey.flow.Flow`
    """

    def __init__(
        self,
        dfs: Union[pandas.DataFrame, Iterable[pandas.DataFrame]],
        key_field: Optional[Union[str, List[str]]] = None,
        id_field: Optional[str] = None,
        **kwargs,
    ):
        if key_field is not None:
            kwargs["key_field"] = key_field
        if id_field is not None:
            kwargs["id_field"] = id_field
        _IterableSource.__init__(self, **kwargs)
        WithUUID.__init__(self)
        if isinstance(dfs, pandas.DataFrame):
            dfs = [dfs]
        self._dfs = dfs
        self._key_field = key_field
        self._id_field = id_field

    def get_key(self, body):
        key = None
        if self._key_field:
            if isinstance(self._key_field, list):
                key = []
                for key_field in self._key_field:
                    key.append(
                        self.get_by_field_or_index(field=key_field, body=body, field_type="key", raise_exception=True)
                    )
            else:
                key = self.get_by_field_or_index(
                    field=self._key_field, body=body, field_type="key", raise_exception=True
                )
        return key

    def get_element(self, body: OrderedDict):
        return dict(body)

    async def _run_loop(self):
        for df in self._dfs:
            try:
                for namedtuple in df.itertuples():
                    body = OrderedDict(namedtuple._asdict())
                    index = body.pop("Index")
                    if len(df.index.names) > 1:
                        for i, index_column in enumerate(df.index.names):
                            body[index_column] = index[i]
                    elif df.index.names[0] is not None:
                        body[df.index.names[0]] = index
                    try:
                        key = self.get_key(body=body)
                        if self._id_field:
                            line_id = self.get_by_field_or_index(
                                field=self._id_field, body=body, field_type="id", raise_exception=False
                            )
                        else:
                            line_id = self._get_uuid()
                        element = self.get_element(body=body)
                        event = Event(element, key=key, id=line_id)
                        await self._do_downstream(event)
                    except self.NoneKeyException as key_error:
                        if self.context:
                            self.context.logger.error(str(key_error))
            except (self.SourceKeyError, self.SourceIndexError) as source_error:
                if self.context:
                    self.context.logger.error(
                        f"{str(source_error)}. \n Dataframe details:\n"
                        f" columns: {str(list(df.columns))}"
                        f" len: {len(df)}"
                        f" hash by hash(df.to_numpy().tobytes()):{hash(df.to_numpy().tobytes())}"
                    )

        return await self._do_downstream(_termination_obj)

    def get_by_field_or_index(self, field, body: OrderedDict, field_type: str, raise_exception=False):
        try:
            result = body[field]
        except KeyError as key_error:
            raise self.SourceKeyError(f"KeyError occurred: {str(key_error)}")
        if raise_exception:
            self.is_nan_validator(result=result, body=body, field_type=field_type, field=field)
        return result

    class NoneKeyException(Exception):
        pass

    class SourceKeyError(KeyError):
        pass

    class SourceIndexError(IndexError):
        pass

    def is_nan_validator(self, result, body, field_type, field):
        if pandas.isna(result) or result is None:
            raise self.NoneKeyException(f"For {body} value of {field_type} {field} is None")


class CSVSource(DataframeSource):
    """Reads Csv files as input source for a flow.

    :parameter paths: paths to Parquet files
    :parameter columns : list, default=None. If not None, only these columns will be read from the file.
    :parameter start_filter: datetime. If not None, the results will be filtered by partitions and
        'filter_column' > start_filter. Default is None.
    :parameter end_filter: datetime. If not None, the results will be filtered by partitions
        'filter_column' <= end_filter. Default is None.
    :parameter filter_column: Optional. if not None, the results will be filtered by this column and before and/or after
    :param key_field: column to be used as key for events. can be list of columns
    :param id_field: column to be used as ID for events.
    """

    def __init__(
        self,
        paths: Union[List[str], str],
        header: bool = False,
        build_dict: bool = False,
        key_field: Union[int, str, List[int], List[str], None] = None,
        time_field: Union[int, str, None] = None,
        timestamp_format: Optional[str] = None,
        id_field: Union[str, int, None] = None,
        type_inference: bool = True,
        parse_dates: Optional[Union[int, str, List[int], List[str]]] = None,
        **kwargs,
    ):

        kwargs["paths"] = paths
        if isinstance(paths, str):
            paths = [paths]
        self._paths = paths
        kwargs["header"] = header
        kwargs["build_dict"] = build_dict
        if key_field is not None:
            kwargs["key_field"] = key_field
        if time_field is not None:
            kwargs["time_field"] = time_field
        if id_field is not None:
            kwargs["id_field"] = id_field
        if timestamp_format is not None:
            kwargs["timestamp_format"] = timestamp_format
        kwargs["type_inference"] = type_inference
        self._storage_options = kwargs.get("storage_options")
        self._paths = paths
        self._with_header = header
        self._build_dict = build_dict
        self._key_field = key_field
        self._time_field = time_field
        self._timestamp_format = timestamp_format
        self._id_field = id_field
        self._type_inference = type_inference
        self._storage_options = kwargs.get("storage_options")
        self._parse_dates = parse_dates
        self._dates_indices = []
        if parse_dates:
            if not isinstance(parse_dates, List):
                parse_dates = [parse_dates]
            if not self._with_header and any([isinstance(f, str) for f in self._parse_dates]):
                raise ValueError("parse_dates can be list of str only when there is header")
            self._dates_indices = parse_dates
        if self._time_field is not None:
            if not header and isinstance(self._time_field, str):
                raise ValueError("time_field can only be set to an string when with_header is false")
            self._dates_indices.append(self._time_field)
        if not header and isinstance(key_field, str):
            raise ValueError("key_field can only be set to an integer when with_header is false")
        super().__init__([], **kwargs)

    def _init(self):
        super()._init()
        self._dfs = []
        for path in self._paths:
            if self._with_header:
                existing_dates_indices = list(set(pandas.read_csv(path, nrows=0).columns).intersection(
                    self._dates_indices))
                df = pandas.read_csv(
                    path,
                    parse_dates=existing_dates_indices,
                    date_parser=lambda x: self._datetime_from_timestamp(x),
                    storage_options=self._storage_options,
                )
            else:
                #  with 'header = None' it will automatically put indexes as columns.
                df = pandas.read_csv(
                    path,
                    parse_dates=self._dates_indices,
                    header=None,
                    date_parser=lambda x: self._datetime_from_timestamp(x),
                    storage_options=self._storage_options,
                )
                df.columns = range(df.shape[1])
            self._dfs.append(df)

    def _datetime_from_timestamp(self, timestamp):
        if timestamp == "" or timestamp is None:
            return None
        if self._timestamp_format:
            return pandas.to_datetime(timestamp, format=self._timestamp_format).floor("u").to_pydatetime()
        else:
            return datetime.fromisoformat(timestamp)

    def get_element(self, body: OrderedDict):
        if self._build_dict and self._with_header:
            return dict(body)
        return list(body.values())

    def get_by_field_or_index(self, field, body: OrderedDict, field_type: str, raise_exception=False):
        result = None
        if self._with_header and isinstance(field, str):
            result = super().get_by_field_or_index(
                field=field, body=body, field_type=field_type, raise_exception=raise_exception
            )
        else:
            try:
                result = list(body.items())[field][1]
            except IndexError as index_error:
                raise self.SourceIndexError(f"IndexError occurred: {str(index_error)}, index: {field}.")
            if raise_exception:
                self.is_nan_validator(result=result, body=body, field_type=field_type, field=field)
        return result


class ParquetSource(DataframeSource):
    """Reads Parquet files as input source for a flow.

    :parameter paths: paths to Parquet files
    :parameter columns : list, default=None. If not None, only these columns will be read from the file.
    :parameter start_filter: datetime. If not None, the results will be filtered by partitions and
        'filter_column' > start_filter. Default is None.
    :parameter end_filter: datetime. If not None, the results will be filtered by partitions
        'filter_column' <= end_filter. Default is None.
    :parameter filter_column: Optional. if not None, the results will be filtered by this column and before and/or after
    :param key_field: column to be used as key for events. can be list of columns
    :param id_field: column to be used as ID for events.
    """

    def __init__(
        self,
        paths: Union[str, Iterable[str]],
        columns=None,
        start_filter: Optional[datetime] = None,
        end_filter: Optional[datetime] = None,
        filter_column: Optional[str] = None,
        **kwargs,
    ):
        if start_filter or end_filter:
            if start_filter is None:
                start_filter = datetime.min
                if end_filter.tzinfo:
                    start_filter = start_filter.replace(tzinfo=pytz.utc)
            if end_filter is None:
                end_filter = datetime.max
                if start_filter.tzinfo:
                    end_filter = end_filter.replace(tzinfo=pytz.utc)
            if (start_filter.tzinfo is None) ^ (end_filter.tzinfo is None):
                raise ValueError("Start and end filters must either both have a time zone or both be naive timestamps")

            if filter_column is None:
                raise TypeError("Filter column is required when passing start/end filters")

        self._paths = paths
        if isinstance(paths, str):
            self._paths = [paths]
        self._columns = columns
        self._start_filter = start_filter
        self._end_filter = end_filter
        self._filter_column = filter_column
        self._storage_options = kwargs.get("storage_options")
        super().__init__([], **kwargs)

    def _read_filtered_parquet(self, path):
        fs, file_path = url_to_file_system(path, self._storage_options)

        partitions_time_attributes = find_partitions(path, fs)
        filters = []
        find_filters(
            partitions_time_attributes,
            self._start_filter,
            self._end_filter,
            filters,
            self._filter_column,
        )
        try:
            return pandas.read_parquet(
                path,
                columns=self._columns,
                filters=filters,
                storage_options=self._storage_options,
            )
        except pyarrow.lib.ArrowInvalid as ex:
            if not str(ex).startswith("Cannot compare timestamp with timezone to timestamp without timezone"):
                raise ex

            if self._start_filter.tzinfo:
                start_filter = self._start_filter.replace(tzinfo=None)
                end_filter = self._end_filter.replace(tzinfo=None)
            else:
                start_filter = self._start_filter.replace(tzinfo=pytz.utc)
                end_filter = self._end_filter.replace(tzinfo=pytz.utc)

            filters = []
            find_filters(
                partitions_time_attributes,
                start_filter,
                end_filter,
                filters,
                self._filter_column,
            )

            return pandas.read_parquet(
                path,
                columns=self._columns,
                filters=filters,
                storage_options=self._storage_options,
            )

    def _init(self):
        super()._init()
        self._dfs = []
        for path in self._paths:
            if self._start_filter or self._end_filter:
                df = self._read_filtered_parquet(path)
            else:
                df = pandas.read_parquet(path, columns=self._columns, storage_options=self._storage_options)
            self._dfs.append(df)


class SQLSource(_IterableSource, WithUUID):
    """Use SQL table as input source for a flow.

    :parameter key_field: the primary key of the table.
    :parameter id_field: column to be used as ID for events.
    :parameter db_path: url string connection to sql database.
    :parameter table_name: the name of the table to access, from the current database
    :parameter time_fields: list of all fields that are timestamps
    """

    def __init__(
        self,
        db_path: str,
        table_name: str,
        key_field: Union[None, str, List[str]] = None,
        id_field: str = None,
        time_fields: List[str] = None,
        **kwargs,
    ):

        if key_field is not None:
            kwargs["key_field"] = key_field
        if id_field is not None:
            kwargs["id_field"] = id_field
        _IterableSource.__init__(self, **kwargs)
        WithUUID.__init__(self)

        self.table_name = table_name
        self.db_path = db_path
        self.time_fields = time_fields

        self._key_field = key_field
        self._id_field = id_field

    async def _run_loop(self):
        import sqlalchemy as db

        engine = db.create_engine(self.db_path)
        with engine.connect() as conn:
            query = f"SELECT * FROM {self.table_name}"
            cursor = pandas.read_sql(query, con=conn, parse_dates=self.time_fields, chunksize=100)

            for df in cursor:
                for row in df.itertuples(index=False):
                    body = dict(row._asdict())
                    key = None
                    if self._key_field:
                        if isinstance(self._key_field, list):
                            key = []
                            for key_field in self._key_field:
                                if key_field not in body or pandas.isna(body[key_field]):
                                    self.context.logger.error(
                                        f"For {body} value there is no {self._key_field} " f"field (key_field)"
                                    )
                                    break
                                key.append(body[key_field])
                        else:
                            key = body.get(self._key_field, None)
                            if key is None:
                                self.context.logger.error(
                                    f"For {body} value there is no {self._key_field} field (key_field)"
                                )
                    if self._id_field:
                        event_id = body[self._id_field]
                    else:
                        event_id = self._get_uuid()
                    event = Event(body, key=key, id=event_id)
                    await self._do_downstream(event)
        return await self._do_downstream(_termination_obj)
