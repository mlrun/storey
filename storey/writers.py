import asyncio
import copy
import csv
import json
import os
import queue
import random
import uuid
from typing import Optional, Union, List, Callable

import pandas as pd
import v3io_frames as frames

from . import Driver
from .dtypes import V3ioError, Event
from .flow import Flow, _termination_obj, _split_path, _Batching, _ConcurrentByKeyJobExecution
from .table import Table
from .utils import url_to_file_system


class _Writer:
    def __init__(self, columns: Union[str, List[str], None], infer_columns_from_data: Optional[bool],
                 index_cols: Union[str, List[str], None] = None, retain_dict: bool = False,
                 storage_options: Optional[dict] = None):
        if infer_columns_from_data is None:
            infer_columns_from_data = not bool(columns)
        self._infer_columns_from_data = infer_columns_from_data

        self._metadata_columns = {}
        self._metadata_index_columns = {}
        self._rename_columns = {}
        self._rename_index_columns = {}
        columns = [columns] if isinstance(columns, str) else columns
        index_cols = [index_cols] if isinstance(index_cols, str) else index_cols
        self._retain_dict = retain_dict
        self._storage_options = storage_options

        self._field_extractor = lambda event_body, field_name: event_body[field_name]
        self._write_missing_fields = False

        def parse_notation(columns, metadata_columns, rename_columns):
            result = []
            if columns:
                for col in columns:
                    if col.startswith('$'):
                        col = col[1:]
                        metadata_columns[col] = col
                    elif '=$' in col:
                        col, metadata_attr = col.split('=$', maxsplit=1)
                        metadata_columns[col] = metadata_attr
                    elif '=' in col:
                        col, rename_from = col.split('=', maxsplit=1)
                        rename_columns[col] = rename_from
                    result.append(col)
            return result

        self._initial_columns = parse_notation(columns, self._metadata_columns, self._rename_columns)
        self._initial_index_cols = parse_notation(index_cols, self._metadata_index_columns, self._rename_index_columns)

    def _init(self):
        self._columns = copy.copy(self._initial_columns)
        self._index_cols = copy.copy(self._initial_index_cols)

    def _get_column_data_from_dict(self, new_data, event, columns, metadata_columns, rename_columns):
        if columns:
            for column in columns:
                if column in metadata_columns:
                    metadata_attr = metadata_columns[column]
                    new_value = getattr(event, metadata_attr)
                elif column in rename_columns:
                    new_value = self._field_extractor(event.body, rename_columns[column])
                else:
                    new_value = self._field_extractor(event.body, column)

                if new_value is None and not self._write_missing_fields:
                    continue

                if isinstance(new_data, list):
                    new_data.append(new_value)
                else:
                    new_data[column] = new_value

    @staticmethod
    def _get_column_data_from_list(new_data, event, original_data, columns, metadata_columns):
        data_cursor = 0
        for column in columns:
            if column in metadata_columns:
                metadata_attr = metadata_columns[column]
                new_value = getattr(event, metadata_attr)
                new_data.append(new_value)
            else:
                new_data.append(original_data[data_cursor])
                data_cursor += 1
        return data_cursor

    def _event_to_writer_entry(self, event):
        data = event.body
        if isinstance(data, dict):
            if self._infer_columns_from_data:
                self._columns.extend(data.keys() - self._index_cols)
                self._columns.sort()
                self._infer_columns_from_data = False
            data = {} if self._retain_dict else []
            self._get_column_data_from_dict(data, event, self._index_cols, self._metadata_index_columns, self._rename_index_columns)
            self._get_column_data_from_dict(data, event, self._columns, self._metadata_columns, self._rename_columns)
        elif isinstance(data, list):
            if self._infer_columns_from_data:
                raise TypeError('Cannot infer_columns_from_data when event type is list. Inference is only possible from dict.')
            sub_metadata = bool(self._columns) and bool(self._metadata_columns)
            sub_index_metadata = bool(self._index_cols) and bool(self._metadata_index_columns)
            if sub_metadata or sub_index_metadata:
                data = []
                cursor = self._get_column_data_from_list(data, event, event.body, self._index_cols, self._metadata_index_columns)
                self._get_column_data_from_list(data, event, event.body[cursor:], self._columns, self._metadata_columns)
        elif self._columns:
            raise TypeError('Writer supports only events of type dict or list.')
        return data


class _V3ioCSVDialect(csv.Dialect):
    """Describe a dialect based on excel dialect but with '\n' line terminator"""
    delimiter = ','
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL


class WriteToCSV(_Batching, _Writer):
    """Writes events to a CSV file.

    :param path: path where CSV file will be written.
    :param columns: Fields to be written to CSV. Will be written as the file header if write_header is True. Will be extracted from
        events when an event is a dictionary (lists will be written as is). Use = notation for renaming fields
        (e.g. write_this=event_field).
        Use $ notation to refer to metadata ($key, event_time=$time). Optional. Defaults to None (will be inferred if event is dictionary).
    :param header: Whether to write the columns as a CSV header.
    :param infer_columns_from_data: Whether to infer columns from the first event, when events are dictionaries. If True, columns will be
        inferred from data and used in place of explicit columns list if none was provided, or appended to the provided list.
        If header is True and columns is not provided, infer_columns_from_data=True is implied.
        Optional. Default to False if columns is provided, True otherwise.
    :param max_lines_before_flush: Number of lines to write before flushing data to the output file. Defaults to 128.
    :param max_seconds_before_flush: Maximum delay in seconds before flushing lines. Defaults to 3.
    :param name: Name of this step, as it should appear in logs. Defaults to class name (WriteToCSV).
    :type name: string
    :param max_events: Maximum number of events to write at a time. If None (default), all events will be written on flow termination,
        or after timeout_secs (if timeout_secs is set).
    :type max_events: int
    :param timeout_secs: Maximum number of seconds to hold events before they are written. If None (default), all events will be written
        on flow termination, or after max_events are accumulated (if max_events is set).
    :type timeout_secs: int
    :param key: batching will be done by key
    :type key: str or Event
    :param storage_options: Extra options that make sense for a particular storage connection, e.g. host, port, username, password, etc.,
        if using a URL that will be parsed by fsspec, e.g., starting “s3://”, “gcs://”. Optional
    :type storage_options: dict
    """

    def __init__(self, path: str, columns: Optional[List[str]] = None, header: bool = False, infer_columns_from_data: Optional[bool] = None,
                 max_lines_before_flush: int = 128, max_seconds_before_flush: int = 3, **kwargs):
        _Batching.__init__(self, max_events=max_lines_before_flush, timeout_secs=max_seconds_before_flush, **kwargs)
        _Writer.__init__(self, columns, infer_columns_from_data, storage_options=kwargs.get('storage_options'))

        self._path = path
        self._write_header = header

        self._field_extractor = lambda event_body, field_name: event_body.get(field_name, '')
        self._write_missing_fields = True

    def _init(self):
        _Batching._init(self)
        _Writer._init(self)
        self._blocking_io_loop_future = None
        self._data_buffer = queue.Queue(1024)
        self._blocking_io_loop_failed = False

    def _blocking_io_loop(self):
        try:
            got_first_event = False
            fs, file_path = url_to_file_system(self._path, self._storage_options)
            dirname = os.path.dirname(self._path)
            if dirname:
                fs.makedirs(dirname, exist_ok=True)
            with fs.open(file_path, mode='w') as f:
                csv_writer = csv.writer(f, _V3ioCSVDialect())
                line_number = 0
                while True:
                    batch = self._data_buffer.get()
                    if batch is _termination_obj:
                        break
                    for data in batch:
                        if not got_first_event:
                            if not self._columns and self._write_header:
                                raise ValueError('columns must be defined when header is True and events type is not dictionary')
                            if self._write_header:
                                csv_writer.writerow(self._columns)
                            got_first_event = True
                        csv_writer.writerow(data)
                        line_number += 1
                    f.flush()
        except BaseException as ex:
            self._blocking_io_loop_failed = True
            if not self._data_buffer.empty():
                self._data_buffer.get()
            raise ex

    def _event_to_batch_entry(self, event):
        writer_entry = self._event_to_writer_entry(event)
        if not isinstance(writer_entry, list):
            raise TypeError(f'CSV writer does not support event body of type {type(event.body)}.')
        return writer_entry

    async def _terminate(self):
        asyncio.get_running_loop().run_in_executor(None, lambda: self._data_buffer.put(_termination_obj))
        await self._blocking_io_loop_future

    async def _emit(self, batch, batch_key, batch_time):
        if not self._blocking_io_loop_future:
            self._blocking_io_loop_future = asyncio.get_running_loop().run_in_executor(None, self._blocking_io_loop)

        if self._blocking_io_loop_failed:
            await self._blocking_io_loop_future
        else:
            await asyncio.get_running_loop().run_in_executor(None, lambda: self._data_buffer.put(batch))


class WriteToParquet(_Batching, _Writer):
    """Writes incoming events to parquet files.

    :param path: Output path. Can be either a file or directory. This parameter is forwarded as-is to pandas.DataFrame.to_parquet().
    :param index_cols: Index columns for writing the data. Use = notation for renaming fields (e.g. write_this=event_field). Use $ notation
        to refer to metadata ($key, event_time=$time).If None (default), no index is set.
    :param columns: Fields to be written to parquet. Will be extracted from events when an event is a dictionary
        (lists will be written as is). Use = notation for renaming fields (e.g. write_this=event_field).
        Use $ notation to refer to metadata ($key, event_time=$time).
        Optional. Defaults to None (will be inferred if event is dictionary).
    :param infer_columns_from_data: Whether to infer columns from the first event, when events are dictionaries.
        If True, columns will be inferred from data and used in place of explicit columns list if none was provided, or appended to the
        provided list. If header is True and columns is not provided, infer_columns_from_data=True is implied.
        Optional. Default to False if columns is provided, True otherwise.
    :param partition_cols: Columns by which to partition the data into separate parquet files. If None (default), data will be written
        to a single file at path. This parameter is forwarded as-is to pandas.DataFrame.to_parquet().
    :param max_events: Maximum number of events to write at a time. If None (default), all events will be written on flow termination,
        or after timeout_secs (if timeout_secs is set).
    :type max_events: int
    :param timeout_secs: Maximum number of seconds to hold events before they are written. If None (default), all events will be written
        on flow termination, or after max_events are accumulated (if max_events is set).
    :type timeout_secs: int
    :param storage_options: Extra options that make sense for a particular storage connection, e.g. host, port, username, password, etc.,
        if using a URL that will be parsed by fsspec, e.g., starting “s3://”, “gcs://”. Optional
    :type storage_options: dict
    """

    def __init__(self, path: str, index_cols: Union[str, List[str], None] = None, columns: Union[str, List[str], None] = None,
                 partition_cols: Union[str, List[str], None] = None, infer_columns_from_data: Optional[bool] = None,
                 max_events: Optional[int] = None, flush_after_seconds: Optional[int] = None, **kwargs):
        if max_events is None:
            max_events = 10000
        if flush_after_seconds is None:
            flush_after_seconds = 60

        kwargs['path'] = path
        if not path.endswith('/'):
            path += '/'
        if index_cols is not None:
            kwargs['index_cols'] = index_cols
        if columns is not None:
            kwargs['columns'] = columns
        if partition_cols is not None:
            kwargs['partition_cols'] = partition_cols
        if infer_columns_from_data is not None:
            kwargs['infer_columns_from_data'] = infer_columns_from_data

        storage_options = kwargs.get('storage_options')
        self._file_system, self._path = url_to_file_system(path, storage_options)

        if isinstance(partition_cols, str):
            partition_cols = [partition_cols]
        self._partition_cols = partition_cols

        partition_col_indices = {}
        if partition_cols is not None:
            for index, col in enumerate(columns):
                if col in self._partition_cols:
                    partition_col_indices[col] = index

        def path_from_event(event):
            res = '/'
            for col in partition_cols:
                if col == '$date':
                    val = f'{event.time.year()}-{event.time.month()}-{event.time.day()}'
                elif col == '$year':
                    val = event.time.year()
                elif col == '$month':
                    val = event.time.month()
                elif col == '$day':
                    val = event.time.day()
                else:
                    if isinstance(event.body, list):
                        val = event.body[partition_col_indices[col]]
                    else:
                        val = event.body[col]
                res += f'{val}/'
            return res

        path_from_event = path_from_event if partition_cols else None

        _Batching.__init__(self, max_events=max_events, flush_after_seconds=flush_after_seconds, key=path_from_event, **kwargs)
        _Writer.__init__(self, columns, infer_columns_from_data, index_cols, storage_options=storage_options)

        self._field_extractor = lambda event_body, field_name: event_body.get(field_name)
        self._write_missing_fields = True

    def _init(self):
        _Batching._init(self)
        _Writer._init(self)

    def _event_to_batch_entry(self, event):
        return self._event_to_writer_entry(event)

    async def _emit(self, batch, batch_key, batch_time):
        df_columns = []
        if self._index_cols:
            df_columns.extend(self._index_cols)
        df_columns.extend(self._columns)
        df = pd.DataFrame(batch, columns=df_columns)
        if self._index_cols:
            df.set_index(self._index_cols, inplace=True)
        dir_path = self._path
        if self._partition_cols:
            dir_path = f'{dir_path}{batch_key}'
        file_path = f'{dir_path}{uuid.uuid4()}.parquet'
        self._file_system.makedirs(dir_path, exist_ok=True)
        with self._file_system.open(file_path, 'wb') as file:
            df.to_parquet(path=file, index=bool(self._index_cols))


class WriteToTSDB(_Batching, _Writer):
    """Writes incoming events to TSDB table.

    :param path: Path to TSDB table.
    :param time_col: Name of the time column. Optional. Defaults to '$time'.
    :param columns: List of column names to be passed to the DataFrame constructor. Use = notation for renaming fields (e.g.
        write_this=event_field). Use $ notation to refer to metadata ($key, event_time=$time).
    :param infer_columns_from_data: Whether to infer columns from the first event, when events are dictionaries. If True, columns will be
        inferred from data and used in place of explicit columns list if none was provided, or appended to the provided list. If header is
        True and columns is not provided, infer_columns_from_data=True is implied. Optional. Default to False if columns is provided,
        True otherwise.
    :param index_cols: List of column names to be used for metric labels.
    :param v3io_frames: Frames service url.
    :param access_key: Access key to the system.
    :param container: Container name for this TSDB table.
    :param rate: TSDB table sample rate.
    :param aggr: Server-side aggregations for this TSDB table (e.g. 'sum,count').
    :param aggr_granularity: Granularity of server-side aggregations for this TSDB table (e.g. '1h').
    :param frames_client: Frames instance. Allows usage of an existing frames client.
    :param max_events: Maximum number of events to write at a time. If None (default), all events will be written on flow termination,
        or after timeout_secs (if timeout_secs is set).
    :type max_events: int
    :param timeout_secs: Maximum number of seconds to hold events before they are written. If None (default), all events will be written
        on flow termination, or after max_events are accumulated (if max_events is set).
    :type timeout_secs: int
    :param storage_options: Extra options that make sense for a particular storage connection, e.g. host, port, username, password, etc.,
        if using a URL that will be parsed by fsspec, e.g., starting “s3://”, “gcs://”. Optional
    :type storage_options: dict
    """

    def __init__(self, path: str, time_col: str = '$time', columns: Union[str, List[str], None] = None,
                 infer_columns_from_data: Optional[bool] = None, index_cols: Union[str, List[str], None] = None,
                 v3io_frames: Optional[str] = None, access_key: Optional[str] = None, container: str = "", rate: str = "", aggr: str = "",
                 aggr_granularity: str = "", frames_client=None, **kwargs):
        kwargs['path'] = path
        kwargs['time_col'] = time_col
        if columns is not None:
            kwargs['columns'] = columns
        if infer_columns_from_data is not None:
            kwargs['infer_columns_from_data'] = infer_columns_from_data
        if index_cols is not None:
            kwargs['index_cols'] = index_cols
        if v3io_frames is not None:
            kwargs['v3io_frames'] = v3io_frames
        if container:
            kwargs['container'] = container
        if rate:
            kwargs['rate'] = rate
        if aggr:
            kwargs['aggr'] = aggr
        if aggr_granularity:
            kwargs['aggr_granularity'] = aggr_granularity
        _Batching.__init__(self, **kwargs)
        new_index_cols = [time_col]
        if index_cols:
            if isinstance(index_cols, str):
                index_cols = [index_cols]
            new_index_cols.extend(index_cols)
        _Writer.__init__(self, columns, infer_columns_from_data, index_cols=new_index_cols)

        self._path = path
        self._rate = rate
        self._aggr = aggr
        self.aggr_granularity = aggr_granularity
        self._created = False
        self._frames_client = frames_client or frames.Client(address=v3io_frames, token=access_key, container=container)

    def _init(self):
        _Batching._init(self)
        _Writer._init(self)

    def _event_to_batch_entry(self, event):
        return self._event_to_writer_entry(event)

    async def _emit(self, batch, batch_key, batch_time):
        df_columns = []
        if self._index_cols:
            df_columns.extend(self._index_cols)
        df_columns.extend(self._columns)
        df = pd.DataFrame(batch, columns=df_columns)
        df.set_index(keys=self._index_cols, inplace=True)
        if not self._created and self._rate:
            self._created = True
            self._frames_client.create(
                'tsdb', table=self._path, if_exists=frames.frames_pb2.IGNORE, rate=self._rate,
                aggregates=self._aggr, aggregation_granularity=self.aggr_granularity)
        self._frames_client.write("tsdb", self._path, df)


class WriteToV3IOStream(Flow, _Writer):
    """Writes all incoming events into a V3IO stream.

    :param storage: Database driver.
    :param stream_path: Path to the V3IO stream.
    :param sharding_func: Function for determining the shard ID to which to write each event. Optional. Default is None
    :param batch_size: Batch size for each write request.
    :param columns: Fields to be written to stream. Will be extracted from events when an event is a dictionary (other types will be written
        as is). Use = notation for renaming fields (e.g. write_this=event_field). Use $ notation to refer to metadata
        ($key, event_time=$time). Optional. Defaults to None (will be inferred if event is dictionary).
    :param infer_columns_from_data: Whether to infer columns from the first event, when events are dictionaries. If True, columns will be
        inferred from data and used in place of explicit columns list if none was provided, or appended to the provided list. If header is
        True and columns is not provided, infer_columns_from_data=True is implied. Optional. Default to False if columns is provided,
        True otherwise.
    :param storage_options: Extra options that make sense for a particular storage connection, e.g. host, port, username, password, etc.,
        if using a URL that will be parsed by fsspec, e.g., starting “s3://”, “gcs://”. Optional
    :type storage_options: dict
    """

    def __init__(self, storage: Driver, stream_path: str, sharding_func: Optional[Callable[[Event], int]] = None, batch_size: int = 8,
                 columns: Optional[List[str]] = None, infer_columns_from_data: Optional[bool] = None, **kwargs):
        kwargs['stream_path'] = stream_path
        kwargs['batch_size'] = batch_size
        if columns:
            kwargs['columns'] = columns
        if infer_columns_from_data:
            kwargs['infer_columns_from_data'] = infer_columns_from_data
        Flow.__init__(self, **kwargs)
        _Writer.__init__(self, columns, infer_columns_from_data, retain_dict=True)

        self._storage = storage

        if sharding_func is not None and not callable(sharding_func):
            raise TypeError(f'Expected a callable, got {type(sharding_func)}')

        self._container, self._stream_path = _split_path(stream_path)

        self._sharding_func = sharding_func
        self._batch_size = batch_size

        self._shard_count = None

    def _init(self):
        Flow._init(self)
        _Writer._init(self)

    @staticmethod
    async def _handle_response(request):
        if request:
            response = await request
            if response.output.failed_record_count == 0:
                return
            raise V3ioError(f'Failed to put records to V3IO. Got {response.status_code} response: {response.body}')

    def _build_request_put_records(self, shard_id, records):
        record_list_for_json = []
        for record in records:
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
                record = self._event_to_writer_entry(event)
                buffers[shard_id].append(record)
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
            raise AssertionError("WriteToV3IOStream worker has already terminated")

        if event is _termination_obj:
            await self._q.put(_termination_obj)
            await self._worker_awaitable
            return await self._do_downstream(_termination_obj)
        else:
            await self._q.put(event)
            if self._worker_awaitable.done():
                await self._worker_awaitable


class WriteToTable(_ConcurrentByKeyJobExecution, _Writer):
    """
    Persists the data in `table` to its associated storage by key.

    :param table: A Table object or name to persist. If a table name is provided, it will be looked up in the context.
    :param columns: Fields to be written to the storage. Will be extracted from events when an event is a dictionary (lists will be written
        as is). Use = notation for renaming fields (e.g. write_this=event_field).
        Use $ notation to refer to metadata ($key, event_time=$time). Optional. Defaults to None (will be inferred if event is dictionary).
    :param infer_columns_from_data: Whether to infer columns from the first event, when events are dictionaries. If True, columns will be
        inferred from data and used in place of explicit columns list if none was provided, or appended to the provided list. If header is
        True and columns is not provided, infer_columns_from_data=True is implied. Optional. Default to False if columns is provided, True
        otherwise.
    :param storage_options: Extra options that make sense for a particular storage connection, e.g. host, port, username, password, etc.,
        if using a URL that will be parsed by fsspec, e.g., starting “s3://”, “gcs://”. Optional
    :type storage_options: dict
    """

    def __init__(self, table: Union[Table, str], columns: Optional[List[str]] = None, infer_columns_from_data: Optional[bool] = None,
                 **kwargs):
        kwargs['table'] = table
        if columns:
            kwargs['columns'] = columns
        if infer_columns_from_data:
            kwargs['infer_columns_from_data'] = infer_columns_from_data
        _ConcurrentByKeyJobExecution.__init__(self, **kwargs)
        _Writer.__init__(self, columns, infer_columns_from_data, retain_dict=True)
        self._table = table
        if isinstance(table, str):
            if not self.context:
                raise TypeError("Table can not be string if no context was provided to the step")
            self._table = self.context.get_table(table)
        self._closeables = [self._table]

        self._field_extractor = lambda event_body, field_name: event_body.get(field_name)
        self._write_missing_fields = False

    def _init(self):
        _ConcurrentByKeyJobExecution._init(self)
        _Writer._init(self)

    async def _process_events(self, events):
        data_to_persist = self._event_to_writer_entry(events[-1])
        return await self._table._persist_key(events[0].key, data_to_persist)

    async def _handle_completed(self, event, response):
        await self._do_downstream(event)
