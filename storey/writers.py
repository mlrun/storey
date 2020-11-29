import asyncio
import csv
import json
import queue
import random
from typing import Optional, Union, List, Callable

import pandas as pd
import v3io_frames as frames

from . import V3ioDriver
from .dtypes import V3ioError, Event
from .flow import Flow, _termination_obj, _split_path, _Batching, _ConcurrentByKeyJobExecution
from .table import Table


class _Writer:
    def __init__(self, columns: Union[str, List[str], None], infer_columns_from_data: Optional[bool],
                 index_cols: Union[str, List[str], None] = None):
        if infer_columns_from_data is None:
            infer_columns_from_data = not bool(columns)
        self._infer_columns_from_data = infer_columns_from_data

        self._metadata_columns = {}
        self._metadata_index_columns = {}
        self._rename_columns = {}
        self._rename_index_columns = {}
        columns = [columns] if isinstance(columns, str) else columns
        index_cols = [index_cols] if isinstance(index_cols, str) else index_cols

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

        self._columns = parse_notation(columns, self._metadata_columns, self._rename_columns)
        self._index_cols = parse_notation(index_cols, self._metadata_index_columns, self._rename_index_columns)

    @staticmethod
    def _get_column_data_from_dict(new_data, event, columns, metadata_columns, rename_columns):
        if columns:
            for column in columns:
                if column in metadata_columns:
                    metadata_attr = metadata_columns[column]
                    new_value = getattr(event, metadata_attr)
                elif column in rename_columns:
                    new_value = event.body[rename_columns[column]]
                else:
                    new_value = event.body[column]
                new_data.append(new_value)

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
                self._columns.extend(data.keys())
                self._columns.sort()
                self._infer_columns_from_data = False
            data = []
            self._get_column_data_from_dict(data, event, self._columns, self._metadata_columns, self._rename_columns)
            self._get_column_data_from_dict(data, event, self._index_cols, self._metadata_index_columns, self._rename_index_columns)
        elif isinstance(data, list):
            if self._infer_columns_from_data:
                raise TypeError('Cannot infer_columns_from_data when event type is list. Inference is only possible from dict.')
            sub_metadata = bool(self._columns) and bool(self._metadata_columns)
            sub_index_metadata = bool(self._index_cols) and bool(self._metadata_index_columns)
            if sub_metadata or sub_index_metadata:
                data = []
                cursor = self._get_column_data_from_list(data, event, event.body, self._index_cols, self._metadata_index_columns)
                self._get_column_data_from_list(data, event, event.body[cursor:], self._columns, self._metadata_columns)
        else:
            raise TypeError('Writer supports only events of type dict or list.')
        return data


class WriteToCSV(_Batching, _Writer):
    """Writes events to a CSV file.

    :param path: path where CSV file will be written.
    :param columns: Fields to be written to CSV. Will be written as the file header if write_header is True. Will be extracted from
    events when an event is a dictionary (lists will be written as is). Use = notation for renaming fields (e.g. write_this=event_field).
    Use $ notation to refer to metadata ($key, event_time=$time). Optional. Defaults to None (will be inferred if event is dictionary).
    :param header: Whether to write the columns as a CSV header.
    :param infer_columns_from_data: Whether to infer columns from the first event, when events are dictionaries. If True, columns will be
    inferred from data and used in place of explicit columns list if none was provided, or appended to the provided list. If header is True
    and columns is not provided, infer_columns_from_data=True is implied. Optional. Default to False if columns is provided, True otherwise.
    :param force_flush_after: Number of lines to write before flushing data to the output file. Defaults to 1 (after every line). Set to
    zero to disable (leave flushing to the standard library).
    :param name: Name of this step, as it should appear in logs. Defaults to class name (WriteToCSV).
    :type name: string
    """

    def __init__(self, path: str, columns: Optional[List[str]] = None, header: bool = False, infer_columns_from_data: bool = False,
                 force_flush_after: int = 1, **kwargs):
        _Batching.__init__(self, max_events=128, timeout_secs=3, **kwargs)
        _Writer.__init__(self, columns, infer_columns_from_data or header and not columns)

        self._path = path
        self._write_header = header
        self._force_flush_after = force_flush_after if force_flush_after > 0 else 0
        self._blocking_io_loop_future = None
        self._data_buffer = queue.Queue(1024)

    def _blocking_io_loop(self):
        got_first_event = False
        with open(self._path, mode='w') as f:
            csv_writer = csv.writer(f)
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
                    if self._force_flush_after and line_number % self._force_flush_after == 0:
                        f.flush()

    def _event_to_batch_entry(self, event):
        return self._event_to_writer_entry(event)

    async def _terminate(self):
        asyncio.get_running_loop().run_in_executor(None, lambda: self._data_buffer.put(_termination_obj))
        await self._blocking_io_loop_future

    async def _emit(self, batch, batch_time):
        if not self._blocking_io_loop_future:
            self._blocking_io_loop_future = asyncio.get_running_loop().run_in_executor(None, self._blocking_io_loop)

        asyncio.get_running_loop().run_in_executor(None, lambda: self._data_buffer.put(batch))

        for data in batch:
            await self._do_downstream(data)


class WriteToParquet(_Batching, _Writer):
    """Writes incoming events to parquet files.

    :param path: Output path. Can be either a file or directory. This parameter is forwarded as-is to pandas.DataFrame.to_parquet().
    :param index_cols: Index columns for writing the data. Use = notation for renaming fields (e.g. write_this=event_field). Use $ notation
    to refer to metadata ($key, event_time=$time).If None (default), no index is set.
    :param columns: Fields to be written to parquet. Will be extracted from events when an event is a dictionary (lists will be written as
    is). Use = notation for renaming fields (e.g. write_this=event_field). Use $ notation to refer to metadata ($key, event_time=$time).
    Optional. Defaults to None (will be inferred if event is dictionary).
    :param infer_columns_from_data: Whether to infer columns from the first event, when events are dictionaries. If True, columns will be
    inferred from data and used in place of explicit columns list if none was provided, or appended to the provided list. If header is True
    and columns is not provided, infer_columns_from_data=True is implied. Optional. Default to False if columns is provided, True otherwise.
    :param partition_cols: Columns by which to partition the data into separate parquet files. If None (default), data will be written
    to a single file at path. This parameter is forwarded as-is to pandas.DataFrame.to_parquet().
    :param max_events: Maximum number of events to write at a time. If None (default), all events will be written on flow termination,
    or after timeout_secs (if timeout_secs is set).
    :type max_events: int
    :param timeout_secs: Maximum number of seconds to hold events before they are written. If None (default), all events will be written
    on flow termination, or after max_events are accumulated (if max_events is set).
    :type timeout_secs: int
    """

    def __init__(self, path: str, index_cols: Union[str, List[str], None] = None, columns: Union[str, List[str], None] = None,
                 partition_cols: Optional[List[str]] = None, infer_columns_from_data: Optional[bool] = None, **kwargs):
        _Batching.__init__(self, **kwargs)
        _Writer.__init__(self, columns, infer_columns_from_data, index_cols)

        self._path = path
        self._partition_cols = partition_cols

    def _event_to_batch_entry(self, event):
        return self._event_to_writer_entry(event)

    async def _emit(self, batch, batch_time):
        df_columns = []
        df_columns.extend(self._columns)
        if self._index_cols:
            df_columns.extend(self._index_cols)
        df = pd.DataFrame(batch, columns=df_columns)
        if self._index_cols:
            df.set_index(self._index_cols, inplace=True)
        df.to_parquet(path=self._path, index=bool(self._index_cols), partition_cols=self._partition_cols)


class WriteToTSDB(_Batching, _Writer):
    """Writes incoming events to TSDB table.

    :param path: Path to TSDB table.
    :param time_col: Name of the time column. Optional. Defaults to '$time'.
    :param columns: List of column names to be passed to the DataFrame constructor. Use = notation for renaming fields (e.g.
    write_this=event_field). Use $ notation to refer to metadata ($key, event_time=$time).
    :param infer_columns_from_data: Whether to infer columns from the first event, when events are dictionaries. If True, columns will be
    inferred from data and used in place of explicit columns list if none was provided, or appended to the provided list. If header is True
    and columns is not provided, infer_columns_from_data=True is implied. Optional. Default to False if columns is provided, True otherwise.
    :param index_cols: List of column names to be used for metric labels.
    :param v3io_frames: Frames service url.
    :param access_key: Access key to the system.
    :param container: Container name for this TSDB table.
    :param rate: TSDB table sample rate.
    :param aggr: Server-side aggregations for this TSDB table (e.g. 'sum,count').
    :param aggr_granularity: Granularity of server-side aggregations for this TSDB table (e.g. '1h').
    """

    def __init__(self, path: str, time_col: str = '$time', columns: Union[str, List[str], None] = None,
                 infer_columns_from_data: bool = False, index_cols: Union[str, List[str], None] = None, v3io_frames: Optional[str] = None,
                 access_key: Optional[str] = None, container: str = "", rate: str = "", aggr: str = "", aggr_granularity: str = "",
                 frames_client=None, **kwargs):
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

    def _event_to_batch_entry(self, event):
        return self._event_to_writer_entry(event)

    async def _emit(self, batch, batch_time):
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


class WriteToV3IOStream(Flow):
    """Writes all incoming events into a V3IO stream.

    :param storage: V3IO driver.
    :param stream_path: Path to the V3IO stream.
    :param sharding_func: Function for determining the shard ID to which to write each event.
    :param batch_size: Batch size for each write request.
    """

    def __init__(self, storage: V3ioDriver, stream_path: str, sharding_func: Optional[Callable[[Event], int]] = None, batch_size: int = 8,
                 **kwargs):
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
    inferred from data and used in place of explicit columns list if none was provided, or appended to the provided list. If header is True
    and columns is not provided, infer_columns_from_data=True is implied. Optional. Default to False if columns is provided, True otherwise.
    """

    def __init__(self, table: Union[Table, str], columns: Optional[List[str]] = None, infer_columns_from_data: bool = False, **kwargs):
        _ConcurrentByKeyJobExecution.__init__(self, **kwargs)
        _Writer.__init__(self, columns, infer_columns_from_data)
        self._table = table
        if isinstance(table, str):
            if not self.context:
                raise TypeError("Table can not be string if no context was provided to the step")
            self._table = self.context.get_table(table)
        self._closeables = [self._table]

    async def _process_event(self, events):
        data_to_persist = {}
        data = self._event_to_writer_entry(events[-1])
        for i, col_name in enumerate(self._columns):
            data_to_persist[col_name] = data[i]
        return await self._table.persist_key(events[0].key, data_to_persist)

    async def _handle_completed(self, event, response):
        await self._do_downstream(event)
