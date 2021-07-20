import asyncio
import copy
import csv
import datetime
import hashlib
import json
import os
import queue
import random
import uuid
from typing import Optional, Union, List, Callable, Tuple
from urllib.parse import urlparse

import pandas as pd
import pyarrow
import v3io_frames as frames

from . import Driver
from .dtypes import V3ioError, Event
from .flow import Flow, _termination_obj, _split_path, _Batching
from .table import Table, _PersistJob
from .utils import url_to_file_system, stringify_key


class _Writer:
    def __init__(self, columns: Union[str, List[Union[str, Tuple[str, str]]], None], infer_columns_from_data: Optional[bool],
                 index_cols: Union[str, List[Union[str, Tuple[str, str]]], None] = None, partition_cols: Union[str, List[str], None] = None,
                 retain_dict: bool = False, storage_options: Optional[dict] = None):
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

        def unzip_cols(columns):
            column_types = []
            if columns and isinstance(columns[0], Tuple):
                columns_no_types = []
                for column in columns:
                    name, column_type = column
                    columns_no_types.append(name)
                    column_types.append(column_type)
            else:
                columns_no_types = columns
            return columns_no_types, column_types

        columns_no_types, column_types = unzip_cols(columns)
        index_cols_no_types, index_cols_types = unzip_cols(index_cols)

        self._initial_columns = parse_notation(columns_no_types, self._metadata_columns, self._rename_columns)
        self._initial_index_cols = parse_notation(index_cols_no_types, self._metadata_index_columns, self._rename_index_columns)
        self._column_types = column_types
        self._index_column_types = index_cols_types

        if column_types:
            fields = []
            for i in range(len(index_cols_types)):
                index_column = index_cols_no_types[i]
                type_name = index_cols_types[i]
                typ = self._type_string_to_pyarrow_type[type_name]
                field = pyarrow.field(index_column, typ, True)
                fields.append(field)
            for i in range(len(column_types)):
                type_name = column_types[i]
                typ = self._type_string_to_pyarrow_type[type_name]
                name = self._initial_columns[i]
                if partition_cols and name in partition_cols:
                    continue
                field = pyarrow.field(name, typ, True)
                fields.append(field)
            self._schema = pyarrow.schema(fields)
        else:
            self._schema = None

        if isinstance(partition_cols, str):
            partition_cols = [partition_cols]
        self._partition_cols = partition_cols

        if partition_cols is not None and index_cols is not None:
            cols_both_partition_and_index = set(partition_cols).intersection(set(index_cols))
            if cols_both_partition_and_index:
                raise ValueError(f'The following columns are used both for partitioning and indexing, which is not allowed: '
                                 f'{list(cols_both_partition_and_index)}')

    _type_string_to_pyarrow_type = {
        'str': pyarrow.string(),
        'int32': pyarrow.int32(),
        'int': pyarrow.int64(),
        'float32': pyarrow.float32(),
        'float': pyarrow.float64(),
        'bool': pyarrow.bool_(),
        'datetime': pyarrow.timestamp('ns'),
    }

    def _init(self):
        self._columns = copy.copy(self._initial_columns)
        self._index_cols = copy.copy(self._initial_index_cols)
        self._init_partition_col_indices()

    def _init_partition_col_indices(self):
        self._partition_col_to_index = {}
        self._partition_col_indices = []
        self._non_partition_columns = self._columns

        self._non_partition_column_types = self._column_types
        if self._partition_cols is not None and self._columns is not None:
            self._non_partition_columns = []
            self._non_partition_column_types = []
            for index, col in enumerate(self._columns):
                if col in self._partition_cols:
                    self._partition_col_to_index[col] = index
                    self._partition_col_indices.append(index)
                else:
                    self._non_partition_columns.append(col)
                    if self._column_types:
                        self._non_partition_column_types.append(self._column_types[index])

    def _path_from_event(self, event):
        res = '/'
        for col in self._partition_cols:
            hash_into = 0
            if isinstance(col, tuple):
                col, hash_into = col
            if col == '$key':
                val = event.key
                if isinstance(val, list):
                    val = '.'.join(map(str, val))
            elif col == '$date':
                val = f'{event.time.year:02}-{event.time.month:02}-{event.time.day:02}'
            elif col == '$year':
                val = f'{event.time.year:02}'
            elif col == '$month':
                val = f'{event.time.month:02}'
            elif col == '$day':
                val = f'{event.time.day:02}'
            elif col == '$hour':
                val = f'{event.time.hour:02}'
            elif col == '$minute':
                val = f'{event.time.minute:02}'
            elif col == '$second':
                val = f'{event.time.second:02}'
            else:
                if isinstance(event.body, list):
                    val = event.body[self._partition_col_to_index[col]]
                else:
                    val = event.body[col]

            if col.startswith('$'):
                col = col[1:]

            if hash_into:
                col = f'hash{hash_into}_{col}'
                if isinstance(val, list):
                    val = '.'.join(map(str, val))
                else:
                    val = str(val)
                sha1 = hashlib.sha1()
                sha1.update(val.encode('utf8'))
                val = int(sha1.hexdigest(), 16) % hash_into

            res += f'{col}={val}/'
        return res

    def _get_column_data_from_dict(self, new_data, event, columns, columns_types, metadata_columns, rename_columns):
        if columns:
            for index, column in enumerate(columns):
                if column in metadata_columns:
                    metadata_attr = metadata_columns[column]
                    new_value = getattr(event, metadata_attr)
                elif column in rename_columns:
                    new_value = self._field_extractor(event.body, rename_columns[column])
                else:
                    new_value = self._field_extractor(event.body, column)

                if new_value is None and not self._write_missing_fields:
                    continue

                if columns_types:
                    column_type = columns_types[index]
                    if column_type == 'datetime':
                        if isinstance(new_value, str):
                            new_value = datetime.datetime.fromisoformat(new_value)
                        elif isinstance(new_value, int):
                            new_value = datetime.datetime.utcfromtimestamp(new_value)

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
                self._init_partition_col_indices()
            data = {} if self._retain_dict else []
            self._get_column_data_from_dict(data, event, self._index_cols, self._index_column_types, self._metadata_index_columns,
                                            self._rename_index_columns)
            self._get_column_data_from_dict(data, event, self._non_partition_columns, self._non_partition_column_types,
                                            self._metadata_columns, self._rename_columns)
        elif isinstance(data, list):
            for index in self._partition_col_indices:
                del data[index]
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


class CSVTarget(_Batching, _Writer):
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
    :param name: Name of this step, as it should appear in logs. Defaults to class name (CSVTarget).
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

    async def _emit(self, batch, batch_key, batch_time, last_event_time=None):
        if not self._blocking_io_loop_future:
            self._blocking_io_loop_future = asyncio.get_running_loop().run_in_executor(None, self._blocking_io_loop)

        if self._blocking_io_loop_failed:
            await self._blocking_io_loop_future
        else:
            await asyncio.get_running_loop().run_in_executor(None, lambda: self._data_buffer.put(batch))


class ParquetTarget(_Batching, _Writer):
    """Writes incoming events to parquet files.

    :param path: Output path. Can be either a file or directory. This parameter is forwarded as-is to pandas.DataFrame.to_parquet().
    :param index_cols: Index columns for writing the data. Use = notation for renaming fields (e.g. write_this=event_field). Use $ notation
        to refer to metadata ($key, event_time=$time).If None (default), no index is set.
    :param columns: Fields to be written to parquet. Will be extracted from events when an event is a dictionary
        (lists will be written as is). Use = notation for renaming fields (e.g. write_this=event_field).
        Use $ notation to refer to metadata ($key, event_time=$time). Can be a list of (name, type) tuples in order to set the
        schema explicitly, e.g. ('my_field', 'str'). Supported types: str, int32, int, float32, float, bool, datetime.
        Optional. Defaults to None (will be inferred if event is dictionary).
    :param infer_columns_from_data: Whether to infer columns from the first event, when events are dictionaries.
        If True, columns will be inferred from data and used in place of explicit columns list if none was provided, or appended to the
        provided list. If header is True and columns is not provided, infer_columns_from_data=True is implied.
        Optional. Default to False if columns is provided, True otherwise.
    :param partition_cols: Columns by which to partition the data into directories. The following metadata columns are also supported:
        $key, $date (e.g. 2020-02-09), $year, $month, $day, $hour, $minute, $second. A column may be specified as a tuple, such as
        ('$key', 64), which means partitioning by the event key hashed into 64 partitions.
        If None (the default), the data will only be partitioned if the path ends in .parquet or .pq. Otherwise, it will be partitioned by
        key/year/month/day/hour, where the key is hashed into 256 buckets.
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

    def __init__(self, path: str, index_cols: Union[str, List[str], None] = None,
                 columns: Union[str, Union[List[str], List[Tuple[str, str]]], None] = None,
                 partition_cols: Union[str, Union[List[str], List[Tuple[str, int]]], None] = None,
                 infer_columns_from_data: Optional[bool] = None, max_events: Optional[int] = None,
                 flush_after_seconds: Optional[int] = None, **kwargs):
        self._single_file_mode = False
        if isinstance(partition_cols, str):
            partition_cols = [partition_cols]
        if partition_cols is None:
            if path.endswith('.parquet') or path.endswith('.pq'):
                self._single_file_mode = True
            else:
                partition_cols = [('$key', 256), '$year', '$month', '$day', '$hour']
        else:
            kwargs['partition_cols'] = partition_cols

        if max_events is None and not self._single_file_mode:
            max_events = 10000
        if flush_after_seconds is None and not self._single_file_mode:
            flush_after_seconds = 60

        kwargs['path'] = path
        if not self._single_file_mode and path.endswith('/'):
            path = path[:-1]
        if index_cols is not None:
            kwargs['index_cols'] = index_cols
        if columns is not None:
            kwargs['columns'] = columns
        if infer_columns_from_data is not None:
            kwargs['infer_columns_from_data'] = infer_columns_from_data

        storage_options = kwargs.get('storage_options')
        self._file_system, self._path = url_to_file_system(path, storage_options)
        self._full_path = path

        path_from_event = self._path_from_event if partition_cols else None

        _Batching.__init__(self, max_events=max_events, flush_after_seconds=flush_after_seconds, key=path_from_event, **kwargs)
        _Writer.__init__(self, columns, infer_columns_from_data, index_cols, partition_cols, storage_options=storage_options)

        self._field_extractor = lambda event_body, field_name: event_body.get(field_name)
        self._write_missing_fields = True
        self._fs_status = kwargs.get('featureset_status')
        self._last_written_event = None

    def _init(self):
        _Batching._init(self)
        _Writer._init(self)

    def _event_to_batch_entry(self, event):
        return self._event_to_writer_entry(event)

    async def _emit(self, batch, batch_key, batch_time, last_event_time=None):
        df_columns = []
        if self._index_cols:
            df_columns.extend(self._index_cols)
        df_columns.extend(self._non_partition_columns)
        df = pd.DataFrame(batch, columns=df_columns)
        if self._index_cols:
            df.set_index(self._index_cols, inplace=True)
        dir_path = os.path.dirname(self._path) if self._single_file_mode else self._path
        if self._partition_cols:
            dir_path = f'{dir_path}{batch_key}'
        else:
            dir_path += '/'
        if dir_path:
            self._file_system.makedirs(dir_path, exist_ok=True)
        file_path = self._path if self._single_file_mode else f'{dir_path}{uuid.uuid4()}.parquet'
        # Remove nanosecs from timestamp columns & index
        for name, _ in df.items():
            if pd.core.dtypes.common.is_string_dtype(df[name]) and self._schema and \
                    isinstance(self._schema.field(name).type, pyarrow.TimestampType):
                df[name] = pd.to_datetime(df[name])
            elif pd.core.dtypes.common.is_datetime64_dtype(df[name]):
                df[name] = df[name].astype('datetime64[us]')
        if pd.core.dtypes.common.is_datetime64_dtype(df.index) or pd.core.dtypes.common.is_datetime64tz_dtype(df.index):
            df.index = df.index.floor('u')
        with self._file_system.open(file_path, 'wb') as file:
            kwargs = {}
            if self._schema is not None:
                kwargs['schema'] = self._schema
            df.to_parquet(path=file, index=bool(self._index_cols), **kwargs)
            if not self._last_written_event or last_event_time > self._last_written_event:
                self._last_written_event = last_event_time

    async def _terminate(self):
        if self._fs_status:
            self._fs_status.update_last_written_for_target(self._full_path, self._last_written_event)


class TSDBTarget(_Batching, _Writer):
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
                 v3io_frames: Optional[str] = None, access_key: Optional[str] = None, rate: str = "", aggr: str = "",
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
        parts = urlparse(path)
        self._path = parts.path
        container = parts.netloc
        if not parts.scheme:
            container, self._path = _split_path(self._path)
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

    async def _emit(self, batch, batch_key, batch_time, last_event_time=None):
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


class StreamTarget(Flow, _Writer):
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
            raise AssertionError("StreamTarget worker has already terminated")

        if event is _termination_obj:
            await self._q.put(_termination_obj)
            await self._worker_awaitable
            return await self._do_downstream(_termination_obj)
        else:
            await self._q.put(event)
            if self._worker_awaitable.done():
                await self._worker_awaitable


class NoSqlTarget(_Writer, Flow):
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

    def __init__(self, table: Union[Table, str], columns: Optional[List[Union[str, Tuple[str, str]]]] = None,
                 infer_columns_from_data: Optional[bool] = None, **kwargs):
        kwargs['table'] = table
        if columns:
            kwargs['columns'] = columns
        if infer_columns_from_data:
            kwargs['infer_columns_from_data'] = infer_columns_from_data
        Flow.__init__(self, **kwargs)
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
        _Writer._init(self)

    async def _handle_completed(self, event, response):
        await self._do_downstream(event)

    async def _do(self, event):
        if event is _termination_obj:
            await self._table._terminate()
            return await self._do_downstream(_termination_obj)

        if event.key is None:
            raise ValueError("Event could not be written to table because it has no key")

        key = stringify_key(event.key)
        if not self._table._flush_interval_secs:
            data_to_persist = self._event_to_writer_entry(event)
            await self._table._persist(_PersistJob(key, data_to_persist, self._handle_completed, event))
        else:
            data_to_persist = self._event_to_writer_entry(event)
            async with self._table._get_lock(key):
                self._table._update_static_attrs(key, data_to_persist)
            self._table._init_flush_task()
            await self._do_downstream(event)
