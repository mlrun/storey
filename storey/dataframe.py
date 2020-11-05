import copy
from typing import Optional

import pandas as pd
import v3io_frames as frames

from .flow import _termination_obj, Flow, _Batching


class ReduceToDataFrame(Flow):
    """Builds a pandas DataFrame from events and returns that DataFrame on flow termination.

    :param index: Name of the column to be used as index. Optional. If not set, DataFrame will be range indexed.
    :type index: string
    :param columns: List of column names to be passed as-is to the DataFrame constructor. Optional.
    :type columns: list of string
    :param insert_key_column_as: Name of the column to be inserted for event keys. Optional.
    If not set, event keys will not be inserted into the DataFrame.
    :type insert_key_column_as: string
    :param insert_time_column_as: Name of the column to be inserted for event times. Optional.
    If not set, event times will not be inserted into the DataFrame.
    :type insert_time_column_as: string
    :param insert_id_column_as: Name of the column to be inserted for event IDs. Optional.
    If not set, event IDs will not be inserted into the DataFrame.
    :type insert_id_column_as: string
    """

    def __init__(self, index=None, columns=None, insert_key_column_as=None, insert_time_column_as=None,
                 insert_id_column_as=None, **kwargs):
        super().__init__(**kwargs)
        self._index = index
        self._columns = columns
        self._insert_key_column_as = insert_key_column_as
        self._key_column = []
        self._insert_time_column_as = insert_time_column_as
        self._time_column = []
        self._insert_id_column_as = insert_id_column_as
        self._id_column = []
        self._data = []

    def to(self, outlet):
        raise ValueError("ToDataFrame is a terminal step. It cannot be piped further.")

    async def _do(self, event):
        if event is _termination_obj:
            df = pd.DataFrame(self._data, columns=self._columns)
            if self._insert_key_column_as:
                df[self._insert_key_column_as] = self._key_column
            if self._insert_time_column_as:
                df[self._insert_time_column_as] = self._time_column
            if self._insert_id_column_as:
                df[self._insert_id_column_as] = self._id_column
            if self._index:
                df.set_index(self._index, inplace=True)
            return df
        else:
            body = event.body
            if isinstance(body, dict) or isinstance(body, list):
                self._data.append(body)
                if self._insert_key_column_as:
                    self._key_column.append(event.key)
                if self._insert_time_column_as:
                    self._time_column.append(event.time)
                if self._insert_id_column_as:
                    self._id_column.append(event.id)
            else:
                raise ValueError(f'ToDataFrame step only supports input of type dictionary or list, not {type(body)}')


class ToDataFrame(Flow):
    def __init__(self, index=None, columns=None, **kwargs):
        super().__init__(**kwargs)
        self._index = index
        self._columns = columns

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            df = pd.DataFrame(event.body, columns=self._columns)
            if self._index:
                df.set_index(self._index, inplace=True)
            new_event = copy.copy(event)
            new_event.body = df
            return await self._do_downstream(new_event)


class WriteToParquet(_Batching):
    """Writes incoming events to parquet files.

    :param path: Output path. Can be either a file or directory. This parameter is forwarded as-is to pandas.DataFrame.to_parquet().
    :type path: string
    :param index: Index columns for writing the data. This parameter is forwarded as-is to pandas.DataFrame.set_index().
    If None (default), no index is set.
    :type index: list of string
    :param columns: Regular columns for writing the data. This parameter is forwarded as-is to pandas.DataFrame().
    :type columns: list of string
    :param partition_cols: Columns by which to partition the data into separate parquet files. If None (default), data will be written
    to a single file at path. This parameter is forwarded as-is to pandas.DataFrame.to_parquet().
    :type partition_cols: list of string
    :param max_events: Maximum number of events to write at a time. If None (default), all events will be written on flow termination,
    or after timeout_secs (if timeout_secs is set).
    :type max_events: int
    :param timeout_secs: Maximum number of seconds to hold events before they are written. If None (default), all events will be written
    on flow termination, or after max_events are accumulated (if max_events is set).
    :type timeout_secs: int
    """

    def __init__(self, path, index: Optional[list] = None, columns: Optional[list] = None, partition_cols: Optional[list] = None,
                 max_events: Optional[int] = None, timeout_secs: Optional[int] = None, **kwargs):
        super().__init__(max_events, timeout_secs, **kwargs)

        self._path = path
        self._index = index
        self._columns = columns
        self._partition_cols = partition_cols

    async def _emit(self, batch, batch_time):
        df = pd.DataFrame(batch, columns=self._columns)
        if self._index:
            df.set_index(self._index, inplace=True)
        df.to_parquet(path=self._path, partition_cols=self._partition_cols)

    async def _terminate(self):
        return await self._do_downstream(_termination_obj)


class WriteToTSDB(Flow):
    def __init__(self, path, time_col, columns, labels_cols=None, v3io_frames=None, access_key=None, container="",
                 rate="", aggr="", aggr_granularity="", **kwargs):
        super().__init__(**kwargs)
        self._path = path
        self._time_col = time_col
        self._columns = columns
        self._labels_cols = labels_cols
        self._rate = rate
        self._aggr = aggr
        self.aggr_granularity = aggr_granularity
        self._created = False
        self._frames_client = frames.Client(address=v3io_frames, token=access_key, container=container)

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            df = pd.DataFrame(event.body, columns=self._columns)
            indices = [self._time_col]
            if self._labels_cols:
                if isinstance(self._labels_cols, list):
                    indices.extend(self._labels_cols)
                else:
                    indices.append(self._labels_cols)
            df.set_index(keys=indices, inplace=True)
            if not self._created and self._rate:
                self._created = True
                self._frames_client.create(
                    'tsdb', table=self._path, if_exists=frames.frames_pb2.IGNORE, rate=self._rate,
                    aggregates=self._aggr, aggregation_granularity=self.aggr_granularity)
            self._frames_client.write("tsdb", self._path, df)
