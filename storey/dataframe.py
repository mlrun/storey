import pandas as pd

from .flow import _termination_obj, Flow


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
            event.body = df
            return await self._do_downstream(event)
