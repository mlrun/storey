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
import pandas as pd
from typing import Optional, List

from .flow import _termination_obj, Flow


class ReduceToDataFrame(Flow):
    """Builds a pandas DataFrame from events and returns that DataFrame on flow termination.

    :param index: Name of the column to be used as index. Optional. If not set, DataFrame will be range indexed.
    :param columns: List of column names to be passed as-is to the DataFrame constructor. Optional.
    :param insert_key_column_as: Name of the column to be inserted for event keys. Optional.
        If not set, event keys will not be inserted into the DataFrame.
    :param insert_time_column_as: Name of the column to be inserted for event times. Optional.
        If not set, event times will not be inserted into the DataFrame.
    :param insert_id_column_as: Name of the column to be inserted for event IDs. Optional.
        If not set, event IDs will not be inserted into the DataFrame.

    for additional params, see documentation of  :class:`storey.flow.Flow`

    """

    def __init__(self, index: Optional[str] = None, columns: Optional[List[str]] = None, insert_key_column_as: Optional[str] = None,
                 insert_time_column_as: Optional[str] = None, insert_id_column_as: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self._index = index
        self._columns = columns
        self._insert_key_column_as = insert_key_column_as
        self._insert_time_column_as = insert_time_column_as
        self._insert_id_column_as = insert_id_column_as

    def _init(self):
        super()._init()
        self._key_column = []
        self._time_column = []
        self._id_column = []
        self._data = []

    def to(self, outlet):
        """Pipe this step to next one. Throws exception since illegal"""
        raise ValueError("ToDataFrame is a terminal step. It cannot be piped further.")

    async def _do(self, event):
        if event is _termination_obj:
            df = pd.DataFrame(self._data, columns=self._columns)
            if not df.empty:
                if self._insert_key_column_as:
                    df[self._insert_key_column_as] = pd.DataFrame(self._key_column)
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
    """Create pandas data frame from events. Can appear in the middle of the flow, as opposed to ReduceToDataFrame

    :param index: Name of the column to be used as index. Optional. If not set, DataFrame will be range indexed.
    :param columns: List of column names to be passed as-is to the DataFrame constructor. Optional.

    for additional params, see documentation of  :class:`storey.flow.Flow`
    """
    def __init__(self, index: Optional[str] = None, columns: Optional[List[str]] = None, **kwargs):
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
