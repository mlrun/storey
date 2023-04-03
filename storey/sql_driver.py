# Copyright 2022 Iguazio
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

from typing import List, Union

import pandas as pd
import sqlalchemy as db

from storey.drivers import Driver


class SQLDriver(Driver):
    """
    SQL database connector.
    :param primary_key: the primary key of the table, format <str>.<str>... or [<str>, <str>,...]
    :param db_path: database url
    :param time_fields: list of all fields that are timestamps
    """

    def __init__(self, primary_key: Union[str, List[str]], db_path: str, time_fields: List[str] = None):
        self._db_path = db_path
        self._sql_connection = None
        self._primary_key = primary_key if isinstance(primary_key, list) else self._extract_list_of_keys(primary_key)
        self._time_fields = time_fields

    def _lazy_init(self):

        if not self._sql_connection:
            self._engine = db.create_engine(self._db_path)
            self._sql_connection = self._engine.connect()

    def _table(self, table_path):
        metadata = db.MetaData()

        return db.Table(
            table_path.split("/")[-1],
            metadata,
            autoload=True,
            autoload_with=self._engine,
        )

    async def _save_key(self, container, table_path, key, aggr_item, partitioned_by_key, additional_data):
        self._lazy_init()
        key = self._extract_list_of_keys(key)
        for i in range(len(self._primary_key)):
            additional_data[self._primary_key[i]] = key[i]
        table = self._table(table_path)
        df = pd.DataFrame(additional_data, index=[0])
        try:
            df.to_sql(table.name, con=self._sql_connection, if_exists="append", index=False)
        except db.exc.IntegrityError:
            self._update_by_key(key, additional_data, table)

    async def _load_aggregates_by_key(self, container, table_path, key):
        self._lazy_init()
        table = self._table(table_path)

        values = await self._get_all_fields(key, table)
        if not values:
            values = None
        return [None, values]

    async def _load_by_key(self, container, table_path, key, attributes):
        self._lazy_init()
        table = self._table(table_path)
        if attributes == "*":
            values = await self._get_all_fields(key, table)
        else:
            values = await self._get_specific_fields(key, table, attributes)
        return values

    async def close(self):
        if self._sql_connection:
            self._sql_connection.close()
            self._sql_connection = None

    async def _get_all_fields(self, key, table):
        where_clause = self._get_where_clause(key, table)
        query = f"SELECT * FROM {table} where {where_clause}"
        results = pd.read_sql(query, con=self._sql_connection, parse_dates=self._time_fields).to_dict(orient="records")

        return results[0]

    async def _get_specific_fields(self, key: str, table, attributes: List[str]):
        where_clause = self._get_where_clause(key, table)
        try:
            query = f"SELECT {','.join(attributes)} FROM {table} as {table.name} where {where_clause}"
            results = pd.read_sql(query, con=self._sql_connection, parse_dates=self._time_fields).to_dict(
                orient="records"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to get key '{key}'") from e

        return results[0]

    def supports_aggregations(self):
        return False

    def _get_where_clause(self, key, sql_table):
        where_clause = ""
        key = self._extract_list_of_keys(key)
        for i in range(len(self._primary_key)):
            if i != 0:
                where_clause += " and "
            if sql_table.columns[self._primary_key[i]].type.python_type == str:
                where_clause += (
                    rf'{sql_table.name}."{self._primary_key[i]}"="{key[i]}"'
                    if "mysql" not in sql_table.dialect_options
                    else rf'{sql_table.name}.{self._primary_key[i]}="{key[i]}"'
                )
            else:
                where_clause += (
                    f'{sql_table.name}."{self._primary_key[i]}"={key[i]}'
                    if "mysql" not in sql_table.dialect_options
                    else f"{sql_table.name}.{self._primary_key[i]}={key[i]}"
                )
        return where_clause

    def _update_by_key(self, key, data, table):
        where_clause = self._get_where_clause(key, table)
        quote = "" if "mysql" in table.dialect_options else '"'
        update_clause = " ,".join(
            [f'[{quote}{key}{quote}]="{value}"' for key, value in data.items() if key not in self._primary_key]
        )
        sql_statement = rf"UPDATE {table} as {table.name} SET {update_clause} where {where_clause}"
        self._sql_connection.execute(sql_statement)

    @staticmethod
    def _extract_list_of_keys(key):
        if isinstance(key, str):
            key = key.split(".")
        if isinstance(key, int):
            key = [key]
        return key
