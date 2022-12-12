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

from typing import List

import sqlalchemy as db

from storey.drivers import Driver


class SQLDriver(Driver):
    """
    SQL database connector.
    :param db_url: database url
    :param primary_key: the primary key of the table.
    """

    def __init__(
        self,
        primary_key: str,
        db_path: str,
    ):
        self._db_path = db_path
        self._sql_connection = None
        self._primary_key = primary_key

    def _lazy_init(self):

        if not self._sql_connection:
            self._engine = db.create_engine(self._db_path)
            self._sql_connection = self._engine.connect()

    def _table(self, table_path):
        metadata = db.MetaData()
        while table_path.startswith("/"):
            table_path = table_path[1:]

        return db.Table(
            table_path.split("/")[1],
            metadata,
            autoload=True,
            autoload_with=self._engine,
        )

    async def _save_schema(self, container, table_path, schema):
        self._lazy_init()

    async def _load_schema(self, container, table_path):
        self._lazy_init()

    async def _save_key(self, container, table_path, key, aggr_item, partitioned_by_key, additional_data):
        self._lazy_init()

        table = self._table(table_path)
        self._sql_connection.execute(table.insert(), [additional_data], autocommit=True)

        await self.close()

    async def _load_aggregates_by_key(self, container, table_path, key):
        self._lazy_init()
        table = self._table(table_path)

        agg_val, values = await self._get_all_fields(key, table)
        if not agg_val:
            agg_val = None
        if not values:
            values = None
        return [agg_val, values]

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
        where_clause = self._get_where_clause(key)
        try:
            my_query = f"SELECT * FROM {table} where {where_clause}"
            results = self._sql_connection.execute(my_query).fetchall()
        except Exception as e:
            raise RuntimeError(f"Failed to get key {key}. Response error was: {e}")

        return results[0]._mapping

    async def _get_specific_fields(self, key: str, table, attributes: List[str]):
        where_clause = self._get_where_clause(key)
        try:
            my_query = f"SELECT {','.join(attributes)} FROM {table} where {where_clause}"
            results = self._sql_connection.execute(my_query).fetchall()
        except Exception as e:
            raise RuntimeError(f"Failed to get key {key}. Response error was: {e}")

        return results[0]._mapping

    def supports_aggregations(self):
        return False

    def _get_where_clause(self, key):
        where_clause = ""
        if isinstance(key, str):
            key = key.split(".")
        if isinstance(key, List):
            for i in range(len(self._primary_key)):
                if i != 0:
                    where_clause += " and "
                where_clause += f'{self._primary_key[i]}="{key[i]}"'
        return where_clause
