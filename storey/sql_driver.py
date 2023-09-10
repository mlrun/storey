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
        import sqlalchemy as db

        if not self._sql_connection:
            self._engine = db.create_engine(self._db_path)
            self._sql_connection = self._engine.connect()

    def _table(self, table_path):
        import sqlalchemy as db

        metadata = db.MetaData()

        return db.Table(
            table_path.split("/")[-1],
            metadata,
            autoload=True,
            autoload_with=self._engine,
        )

    async def _save_key(self, container, table_path, key, aggr_item, partitioned_by_key, additional_data):
        import sqlalchemy as db

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
        import sqlalchemy as db

        key = self._extract_list_of_keys(key)
        select_object = db.select(table).where(
            db.and_(getattr(table.c, self._primary_key[i]) == key[i] for i in range(len(self._primary_key)))
        )
        results = pd.read_sql(select_object, con=self._sql_connection, parse_dates=self._time_fields).to_dict(
            orient="records"
        )

        return results[0]

    async def _get_specific_fields(self, key: str, table, attributes: List[str]):
        import sqlalchemy as db

        key = self._extract_list_of_keys(key)
        try:
            select_object = db.select(*[getattr(table.c, atr) for atr in attributes]).where(
                db.and_(getattr(table.c, self._primary_key[i]) == key[i] for i in range(len(self._primary_key)))
            )
            results = pd.read_sql(select_object, con=self._sql_connection, parse_dates=self._time_fields).to_dict(
                orient="records"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to get key '{key}'") from e

        return results[0]

    def supports_aggregations(self):
        return False

    def _update_by_key(self, key, data, sql_table):
        import sqlalchemy as db

        self._sql_connection.execute(
            db.update(sql_table)
            .values({getattr(sql_table.c, k): v for k, v in data.items() if k not in self._primary_key})
            .where(db.and_(getattr(sql_table.c, self._primary_key[i]) == key[i] for i in range(len(self._primary_key))))
        )

    @staticmethod
    def _extract_list_of_keys(key):
        if isinstance(key, str):
            key = key.split(".")
        elif not isinstance(key, list):
            key = [key]
        return key
