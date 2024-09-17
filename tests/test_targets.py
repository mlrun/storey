# Copyright 2024 Iguazio
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

from typing import Optional
from unittest.mock import Mock

import pytest
import taosws

from storey.dtypes import TDEngineValueError
from storey.targets import TDEngineTarget


class TestTDEngineTarget:
    @staticmethod
    def test_tags_mapping_consistency() -> None:
        for type_, func in TDEngineTarget._get_tdengine_type_to_tag_func().items():
            assert func.__name__ == f"{type_.lower()}_to_tag"

    @staticmethod
    def test_columns_mapping_consistency() -> None:
        for type_, func in TDEngineTarget._get_tdengine_type_to_column_func().items():
            if type_ == "TIMESTAMP":
                assert func.__name__.startswith("millis_timestamp")
            else:
                assert func.__name__.startswith(type_.lower())
            assert func.__name__.endswith("_to_column")

    @staticmethod
    @pytest.mark.parametrize(
        ("database", "table", "supertable", "table_col", "tag_cols"),
        [
            (None, None, "my_super_tb", "pass_this_check", ["also_this_one"]),
            ("mydb", None, "my super  tb", "pass_this_check", ["also_this_one"]),
            ("_db", "9table", None, None, None),
            ("_db", " cars", None, None, None),
        ],
    )
    def test_invalid_names(
        database: Optional[str],
        table: Optional[str],
        supertable: Optional[str],
        table_col: Optional[str],
        tag_cols: Optional[list[str]],
    ) -> None:
        with pytest.raises(TDEngineValueError):
            TDEngineTarget(
                url="taosws://root:taosdata@localhost:6041",
                time_col="ts",
                columns=["value"],
                table_col=table_col,
                tag_cols=tag_cols,
                database=database,
                table=table,
                supertable=supertable,
            )

    @staticmethod
    @pytest.fixture
    def tdengine_target() -> TDEngineTarget:
        target = TDEngineTarget(
            url="taosws://root:taosdata@localhost:6041",
            time_col="ts",
            columns=["value"],
            database="test",
            table="d6241",
        )

        target._connection = Mock()
        # The following test schema is obtained from the `taosBenchmark` data:
        # https://docs.tdengine.com/get-started/docker/#test-data-insert-performance
        # list(conn.query("describe test.d6241;"))
        target._connection.query = Mock(
            return_value=[
                ("ts", "TIMESTAMP", 8, "", "delta-i", "lz4", "medium"),
                ("current", "FLOAT", 4, "", "delta-d", "lz4", "medium"),
                ("voltage", "INT", 4, "", "simple8b", "lz4", "medium"),
                ("phase", "FLOAT", 4, "", "delta-d", "lz4", "medium"),
                ("groupid", "INT", 4, "TAG", "disabled", "disabled", "disabled"),
                ("location", "VARCHAR", 24, "TAG", "disabled", "disabled", "disabled"),
            ],
        )
        return target

    @staticmethod
    def test_get_table_schema(tdengine_target: TDEngineTarget) -> None:
        """Test that the parsing works"""
        tags_schema, reg_cols_schema = tdengine_target._get_table_schema("d6241")
        assert tags_schema == [("groupid", taosws.int_to_tag), ("location", taosws.varchar_to_tag)]
        assert reg_cols_schema == [
            ("ts", taosws.millis_timestamps_to_column),
            ("current", taosws.floats_to_column),
            ("voltage", taosws.ints_to_column),
            ("phase", taosws.floats_to_column),
        ]
