import os
from datetime import datetime

import pytest
import pytz
import taosrest
from taosrest import ConnectError
from taosws import QueryError

from storey import SyncEmitSource, build_flow
from storey.targets import TDEngineTarget

url = os.getenv("TDENGINE_URL")
user = os.getenv("TDENGINE_USER")
password = os.getenv("TDENGINE_PASSWORD")
has_tdengine_credentials = all([url, user, password]) or (url and url.startswith("taosws"))


@pytest.fixture()
def tdengine():
    db_name = "storey"
    supertable_name = "test_supertable"

    # Setup
    if url.startswith("taosws"):
        import taosws

        connection = taosws.connect(url)
        db_prefix = ""
    else:
        db_prefix = db_name + "."
        connection = taosrest.connect(
            url=url,
            user=user,
            password=password,
            timeout=30,
        )

    try:
        connection.execute(f"DROP DATABASE {db_name};")
    except (ConnectError, QueryError) as err:  # websocket connection raises QueryError
        if "Database not exist" not in str(err):
            raise err

    connection.execute(f"CREATE DATABASE {db_name};")

    if not db_prefix:
        connection.execute(f"USE {db_name}")

    try:
        connection.execute(f"DROP STABLE {db_prefix}{supertable_name};")
    except (ConnectError, QueryError) as err:  # websocket connection raises QueryError
        if "STable not exist" not in str(err):
            raise err

    connection.execute(
        f"CREATE STABLE {db_prefix}{supertable_name} (time TIMESTAMP, my_string NCHAR(10)) TAGS (my_int INT);"
    )

    # Test runs
    yield connection, url, user, password, db_name, supertable_name, db_prefix

    # Teardown
    connection.execute(f"DROP DATABASE {db_name};")
    connection.close()


@pytest.mark.parametrize("table_col", [None, "$key", "table"])
@pytest.mark.skipif(not has_tdengine_credentials, reason="Missing TDEngine URL, user, and/or password")
def test_tdengine_target(tdengine, table_col):
    connection, url, user, password, db_name, supertable_name, db_prefix = tdengine
    time_format = "%d/%m/%y %H:%M:%S UTC%z"

    table_name = "test_table"

    # Table is created automatically only when using a supertable
    if not table_col:
        connection.execute(f"CREATE TABLE {db_prefix}{table_name} (time TIMESTAMP, my_string NCHAR(10), my_int INT);")

    controller = build_flow(
        [
            SyncEmitSource(),
            TDEngineTarget(
                url=url,
                time_col="time",
                columns=["my_string"] if table_col else ["my_string", "my_int"],
                user=user,
                password=password,
                database=db_name,
                table=None if table_col else table_name,
                table_col=table_col,
                supertable=supertable_name if table_col else None,
                tag_cols=["my_int"] if table_col else None,
                time_format=time_format,
                max_events=10,
            ),
        ]
    ).run()

    date_time_str = "18/09/19 01:55:1"
    for i in range(5):
        timestamp = f"{date_time_str}{i} UTC-0000"
        event_body = {"time": timestamp, "my_int": i, "my_string": f"hello{i}"}
        event_key = None
        subtable_name = f"{table_name}{i}"
        if table_col == "$key":
            event_key = subtable_name
        elif table_col:
            event_body[table_col] = subtable_name
        controller.emit(event_body, event_key)

    controller.terminate()
    controller.await_termination()

    if table_col:
        query_table = supertable_name
        where_clause = " WHERE my_int > 0 AND my_int < 3"
    else:
        query_table = table_name
        where_clause = ""
    result = connection.query(f"SELECT * FROM {db_prefix}{query_table} {where_clause} ORDER BY my_int;")
    result_list = []
    for row in result:
        row = list(row)
        for field_index, field in enumerate(result.fields):
            typ = field.type() if url.startswith("taosws") else field["type"]
            if typ == "TIMESTAMP":
                if url.startswith("taosws"):
                    t = datetime.fromisoformat(row[field_index])
                    # websocket returns a timestamp with the local time zone
                    t = t.astimezone(pytz.UTC).replace(tzinfo=None)
                    row[field_index] = t
                else:
                    t = row[field_index]
                    # REST API returns a naive timestamp matching the local time zone
                    t = t.astimezone(pytz.UTC).replace(tzinfo=None)
                    row[field_index] = t
        result_list.append(row)
    if table_col:
        expected_result = [
            [datetime(2019, 9, 18, 1, 55, 11), "hello1", 1],
            [datetime(2019, 9, 18, 1, 55, 12), "hello2", 2],
        ]
    else:
        expected_result = [
            [datetime(2019, 9, 18, 1, 55, 10), "hello0", 0],
            [datetime(2019, 9, 18, 1, 55, 11), "hello1", 1],
            [datetime(2019, 9, 18, 1, 55, 12), "hello2", 2],
            [datetime(2019, 9, 18, 1, 55, 13), "hello3", 3],
            [datetime(2019, 9, 18, 1, 55, 14), "hello4", 4],
        ]
    assert result_list == expected_result
