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
    table_name = "test"

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
        connection.execute(f"CREATE DATABASE {db_name};")
    except (ConnectError, QueryError) as err:  # websocket connection raises QueryError
        if "Database already exists" not in str(err):
            raise err

    if not db_prefix:
        connection.execute(f"USE {db_name}")

    try:
        connection.execute(f"DROP TABLE {db_prefix}{table_name};")
    except (ConnectError, QueryError) as err:  # websocket connection raises QueryError
        if "Table does not exist" not in str(err):
            raise err

    connection.execute(f"CREATE TABLE {db_prefix}{table_name} (time TIMESTAMP, my_int INT, my_string NCHAR(10));")

    # Test runs
    yield connection, url, user, password, db_name, table_name, db_prefix

    # Teardown
    connection.execute(f"DROP TABLE {db_prefix}{table_name};")
    connection.close()


@pytest.mark.parametrize("dynamic_table", [None, "$key", "table"])
@pytest.mark.skipif(not has_tdengine_credentials, reason="Missing TDEngine URL, user, and/or password")
def test_tdengine_target(tdengine, dynamic_table):
    connection, url, user, password, db_name, table_name, db_prefix = tdengine
    time_format = "%d/%m/%y %H:%M:%S UTC%z"
    controller = build_flow(
        [
            SyncEmitSource(),
            TDEngineTarget(
                url=url,
                user=user,
                password=password,
                database=db_name,
                table=None if dynamic_table else table_name,
                dynamic_table=dynamic_table,
                time_col="time",
                columns=["my_int", "my_string"],
                time_format=time_format,
                max_events=2,
            ),
        ]
    ).run()

    date_time_str = "18/09/19 01:55:1"
    for i in range(9):
        timestamp = f"{date_time_str}{i} UTC-0000"
        event_body = {"time": timestamp, "my_int": i, "my_string": f"hello{i}"}
        event_key = None
        if dynamic_table == "$key":
            event_key = table_name
        elif dynamic_table:
            event_body[dynamic_table] = table_name
        controller.emit(event_body, event_key)

    controller.terminate()
    controller.await_termination()

    result = connection.query(f"SELECT * FROM {db_prefix}{table_name};")
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
    assert result_list == [
        [datetime(2019, 9, 18, 1, 55, 10), 0, "hello0"],
        [datetime(2019, 9, 18, 1, 55, 11), 1, "hello1"],
        [datetime(2019, 9, 18, 1, 55, 12), 2, "hello2"],
        [datetime(2019, 9, 18, 1, 55, 13), 3, "hello3"],
        [datetime(2019, 9, 18, 1, 55, 14), 4, "hello4"],
        [datetime(2019, 9, 18, 1, 55, 15), 5, "hello5"],
        [datetime(2019, 9, 18, 1, 55, 16), 6, "hello6"],
        [datetime(2019, 9, 18, 1, 55, 17), 7, "hello7"],
        [datetime(2019, 9, 18, 1, 55, 18), 8, "hello8"],
    ]
