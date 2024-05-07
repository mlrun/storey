import os
from datetime import datetime

import pytest
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


@pytest.mark.skipif(not has_tdengine_credentials, reason="Missing TDEngine URL, user, and/or password")
def test_tdengine_target(tdengine):
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
                table=table_name,
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
        controller.emit({"time": timestamp, "my_int": i, "my_string": f"hello{i}"})

    controller.terminate()
    controller.await_termination()

    result = connection.query(f"SELECT * FROM {db_prefix}{table_name};")
    if url.startswith("taosws"):
        result_list = []
        for row in result:
            row = list(row)
            for field_index, field in enumerate(result.fields):
                if field.type() == "TIMESTAMP":
                    t = datetime.fromisoformat(row[field_index])
                    # REST API returns a naive timestamp, but websocket returns a timestamp with a time zone
                    t = t.replace(tzinfo=None)
                    row[field_index] = t
            result_list.append(row)
    else:
        result_list = result.data
    assert result_list == [
        [datetime(2019, 9, 18, 9, 55, 10), 0, "hello0"],
        [datetime(2019, 9, 18, 9, 55, 11), 1, "hello1"],
        [datetime(2019, 9, 18, 9, 55, 12), 2, "hello2"],
        [datetime(2019, 9, 18, 9, 55, 13), 3, "hello3"],
        [datetime(2019, 9, 18, 9, 55, 14), 4, "hello4"],
        [datetime(2019, 9, 18, 9, 55, 15), 5, "hello5"],
        [datetime(2019, 9, 18, 9, 55, 16), 6, "hello6"],
        [datetime(2019, 9, 18, 9, 55, 17), 7, "hello7"],
        [datetime(2019, 9, 18, 9, 55, 18), 8, "hello8"],
    ]
