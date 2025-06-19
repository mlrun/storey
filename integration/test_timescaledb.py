import contextlib
import os
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import NamedTuple, Optional
from urllib.parse import urlparse

import pytest

from storey import AsyncEmitSource, SyncEmitSource, build_flow

# Skip entire module if DSN not provided
dsn = os.getenv("TIMESCALEDB_DSN")
if not dsn:
    pytest.skip("Missing TimescaleDB DSN", allow_module_level=True)


# Import dependencies only if we're not skipping
import psycopg2  # noqa: E402
import psycopg2.extensions  # noqa: E402
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT  # noqa: E402

from storey.timescaledb_target import TimescaleDBTarget  # noqa: E402


class TimescaleDBData(NamedTuple):
    connection: psycopg2.extensions.connection
    host: str
    port: str
    database: str
    user: str
    password: str
    table_name: str
    dsn_url: Optional[str]
    timestamp_precision: str
    columns_config: list


def _parse_dsn_for_connection_params(dsn_url: str) -> dict:

    parsed = urlparse(dsn_url)
    return {
        "host": parsed.hostname or "localhost",
        "port": str(parsed.port) if parsed.port else "5432",
        "database": parsed.path.lstrip("/") if parsed.path else "postgres",
        "user": parsed.username,
        "password": parsed.password,
    }


def _generate_test_data(precision: str, row_count: int = 5):
    """Generate test data with different data types"""
    test_data = []
    base_time = datetime(2019, 9, 18, 1, 55, 10, tzinfo=timezone.utc)

    for i in range(row_count):
        # Calculate time with different precisions
        if precision == "milliseconds":
            time_val = base_time.replace(microsecond=i * 1000)  # millisecond precision
            time_str = f"18/09/19 01:55:10.{i:03d} UTC+0000"
        elif precision == "microseconds":
            time_val = base_time.replace(microsecond=i)  # microsecond precision
            time_str = f"18/09/19 01:55:10.{i:06d} UTC+0000"
        else:
            time_val = base_time.replace(second=10 + i)
            time_str = f"18/09/19 01:55:{10+i:02d} UTC+0000"

        data = {
            "time": time_str,
            "binary_col": b"test_binary_" + str(i).encode(),
            "bool_col": i % 2 == 0,
            "double_col": 123.456789 + i,
            "float_col": float(12.34 + i),
            "int_col": i,
            "timestamp_col": time_val,
            "nchar_col": f"N{i:02d}",
            "varchar_col": f"varchar_test_{i}",
        }
        test_data.append(data)

    return test_data


# Remove the basic fixture since we're consolidating


@pytest.fixture(params=[("milliseconds"), ("microseconds")])
def timescaledb(request: "pytest.FixtureRequest") -> Iterator[TimescaleDBData]:
    """Fixture for extended type testing"""
    timestamp_precision = request.param
    test_type = "extended"
    table_name = f"test_table_{test_type}_{timestamp_precision}"

    # Setup database connection
    connection_params = _parse_dsn_for_connection_params(dsn)
    conn_host = connection_params["host"]
    conn_port = connection_params["port"]
    conn_database = connection_params["database"]
    conn_user = connection_params["user"]
    conn_password = connection_params["password"]
    connection = psycopg2.connect(dsn)

    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    test_connection = connection
    test_cursor = test_connection.cursor()

    # Create TimescaleDB extension
    test_cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

    # Drop and create extended table
    with contextlib.suppress(psycopg2.Error):
        test_cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    test_cursor.execute(
        f"""
        CREATE TABLE {table_name} (
            time TIMESTAMPTZ NOT NULL,
            binary_col BYTEA,
            bool_col BOOLEAN,
            double_col DOUBLE PRECISION,
            float_col REAL,
            int_col INTEGER,
            timestamp_col TIMESTAMPTZ,
            nchar_col CHAR(10),
            varchar_col VARCHAR(100)
        );
        """
    )

    test_cursor.execute(f"SELECT create_hypertable('{table_name}', 'time');")
    test_cursor.close()

    columns_config = [
        "binary_col",
        "bool_col",
        "double_col",
        "float_col",
        "int_col",
        "timestamp_col",
        "nchar_col",
        "varchar_col",
    ]

    yield (
        test_connection,
        conn_host,
        conn_port,
        conn_database,
        conn_user,
        conn_password,
        table_name,
        dsn,
        timestamp_precision,
        columns_config,
    )

    # Cleanup
    try:
        cleanup_cursor = test_connection.cursor()
        cleanup_cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        cleanup_cursor.close()
    except Exception as e:
        print(f"Cleanup error: {e}")
    test_connection.close()


def test_timescaledb_all_types_and_precision(timescaledb):
    """Comprehensive test for all data types with millisecond and microsecond precision"""
    (connection, host, port, db_name, user, password, table_name, dsn_url, timestamp_precision, columns_config) = (
        timescaledb
    )

    time_format = "%d/%m/%y %H:%M:%S UTC%z"
    if timestamp_precision in ["milliseconds", "microseconds"]:
        time_format = "%d/%m/%y %H:%M:%S.%f UTC%z"

    controller = build_flow(
        [
            SyncEmitSource(),
            TimescaleDBTarget(
                dsn=dsn_url,
                table=table_name,
                time_col="time",
                columns=columns_config,
                time_format=time_format,
                max_events=10,
            ),
        ]
    ).run()

    # Generate test data
    test_data = _generate_test_data(timestamp_precision, 3)

    for data in test_data:
        controller.emit(data, None)

    controller.terminate()
    controller.await_termination()

    # Verify results
    cursor = connection.cursor()
    query = f"""
        SELECT time, binary_col, bool_col, double_col, float_col,
               int_col, timestamp_col, nchar_col, varchar_col
        FROM {table_name} ORDER BY int_col;
    """
    cursor.execute(query)

    result_list = []
    result_list.extend(list(row) for row in cursor.fetchall())
    cursor.close()

    # Verify we got the expected number of rows
    assert len(result_list) == 3

    # Verify data types for each row
    for i, row in enumerate(result_list):
        assert isinstance(row[0], datetime)  # time (TIMESTAMPTZ)
        assert isinstance(row[1], (bytes, memoryview))  # binary_col (BYTEA)
        assert isinstance(row[2], bool)  # bool_col (BOOLEAN)
        assert isinstance(row[3], float)  # double_col (DOUBLE PRECISION)
        assert isinstance(row[4], float)  # float_col (REAL)
        assert isinstance(row[5], int)  # int_col (INTEGER)
        assert isinstance(row[6], datetime)  # timestamp_col (TIMESTAMPTZ)
        assert isinstance(row[7], str)  # nchar_col (CHAR)
        assert isinstance(row[8], str)  # varchar_col (VARCHAR)

        # Verify specific values
        assert row[5] == i  # int_col matches index
        assert row[2] == (i % 2 == 0)  # bool_col alternates
        assert f"varchar_test_{i}" in row[8]  # varchar contains expected pattern

    # Test precision-specific functionality within the same test
    cursor = connection.cursor()
    if timestamp_precision == "milliseconds":
        # Test millisecond precision
        test_time = "2019-09-18 01:55:10.123+00"
        cursor.execute(
            f"""
            INSERT INTO {table_name} (time, binary_col, bool_col, double_col, float_col,
                                    int_col, timestamp_col, nchar_col, varchar_col)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (test_time, b"test", True, 1.23, 4.56, 999, test_time, "MS", "millisecond_test"),
        )

        cursor.execute(f"SELECT EXTRACT(MILLISECONDS FROM time) FROM {table_name} WHERE int_col = 999")
        result = cursor.fetchone()
        assert result[0] >= 10000  # At least 10 seconds worth of milliseconds

    elif timestamp_precision == "microseconds":
        # Test microsecond precision
        test_time = "2019-09-18 01:55:10.123456+00"
        cursor.execute(
            f"""
            INSERT INTO {table_name} (time, binary_col, bool_col, double_col, float_col,
                                    int_col, timestamp_col, nchar_col, varchar_col)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (test_time, b"test", True, 1.23, 4.56, 999, test_time, "US", "microsecond_test"),
        )

        cursor.execute(f"SELECT EXTRACT(MICROSECONDS FROM time) FROM {table_name} WHERE int_col = 999")
        result = cursor.fetchone()
        assert result[0] >= 10000000  # At least 10 seconds worth of microseconds

    cursor.close()


@pytest.mark.asyncio
async def test_timescaledb_async_emit(timescaledb):
    """Test async emission to TimescaleDB"""
    (connection, host, port, db_name, user, password, table_name, dsn_url, timestamp_precision, columns_config) = (
        timescaledb
    )

    time_format = "%d/%m/%y %H:%M:%S UTC%z"
    if timestamp_precision in ["milliseconds", "microseconds"]:
        time_format = "%d/%m/%y %H:%M:%S.%f UTC%z"

    controller = build_flow(
        [
            AsyncEmitSource(),
            TimescaleDBTarget(
                dsn=dsn_url,
                table=table_name,
                time_col="time",
                columns=columns_config,
                time_format=time_format,
                max_events=10,
            ),
        ]
    ).run()

    test_data = _generate_test_data(timestamp_precision, 2)

    for data in test_data:
        await controller.emit(data, None)

    await controller.terminate()
    await controller.await_termination()

    # Verify data was inserted
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.close()

    assert count == 2, "Async emission should insert exactly 2 rows"
