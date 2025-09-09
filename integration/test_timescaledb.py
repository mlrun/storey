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
import psycopg  # noqa: E402
from psycopg import Error as PsycopgError  # noqa: E402

from storey.timescaledb_target import TimescaleDBTarget  # noqa: E402


class TimescaleDBData(NamedTuple):
    connection: psycopg.Connection
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
    connection = psycopg.connect(dsn, autocommit=True)
    test_connection = connection
    test_cursor = test_connection.cursor()

    # Create TimescaleDB extension
    test_cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

    # Drop and create extended table
    with contextlib.suppress(PsycopgError):
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


def test_timescaledb_schema_validation_missing_required_column(timescaledb):
    """Test that validation properly catches missing required (non-nullable) columns"""
    (connection, _, _, _, _, _, table_name, dsn_url, _, _) = timescaledb

    # Create a table with a non-nullable column
    cursor = connection.cursor()
    validation_table = f"{table_name}_validation"
    with contextlib.suppress(PsycopgError):
        cursor.execute(f"DROP TABLE IF EXISTS {validation_table};")

    cursor.execute(
        f"""
        CREATE TABLE {validation_table} (
            time TIMESTAMPTZ NOT NULL,
            required_col INTEGER NOT NULL,
            optional_col VARCHAR(50)
        );
    """
    )
    cursor.execute(f"SELECT create_hypertable('{validation_table}', 'time');")
    cursor.close()

    time_format = "%d/%m/%y %H:%M:%S UTC%z"
    controller = build_flow(
        [
            SyncEmitSource(),
            TimescaleDBTarget(
                dsn=dsn_url,
                table=validation_table,
                time_col="time",
                columns=["required_col", "optional_col"],
                time_format=time_format,
                max_events=10,
            ),
        ]
    ).run()

    # Test data missing the required column
    invalid_data = {
        "time": "18/09/19 01:55:10 UTC+0000",
        "optional_col": "test_value",
        # Missing required_col (non-nullable)
    }

    # Should raise ValueError for missing required column
    with pytest.raises(ValueError, match=r"Missing required non-nullable column 'required_col'"):
        controller.emit(invalid_data, None)
        controller.terminate()
        controller.await_termination()

    # Cleanup
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {validation_table} CASCADE;")
    cursor.close()


def test_timescaledb_schema_validation_missing_nullable_column(timescaledb):
    """Test that validation handles missing nullable columns correctly"""
    (connection, _, _, _, _, _, table_name, dsn_url, _, _) = timescaledb

    # Create a table with nullable and non-nullable columns
    cursor = connection.cursor()
    validation_table = f"{table_name}_nullable"
    with contextlib.suppress(PsycopgError):
        cursor.execute(f"DROP TABLE IF EXISTS {validation_table};")

    cursor.execute(
        f"""
        CREATE TABLE {validation_table} (
            time TIMESTAMPTZ NOT NULL,
            required_col INTEGER NOT NULL,
            nullable_col VARCHAR(50)
        );
    """
    )
    cursor.execute(f"SELECT create_hypertable('{validation_table}', 'time');")
    cursor.close()

    time_format = "%d/%m/%y %H:%M:%S UTC%z"
    controller = build_flow(
        [
            SyncEmitSource(),
            TimescaleDBTarget(
                dsn=dsn_url,
                table=validation_table,
                time_col="time",
                columns=["required_col", "nullable_col"],
                time_format=time_format,
                max_events=10,
            ),
        ]
    ).run()

    # Test data with missing nullable column - should work fine
    valid_data = {
        "time": "18/09/19 01:55:10 UTC+0000",
        "required_col": 42,
        # Missing nullable_col - should be set to None
    }

    controller.emit(valid_data, None)
    controller.terminate()
    controller.await_termination()

    # Verify data was inserted with NULL for missing nullable column
    cursor = connection.cursor()
    cursor.execute(f"SELECT required_col, nullable_col FROM {validation_table}")
    result = cursor.fetchone()
    assert result[0] == 42
    assert result[1] is None  # nullable_col should be None
    cursor.close()

    # Cleanup
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {validation_table} CASCADE;")
    cursor.close()


def test_timescaledb_schema_validation_table_not_found(timescaledb):
    """Test that schema validation raises appropriate error for non-existent table"""
    (_, _, _, _, _, _, _, dsn_url, _, _) = timescaledb

    non_existent_table = "non_existent_table_12345"

    controller = build_flow(
        [
            SyncEmitSource(),
            TimescaleDBTarget(
                dsn=dsn_url,
                table=non_existent_table,
                time_col="time",
                columns=["col1"],
                max_events=10,
            ),
        ]
    ).run()

    test_data = {"time": "2019-09-18 01:55:10+00:00", "col1": "test_value"}

    # Should raise ValueError for table not found
    with pytest.raises(ValueError, match=r"Table 'public\.non_existent_table_12345' not found or has no columns"):
        controller.emit(test_data, None)
        controller.terminate()
        controller.await_termination()


def test_timescaledb_schema_validation_with_schema_prefix(timescaledb):
    """Test schema validation works correctly with schema.table format"""
    (connection, _, _, _, _, _, table_name, dsn_url, _, _) = timescaledb

    # Create a schema and table
    cursor = connection.cursor()
    schema_name = "test_schema"
    with contextlib.suppress(PsycopgError):
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    schema_table = f"{schema_name}.{table_name}_schema"
    with contextlib.suppress(PsycopgError):
        cursor.execute(f"DROP TABLE IF EXISTS {schema_table};")

    cursor.execute(
        f"""
        CREATE TABLE {schema_table} (
            time TIMESTAMPTZ NOT NULL,
            required_col INTEGER NOT NULL,
            optional_col VARCHAR(50)
        );
    """
    )
    cursor.execute(f"SELECT create_hypertable('{schema_table}', 'time');")
    cursor.close()

    time_format = "%d/%m/%y %H:%M:%S UTC%z"
    controller = build_flow(
        [
            SyncEmitSource(),
            TimescaleDBTarget(
                dsn=dsn_url,
                table=schema_table,  # Using schema.table format
                time_col="time",
                columns=["required_col", "optional_col"],
                time_format=time_format,
                max_events=10,
            ),
        ]
    ).run()

    # Test with missing required column
    invalid_data = {
        "time": "18/09/19 01:55:10 UTC+0000",
        "optional_col": "test_value",
        # Missing required_col
    }

    # Should raise ValueError for missing required column, showing correct schema.table format
    with pytest.raises(ValueError, match=r"Missing required non-nullable column 'required_col'"):
        controller.emit(invalid_data, None)
        controller.terminate()
        controller.await_termination()

    # Cleanup
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {schema_table} CASCADE;")
    cursor.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
    cursor.close()


@pytest.mark.asyncio
async def test_timescaledb_schema_caching(timescaledb):
    """Test that schema information is properly cached to avoid repeated queries"""
    (_, _, _, _, _, _, table_name, dsn_url, timestamp_precision, columns_config) = timescaledb

    time_formats = {"milliseconds": "%d/%m/%y %H:%M:%S.%f UTC%z", "microseconds": "%d/%m/%y %H:%M:%S.%f UTC%z"}
    time_format = time_formats.get(timestamp_precision, "%d/%m/%y %H:%M:%S UTC%z")

    target = TimescaleDBTarget(
        dsn=dsn_url,
        table=table_name,
        time_col="time",
        columns=columns_config,
        time_format=time_format,
        max_events=10,
    )

    # Initialize the target
    await target._async_init()

    # First call to get schema should query the database
    schema1 = await target._get_table_schema()
    assert isinstance(schema1, dict)
    assert len(schema1) > 0

    # Second call should return cached result (same object)
    schema2 = await target._get_table_schema()
    assert schema1 is schema2  # Should be the same object (cached)

    # Verify schema contains expected information
    assert "time" in schema1
    assert "binary_col" in schema1
    assert schema1["time"]["nullable"] is False  # time column is NOT NULL
    assert schema1["binary_col"]["nullable"] is True  # binary_col allows NULL

    # Cleanup
    await target._terminate()


def test_timescaledb_validation_with_extra_columns(timescaledb):
    """Test that validation works when event has columns not in schema"""
    (connection, _, _, _, _, _, table_name, dsn_url, _, _) = timescaledb

    time_format = "%d/%m/%y %H:%M:%S UTC%z"

    # Test with extra columns that aren't in the target column configuration
    # This tests the fallback behavior for columns not in schema
    controller = build_flow(
        [
            SyncEmitSource(),
            TimescaleDBTarget(
                dsn=dsn_url,
                table=table_name,
                time_col="time",
                columns=["binary_col", "int_col"],  # Only specify columns that exist
                time_format=time_format,
                max_events=10,
            ),
        ]
    ).run()

    # Test data with extra columns in the event data (should be ignored)
    test_data = {
        "time": "18/09/19 01:55:10 UTC+0000",
        "binary_col": b"test_binary",
        "int_col": 42,
        "extra_column_not_in_config": "this_column_is_extra",
        "another_extra_column": "also_extra",
    }

    # Should work fine - extra columns in event data are ignored
    controller.emit(test_data, None)
    controller.terminate()
    controller.await_termination()

    # Verify data was inserted correctly for configured columns
    cursor = connection.cursor()
    cursor.execute(f"SELECT binary_col, int_col FROM {table_name} ORDER BY int_col DESC LIMIT 1")
    result = cursor.fetchone()
    assert result[1] == 42  # int_col
    # PostgreSQL returns memoryview for binary data, convert to bytes for comparison
    binary_result = bytes(result[0]) if isinstance(result[0], memoryview) else result[0]
    assert binary_result == b"test_binary"  # binary_col
    cursor.close()


def test_timescaledb_validation_non_dict_data_type_error(timescaledb):
    """Test that validation properly rejects non-dictionary data types"""
    (_, _, _, _, _, _, table_name, dsn_url, _, columns_config) = timescaledb

    time_format = "%d/%m/%y %H:%M:%S UTC%z"
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

    # Test with non-dictionary data
    invalid_data = "this_is_not_a_dictionary"

    # Should raise TypeError for non-dictionary data (the error occurs in the parent Writer class)
    with pytest.raises(TypeError, match=r"string indices must be integers"):
        controller.emit(invalid_data, None)
        controller.terminate()
        controller.await_termination()


@pytest.mark.asyncio
async def test_timescaledb_validation_direct_emit_non_dict_error(timescaledb):
    """Test that TimescaleDBTarget._emit properly validates dictionary data types"""
    (_, _, _, _, _, _, table_name, dsn_url, timestamp_precision, columns_config) = timescaledb

    time_formats = {"milliseconds": "%d/%m/%y %H:%M:%S.%f UTC%z", "microseconds": "%d/%m/%y %H:%M:%S.%f UTC%z"}
    time_format = time_formats.get(timestamp_precision, "%d/%m/%y %H:%M:%S UTC%z")

    target = TimescaleDBTarget(
        dsn=dsn_url,
        table=table_name,
        time_col="time",
        columns=columns_config,
        time_format=time_format,
        max_events=10,
    )

    # Initialize the target
    await target._async_init()

    # Test with non-dictionary data directly in _emit
    non_dict_batch = ["this_is_not_a_dictionary", 123, None]

    # Should raise TypeError for non-dictionary data in our validation code
    with pytest.raises(TypeError, match=r"TimescaleDBTarget only supports dictionary data, got <class 'str'>"):
        await target._emit(non_dict_batch, None, None, None)

    # Cleanup
    await target._terminate()
