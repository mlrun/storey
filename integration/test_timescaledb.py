import os
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import NamedTuple
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from storey import AsyncEmitSource, SyncEmitSource, build_flow

# Skip entire module if DSN not provided
dsn = os.getenv("TIMESCALEDB_DSN")
if not dsn:
    pytest.skip("Missing TimescaleDB DSN", allow_module_level=True)


# Import dependencies only if we're not skipping
import psycopg  # noqa: E402

from storey.timescaledb_target import TimescaleDBTarget  # noqa: E402


class TimescaleDBData(NamedTuple):
    table_name: str
    dsn_url: str
    timestamp_precision: str
    columns_config: list


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


@pytest.fixture(scope="function")
def table_cleanup():
    """Simple fixture to track and cleanup tables after each test"""
    tables = []

    class TableCleanup:
        def add_table(self, table_name: str):
            tables.append(table_name)

    cleanup = TableCleanup()

    try:
        yield cleanup
    finally:
        if tables:
            with psycopg.connect(dsn, autocommit=True) as connection:
                with connection.cursor() as cursor:
                    for table_name in tables:
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")


def _create_timescaledb_table_and_data(table_name: str, timestamp_precision: str, table_cleanup) -> TimescaleDBData:
    """Helper function to create TimescaleDB table and return fixture data."""
    table_cleanup.add_table(table_name)

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cursor:
            # Create TimescaleDB extension if needed
            cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

            # Drop and create table
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            cursor.execute(
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
                    varchar_col VARCHAR(255),
                    text_col TEXT
                );
                """
            )

            cursor.execute(f"SELECT create_hypertable('{table_name}', 'time');")

    columns_config = [
        "binary_col",
        "bool_col",
        "double_col",
        "float_col",
        "int_col",
        "timestamp_col",
        "nchar_col",
        "varchar_col",
        "text_col",
    ]

    return TimescaleDBData(
        table_name,
        dsn,
        timestamp_precision,
        columns_config,
    )


@pytest.fixture(params=[("milliseconds"), ("microseconds")])
def timescaledb_multiple_precision(request: "pytest.FixtureRequest", table_cleanup) -> Iterator[TimescaleDBData]:
    """Fixture for tests that need both millisecond and microsecond precision testing."""
    timestamp_precision = request.param
    table_name = f"test_table_extended_{timestamp_precision}"

    yield _create_timescaledb_table_and_data(table_name, timestamp_precision, table_cleanup)


@pytest.fixture(scope="function")
def timescaledb(table_cleanup) -> TimescaleDBData:
    """TimescaleDB fixture for tests that don't need timestamp precision variations."""
    table_name = f"test_table_{uuid4().hex[:8]}"
    timestamp_precision = "milliseconds"  # Default precision for single tests

    return _create_timescaledb_table_and_data(table_name, timestamp_precision, table_cleanup)


def test_timescaledb_all_types_and_precision(timescaledb_multiple_precision):
    """Comprehensive test for all data types with millisecond and microsecond precision"""
    (table_name, dsn_url, timestamp_precision, columns_config) = timescaledb_multiple_precision

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
        controller.emit(data)

    controller.terminate()
    controller.await_termination()

    # Verify results
    with psycopg.connect(dsn_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
            query = f"""
                SELECT time, binary_col, bool_col, double_col, float_col,
                       int_col, timestamp_col, nchar_col, varchar_col
                FROM {table_name} ORDER BY int_col;
            """
            cursor.execute(query)

            result_list = []
            result_list.extend(list(row) for row in cursor.fetchall())

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
    with psycopg.connect(dsn_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
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


@pytest.mark.asyncio
async def test_timescaledb_async_emit(timescaledb_multiple_precision):
    """Test async emission to TimescaleDB"""
    (table_name, dsn_url, timestamp_precision, columns_config) = timescaledb_multiple_precision

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
        await controller.emit(data)

    await controller.terminate()
    await controller.await_termination()

    # Verify data was inserted
    with psycopg.connect(dsn_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            assert count == 2, "Async emission should insert exactly 2 rows"


@pytest.mark.parametrize("nullable", [True, False])
def test_timescaledb_schema_validation_missing_column(timescaledb, nullable, table_cleanup):
    """Test that validation properly handles missing columns based on nullability"""
    (table_name, dsn_url, _, _) = timescaledb

    # Create a table with nullable and non-nullable columns
    validation_table = f"{table_name}_validation_{'nullable' if nullable else 'required'}"

    # Register table for cleanup
    table_cleanup.add_table(validation_table)

    with psycopg.connect(dsn_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {validation_table};")

            # Create table with different column nullability based on parameter
            test_col_nullable = "NULL" if nullable else "NOT NULL"
            cursor.execute(
                f"""
                CREATE TABLE {validation_table} (
                    time TIMESTAMPTZ NOT NULL,
                    required_col INTEGER NOT NULL,
                    test_col VARCHAR(50) {test_col_nullable}
                );
            """
            )
            cursor.execute(f"SELECT create_hypertable('{validation_table}', 'time');")

    time_format = "%d/%m/%y %H:%M:%S UTC%z"
    controller = build_flow(
        [
            SyncEmitSource(),
            TimescaleDBTarget(
                dsn=dsn_url,
                table=validation_table,
                time_col="time",
                columns=["required_col", "test_col"],
                time_format=time_format,
                max_events=10,
            ),
        ]
    ).run()

    # Test data missing the test column
    test_data = {
        "time": "18/09/19 01:55:10 UTC+0000",
        "required_col": 42,
        # Missing test_col
    }

    if nullable:
        # Should work fine for nullable columns - missing column gets NULL
        controller.emit(test_data)
        controller.terminate()
        controller.await_termination()

        # Verify data was inserted with NULL for missing nullable column
        with psycopg.connect(dsn_url, autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT required_col, test_col FROM {validation_table}")
                result = cursor.fetchone()
                assert result[0] == 42
                assert result[1] is None  # test_col should be None
    else:
        # Should raise ValueError for missing required (non-nullable) column
        with pytest.raises(ValueError, match=r"Missing required non-nullable column 'test_col'"):
            controller.emit(test_data)
            controller.terminate()
            controller.await_termination()


def test_timescaledb_schema_validation_table_not_found(timescaledb):
    """Test that schema validation raises appropriate error for non-existent table"""
    (table_name, dsn_url, _, _) = timescaledb

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
    with pytest.raises(ValueError, match=r"Table 'public\.non_existent_table_12345' does not exist"):
        controller.emit(test_data)
        controller.terminate()
        controller.await_termination()


def test_timescaledb_schema_validation_with_schema_prefix(timescaledb, table_cleanup):
    """Test schema validation works correctly with schema.table format"""
    (table_name, dsn_url, _, _) = timescaledb

    # Create a schema and table
    schema_name = "test_schema"
    schema_table = f"{schema_name}.{table_name}_schema"

    # Register table for cleanup (schema is persistent)
    table_cleanup.add_table(schema_table)

    with psycopg.connect(dsn_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
            # Create TimescaleDB extension if needed
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
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
        controller.emit(invalid_data)
        controller.terminate()
        controller.await_termination()


def test_timescaledb_schema_caching(timescaledb):
    """Test that schema information is properly cached to avoid repeated queries"""
    (table_name, dsn_url, timestamp_precision, columns_config) = timescaledb

    time_formats = {"milliseconds": "%d/%m/%y %H:%M:%S.%f UTC%z", "microseconds": "%d/%m/%y %H:%M:%S.%f UTC%z"}
    time_format = time_formats.get(timestamp_precision, "%d/%m/%y %H:%M:%S UTC%z")

    # Create a flow with the target that will trigger schema caching
    controller = build_flow(
        [
            SyncEmitSource(),
            TimescaleDBTarget(
                dsn=dsn_url,
                table=table_name,
                time_col="time",
                columns=columns_config,
                time_format=time_format,
                max_events=1,  # Force immediate writes to trigger schema lookups
            ),
        ]
    ).run()

    # Emit multiple events to trigger repeated schema lookups
    # The first event will query the database for schema
    # Subsequent events should use cached schema
    if timestamp_precision in ["milliseconds", "microseconds"]:
        time_str = (
            "18/09/19 01:55:10.123000 UTC+0000"
            if timestamp_precision == "microseconds"
            else "18/09/19 01:55:10.123 UTC+0000"
        )
    else:
        time_str = "18/09/19 01:55:10 UTC+0000"

    # Emit first event - this will trigger initial schema query
    controller.emit({"time": time_str, "binary_col": b"test_data1", "int_col": 42, "text_col": "first"})

    # Emit second event - this should use cached schema
    controller.emit({"time": time_str, "binary_col": b"test_data2", "int_col": 43, "text_col": "second"})

    # Emit third event - this should also use cached schema
    controller.emit({"time": time_str, "binary_col": b"test_data3", "int_col": 44, "text_col": "third"})

    controller.terminate()
    controller.await_termination()

    # Verify data was written correctly (which indirectly tests that schema caching worked)
    with psycopg.connect(dsn_url, autocommit=True) as verification_conn:
        with verification_conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            assert count == 3, f"Expected 3 records, but found {count}"


def test_timescaledb_validation_with_extra_columns(timescaledb):
    """Test that validation works when event has columns not in schema"""
    (table_name, dsn_url, _, _) = timescaledb

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
    controller.emit(test_data)
    controller.terminate()
    controller.await_termination()

    # Verify data was inserted correctly for configured columns
    with psycopg.connect(dsn_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT binary_col, int_col FROM {table_name} ORDER BY int_col DESC LIMIT 1")
            result = cursor.fetchone()
            assert result[1] == 42  # int_col
            # PostgreSQL returns memoryview for binary data, convert to bytes for comparison
            binary_result = bytes(result[0]) if isinstance(result[0], memoryview) else result[0]
            assert binary_result == b"test_binary"  # binary_col


@pytest.mark.parametrize(
    "invalid_data,expected_error",
    [
        ("string_data", "string indices must be integers"),
        (123, "'int' object is not subscriptable"),
        (["list_data"], "time data 'list_data' does not match format"),
        (None, "'NoneType' object is not subscriptable"),
    ],
)
def test_timescaledb_validation_non_dict_data_type_error(timescaledb, invalid_data, expected_error):
    """Test that validation properly rejects non-dictionary data types"""
    (table_name, dsn_url, _, columns_config) = timescaledb

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

    # Should raise appropriate error for non-dictionary data (the error occurs during processing)
    with pytest.raises((TypeError, ValueError), match=expected_error):
        controller.emit(invalid_data)
        controller.terminate()
        controller.await_termination()


def test_timescaledb_validation_non_dict_in_graph(timescaledb):
    """Test that non-dictionary data is properly rejected within a complete graph processing flow"""
    (table_name, dsn_url, timestamp_precision, columns_config) = timescaledb

    time_formats = {"milliseconds": "%d/%m/%y %H:%M:%S.%f UTC%z", "microseconds": "%d/%m/%y %H:%M:%S.%f UTC%z"}
    time_format = time_formats.get(timestamp_precision, "%d/%m/%y %H:%M:%S UTC%z")

    # Create a flow with max_events=1 to trigger immediate processing
    controller = build_flow(
        [
            SyncEmitSource(),
            TimescaleDBTarget(
                dsn=dsn_url,
                table=table_name,
                time_col="time",
                columns=columns_config,
                time_format=time_format,
                max_events=1,  # Process immediately to test validation
            ),
        ]
    ).run()

    # Test with non-dictionary data - should raise TypeError during graph processing
    # The error occurs in the parent Writer class when it tries to process non-dict data
    with pytest.raises(TypeError, match=r"string indices must be integers"):
        controller.emit("this_is_not_a_dictionary")
        controller.terminate()
        controller.await_termination()


def test_timescaledb_non_dict_emission_in_graph_context(timescaledb):
    """Test non-dictionary data emission within a complete graph processing context"""
    (table_name, dsn_url, timestamp_precision, columns_config) = timescaledb

    time_formats = {"milliseconds": "%d/%m/%y %H:%M:%S.%f UTC%z", "microseconds": "%d/%m/%y %H:%M:%S.%f UTC%z"}
    time_format = time_formats.get(timestamp_precision, "%d/%m/%y %H:%M:%S UTC%z")

    # Create a flow that processes data through multiple steps
    controller = build_flow(
        [
            SyncEmitSource(),
            TimescaleDBTarget(
                dsn=dsn_url,
                table=table_name,
                time_col="time",
                columns=columns_config,
                time_format=time_format,
                max_events=1,
            ),
        ]
    ).run()

    # First emit valid data to ensure flow works - use proper time format for precision
    if timestamp_precision in ["milliseconds", "microseconds"]:
        time_str = (
            "18/09/19 01:55:10.123000 UTC+0000"
            if timestamp_precision == "microseconds"
            else "18/09/19 01:55:10.123 UTC+0000"
        )
    else:
        time_str = "18/09/19 01:55:10 UTC+0000"

    # Use a proper datetime object for timestamp_col
    from datetime import datetime, timezone

    timestamp_obj = datetime(2019, 9, 18, 1, 55, 10, tzinfo=timezone.utc)

    valid_data = {
        "time": time_str,
        "binary_col": b"test_binary",
        "bool_col": True,
        "double_col": 123.456,
        "float_col": 12.34,
        "int_col": 42,
        "timestamp_col": timestamp_obj,
        "nchar_col": "TEST",
        "varchar_col": "test_value",
    }

    controller.emit(valid_data)

    # Now test with non-dictionary data in the context of a running flow
    non_dict_data = "non_dictionary_string"

    # This should raise an error during processing within the graph context
    with pytest.raises(TypeError, match=r"string indices must be integers"):
        controller.emit(non_dict_data)
        controller.terminate()
        controller.await_termination()


# Unit tests for retry mechanism - these don't depend on timestamp precision


@pytest.mark.asyncio
async def test_timescaledb_retry_success_first_attempt(timescaledb):
    """Test successful operation on first attempt."""
    (table_name, dsn_url, _, columns_config) = timescaledb

    target = TimescaleDBTarget(
        dsn=dsn_url, time_col="time", columns=columns_config, table=table_name, max_retries=3, retry_delay=1.0
    )

    mock_operation = AsyncMock(return_value="success")

    result = await target._execute_with_retry(mock_operation)

    assert result == "success"
    mock_operation.assert_called_once()


@pytest.mark.asyncio
@patch("asyncio.sleep")
async def test_timescaledb_retry_deadlock_behavior(mock_sleep, timescaledb):
    """Test deadlock retry with correct timing and jitter."""
    (table_name, dsn_url, _, columns_config) = timescaledb

    target = TimescaleDBTarget(
        dsn=dsn_url, time_col="time", columns=columns_config, table=table_name, max_retries=3, retry_delay=1.0
    )

    mock_operation = AsyncMock()
    # First 3 calls raise deadlock, 4th succeeds
    mock_operation.side_effect = [
        psycopg.errors.DeadlockDetected("deadlock detected"),
        psycopg.errors.DeadlockDetected("deadlock detected"),
        psycopg.errors.DeadlockDetected("deadlock detected"),
        "success",
    ]

    with patch("random.uniform", return_value=0.025):  # Fixed jitter for testing
        result = await target._execute_with_retry(mock_operation)

    assert result == "success"
    assert mock_operation.call_count == 4

    # Verify sleep calls with expected timing: 0.1 + 0.025, 0.2 + 0.025, 0.4 + 0.025
    expected_delays = [0.125, 0.225, 0.425]
    actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
    # Use approximate comparison for floating point precision
    for actual, expected in zip(actual_delays, expected_delays):
        assert abs(actual - expected) < 1e-10


@pytest.mark.asyncio
@patch("asyncio.sleep")
async def test_timescaledb_retry_deadlock_exhaustion(mock_sleep, timescaledb):
    """Test deadlock retry gives up after MAX_DEADLOCK_RETRIES."""
    (table_name, dsn_url, _, columns_config) = timescaledb

    target = TimescaleDBTarget(
        dsn=dsn_url, time_col="time", columns=columns_config, table=table_name, max_retries=3, retry_delay=1.0
    )

    mock_operation = AsyncMock()
    # Always raise deadlock
    mock_operation.side_effect = psycopg.errors.DeadlockDetected("persistent deadlock")

    with pytest.raises(ValueError, match="Deadlock persisted after 3 retries"):
        await target._execute_with_retry(mock_operation)

    # Should attempt 4 times (initial + 3 retries)
    assert mock_operation.call_count == 4
    # Should sleep 3 times (after each failed retry)
    assert mock_sleep.call_count == 3


@pytest.mark.asyncio
@patch("asyncio.sleep")
async def test_timescaledb_retry_connection_error_retry_behavior(mock_sleep, timescaledb):
    """Test connection error retry with exponential backoff."""
    (table_name, dsn_url, _, columns_config) = timescaledb

    target = TimescaleDBTarget(
        dsn=dsn_url, time_col="time", columns=columns_config, table=table_name, max_retries=3, retry_delay=1.0
    )

    mock_operation = AsyncMock()
    # First 2 calls raise connection error, 3rd succeeds
    mock_operation.side_effect = [
        psycopg.OperationalError("connection timeout"),
        psycopg.OperationalError("connection timeout"),
        "success",
    ]

    result = await target._execute_with_retry(mock_operation)

    assert result == "success"
    assert mock_operation.call_count == 3

    # Verify exponential backoff: 1s, 2s
    expected_delays = [1.0, 2.0]
    actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
    assert actual_delays == expected_delays


@pytest.mark.asyncio
async def test_timescaledb_retry_non_retriable_error_passthrough(timescaledb):
    """Test that non-retriable errors pass through without retry."""
    (table_name, dsn_url, _, columns_config) = timescaledb

    target = TimescaleDBTarget(
        dsn=dsn_url, time_col="time", columns=columns_config, table=table_name, max_retries=3, retry_delay=1.0
    )

    mock_operation = AsyncMock()
    mock_operation.side_effect = psycopg.DataError("invalid data format")

    with pytest.raises(psycopg.DataError):
        await target._execute_with_retry(mock_operation)

    # Should only attempt once
    mock_operation.assert_called_once()


@pytest.mark.asyncio
@patch("asyncio.sleep")
async def test_timescaledb_retry_custom_retry_configuration(mock_sleep, timescaledb):
    """Test custom retry configuration parameters."""
    (table_name, dsn_url, _, columns_config) = timescaledb

    custom_target = TimescaleDBTarget(
        dsn=dsn_url,
        time_col="time",
        columns=columns_config,
        table=table_name,
        max_retries=2,  # Custom max retries
        retry_delay=0.5,  # Custom delay
    )

    mock_operation = AsyncMock()
    mock_operation.side_effect = [
        psycopg.OperationalError("connection error"),
        psycopg.OperationalError("connection error"),
        "success",
    ]

    result = await custom_target._execute_with_retry(mock_operation)

    assert result == "success"
    # Verify custom exponential backoff: 0.5s, 1.0s
    expected_delays = [0.5, 1.0]
    actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
    assert actual_delays == expected_delays


def test_timescaledb_retry_retry_configuration_defaults(timescaledb):
    """Test default retry configuration values."""
    (table_name, dsn_url, _, columns_config) = timescaledb

    default_target = TimescaleDBTarget(dsn=dsn_url, time_col="time", columns=columns_config, table=table_name)

    assert default_target._max_retries == TimescaleDBTarget.DEFAULT_MAX_RETRIES
    assert default_target._retry_delay == TimescaleDBTarget.DEFAULT_RETRY_DELAY
    assert default_target.MAX_DEADLOCK_RETRIES == 3
