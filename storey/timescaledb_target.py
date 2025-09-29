# Copyright 2025 Iguazio
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

import asyncio
import random
from typing import TYPE_CHECKING, Any, Callable, Optional

from storey.targets import _Batching, _Writer

if TYPE_CHECKING:
    from psycopg_pool import AsyncConnectionPool


class TimescaleDBTarget(_Batching, _Writer):
    """Writes incoming events to a TimescaleDB hypertable.

    TimescaleDB is a time-series database built on PostgreSQL that provides automatic partitioning and optimization
    for time-series data. This target leverages TimescaleDB's hypertables for efficient storage and querying of
    time-series events.

    :param dsn: PostgreSQL/TimescaleDB connection string in the format:
        postgresql://user:password@host:port/database or postgres://user:password@host:port/database
    :param time_col: Name of the time column that will be used as the primary time dimension for the hypertable.
        This column must contain timestamp data and will be used for time-based partitioning.
    :param columns: list of column names to be written to the hypertable. Will be extracted from events when an event
        is a dictionary. Use = notation for renaming fields (e.g. write_this=event_field). Use $ notation to refer to
        metadata ($key, event_time=$time). The time column should not be included in this list as it's specified
        separately via time_col parameter.
    :param time_format: If time_col contains string timestamps, this parameter specifies the format for parsing.
        If not provided, timestamps will be parsed according to ISO-8601 format. Common formats include:
        "%Y-%m-%d %H:%M:%S", "%d/%m/%y %H:%M:%S UTC%z", etc.
    :param table: Name of the TimescaleDB hypertable where events will be written. The table must exist and be
        configured as a hypertable before writing data. If the table name contains a '.', it will be interpreted
        as <schema>.<table> format.
    :param max_events: Maximum number of events to write in a single batch. If None (default), all events will be
        written on flow termination, or after flush_after_seconds (if flush_after_seconds is set). Larger batches
        improve write performance but increase memory usage.
    :param flush_after_seconds: Maximum number of seconds to hold events before they are written. If None (default),
        events will be written on flow termination, or after max_events are accumulated (if max_events is set).
    :param max_retries: Maximum number of retry attempts for connection-related database errors (default: 3).
        Does not apply to deadlock errors which have their own retry limit.
    :param retry_delay: Base delay in seconds between retry attempts for connection errors (default: 1.0).
        Uses exponential backoff: delay * (2^attempt). Deadlock retries use faster timing.

    Example:
        >>> # Basic usage with millisecond precision timestamps
        >>> target = TimescaleDBTarget(
        ...     dsn="postgresql://user:pass@localhost:5432/mydb",
        ...     time_col="timestamp",
        ...     columns=["sensor_id", "temperature", "humidity"],
        ...     table="sensor_data",
        ...     time_format="%Y-%m-%d %H:%M:%S.%f",
        ...     max_events=1000,
        ...     flush_after_seconds=5
        ... )

        >>> # Usage with event metadata
        >>> target = TimescaleDBTarget(
        ...     dsn="postgresql://user:pass@localhost:5432/mydb",
        ...     time_col="event_time",
        ...     columns=["$key", "value", "source=device_name"],
        ...     table="events"
        ... )

    Note:
        - The target table must be created as a TimescaleDB hypertable before use
        - The time column should be a timestamp type, preferably TIMESTAMPTZ for timezone awareness
        - Events are written using PostgreSQL's COPY protocol for optimal performance
        - Connection pooling is handled automatically with proper cleanup on termination
        - Built-in retry logic handles deadlocks (fast retry) and connection issues (exponential backoff)
        - Deadlock retries: 3 attempts with 0.1s, 0.2s, 0.4s delays (with jitter)
        - Connection retries: Configurable attempts with exponential backoff (1s, 2s, 4s by default)
    """

    # Retry configuration constants
    MAX_DEADLOCK_RETRIES = 3
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY = 1.0

    def __init__(
        self,
        dsn: str,
        time_col: str,
        columns: list[str],
        table: str,
        time_format: Optional[str] = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        **kwargs,
    ) -> None:

        # Pass parameters to parent classes
        kwargs["time_col"] = time_col
        kwargs["columns"] = columns
        if table:
            kwargs["table"] = table
        if time_format:
            kwargs["time_format"] = time_format

        # Initialize batching functionality
        _Batching.__init__(self, **kwargs)

        # Initialize writer functionality with time column as first column
        # This ensures proper column ordering for TimescaleDB's time-partitioned structure
        _Writer.__init__(
            self,
            columns=[time_col] + columns,
            infer_columns_from_data=False,
            retain_dict=True,
            time_field=time_col,
            time_format=time_format,
        )
        self._table = table

        # Store configuration
        self._time_col = time_col
        self._columns = columns
        self._max_retries = max_retries
        self._retry_delay = retry_delay

        # Database connection configuration
        self._dsn = dsn
        # Connection pool: Single connection is sufficient for most use cases since:
        # 1. COPY operations are already bulk-optimized and very fast
        # 2. Multiple concurrent COPY operations may cause lock contention
        # 3. Most data flows process batches sequentially, not concurrently
        # For high-throughput scenarios with concurrent batches, consider increasing pool size
        self._pool: Optional[AsyncConnectionPool] = None  # Connection pool will be created lazily during first use
        self._column_names = self._get_column_names()
        self._schema = None
        self._table_schema = None  # Cached table schema information
        if "." in self._table:
            self._schema, self._table = self._table.split(".", 1)

    def _init(self):
        """Initialize the target (called synchronously).

        Performs synchronous initialization including:
        1. Parent class initialization
        2. Database connection validation

        The actual connection pool creation is deferred to the first async operation
        to avoid blocking the synchronous initialization phase.
        """
        _Batching._init(self)
        _Writer._init(self)

    async def _async_init(self):
        """Initialize async components.

        Creates the connection pool on first use (lazy initialization).
        This approach avoids creating database connections during synchronous initialization
        while ensuring the pool is available when needed for data operations.
        """
        from psycopg_pool import AsyncConnectionPool

        if self._pool is None:
            self._pool = AsyncConnectionPool(self._dsn, min_size=1, max_size=1, open=False)
            await self._pool.open()

    async def _execute_with_retry(self, operation_callable: Callable[[], Any]) -> Any:
        """
        Generic async retry wrapper for database operations.

        PostgreSQL Error Handling Strategy Matrix:

        | Category                    |Retry?| Timing           | Reason                           |
        |-----------------------------|------|------------------|----------------------------------|
        | DeadlockDetected            |  Yes | 0.1s, 0.2s, 0.4s | Auto-rollback, fast resolution  |
        | Other OperationalError      |  Yes | 1s, 2s, 4s       | Network/server recovery time     |
        | InterfaceError              |  Yes | 1s, 2s, 4s       | Client connection issues         |
        | All Other psycopg.Error     |  No  | -                | Pass through without wrapping    |

        Note: PostgreSQL automatically rolls back failed transactions, so explicit
        rollback is only needed for DeadlockDetected where we retry the operation.

        Note: Unhandled errors are passed through without wrapping to preserve
        original exception types and stack traces for proper debugging.

        :param operation_callable: Async function that executes the database operation
        :return: Result of operation_callable()
        """
        import psycopg

        deadlock_attempts = 0
        connection_attempts = 0

        while True:
            try:
                return await operation_callable()
            except (psycopg.OperationalError, psycopg.InterfaceError) as e:
                # Different retry limits and timing based on error type
                if isinstance(e, psycopg.errors.DeadlockDetected):
                    if deadlock_attempts >= self.MAX_DEADLOCK_RETRIES:
                        raise ValueError(f"Deadlock persisted after {self.MAX_DEADLOCK_RETRIES} retries: {e}") from e
                    # Fast retry for deadlocks: ~0.1s, ~0.2s, ~0.4s with jitter
                    delay = (2**deadlock_attempts) * 0.1 + random.uniform(0, 0.05)
                    deadlock_attempts += 1
                else:
                    if connection_attempts >= self._max_retries:
                        raise ValueError(f"Connection failed after {self._max_retries} retries: {e}") from e
                    # Slower retry for connection issues with exponential backoff
                    delay = self._retry_delay * (2**connection_attempts)
                    connection_attempts += 1

                await asyncio.sleep(delay)

    async def _get_table_schema(self) -> dict:
        """Retrieve table schema from PostgreSQL information_schema.

        Queries the database to get column information including data types,
        nullability constraints, and default values. Results are cached to
        avoid repeated database queries.

        Returns:
            dict: Column schema information with keys as column names and values
                 containing 'data_type', 'nullable', and 'default' information

        Raises:
            ValueError: If table doesn't exist or schema query fails
        """
        if self._table_schema is not None:
            return self._table_schema

        await self._async_init()

        async def schema_operation():
            async with self._pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Query column information including nullability
                    query = """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = %s
                    AND table_schema = %s
                    ORDER BY ordinal_position
                    """
                    schema_name = self._schema or "public"
                    await cur.execute(query, (self._table, schema_name))
                    rows = await cur.fetchall()

                    if not rows:
                        # Check if table exists to provide a more specific error message
                        table_exists_query = """
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = %s AND table_schema = %s
                        """
                        await cur.execute(table_exists_query, (self._table, schema_name))
                        table_exists = await cur.fetchone()

                        if not table_exists:
                            raise ValueError(f"Table '{schema_name}.{self._table}' does not exist")
                        else:
                            raise ValueError(f"Table '{schema_name}.{self._table}' exists but has no columns")

                    schema_result = {}
                    for row in rows:
                        column_name, data_type, is_nullable, column_default = row
                        schema_result[column_name] = {
                            "data_type": data_type,
                            "nullable": is_nullable == "YES",
                            "default": column_default,
                        }
                    return schema_result

        self._table_schema = await self._execute_with_retry(schema_operation)
        return self._table_schema

    def _event_to_batch_entry(self, event):
        """Convert an event to a batch entry format.

        Transforms incoming events into the format expected by the writer system.
        This method delegates to the parent _Writer class which handles field extraction,
        renaming, and metadata processing.

        Args:
            event: The incoming event to be processed

        Returns:
            Processed event data ready for batch writing
        """
        return self._event_to_writer_entry(event)

    async def _emit(self, batch, batch_key, batch_time, batch_events, last_event_time=None):
        """Write a batch of events to TimescaleDB.

        This method performs the core data writing functionality:
        1. Ensures the connection pool is initialized
        2. Converts dictionary events to tuples for efficient COPY operations
        3. Uses PostgreSQL's COPY protocol for high-performance bulk inserts
        4. Maintains proper column ordering for TimescaleDB compatibility

        Args:
            batch: list of events to write
            batch_key: Key used for batching (unused in this implementation)
            batch_time: Timestamp when batch was created
            batch_events: list of original event objects
            last_event_time: Timestamp of the most recent event in the batch
        """
        # Ensure connection pool is created
        await self._async_init()

        # Skip processing if batch is empty
        if not batch:
            return

        # Get table schema for validation on first use
        schema = await self._get_table_schema()

        # Convert dictionaries to tuples for copy_records_to_table
        # PostgreSQL's COPY protocol requires data in tuple format with consistent column ordering
        # Validate against schema to catch missing required columns early

        records = []
        for item in batch:
            if not isinstance(item, dict):
                # Only dictionaries are supported as input
                raise TypeError(f"TimescaleDBTarget only supports dictionary data, got {type(item).__name__}")

            # Validate against schema and convert to tuple in correct column order
            record = []
            for col in self._column_names:
                if col not in schema:
                    raise ValueError(f"Column '{col}' is configured but not found in table '{self._table}' schema")

                col_info = schema[col]
                value = item.get(col)  # Get value or None if missing

                if value is None and not col_info["nullable"]:
                    # Column missing but required (not nullable)
                    raise ValueError(
                        f"Missing required non-nullable column '{col}' in event. "
                        f"Available columns: {', '.join(item.keys())} in table '{self._table}'"
                    )

                record.append(value)

            records.append(tuple(record))

        # Write data using connection pool with retry logic
        async def batch_write_operation():
            import psycopg.sql as sql

            async with self._pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Use PostgreSQL's COPY protocol for optimal performance
                    # This is significantly faster than individual INSERT statements
                    table_identifier = sql.Identifier(self._table)
                    if self._schema:
                        table_identifier = sql.Identifier(self._schema, self._table)

                    column_identifiers = [sql.Identifier(col) for col in self._column_names]
                    copy_query = sql.SQL("COPY {} ({}) FROM STDIN").format(
                        table_identifier, sql.SQL(", ").join(column_identifiers)
                    )

                    async with cur.copy(copy_query) as copy_context:
                        for record in records:
                            await copy_context.write_row(record)

        await self._execute_with_retry(batch_write_operation)

    async def _terminate(self):
        """Terminate and cleanup resources.

        Properly closes the connection pool and releases all database connections.
        This method is called during flow shutdown to ensure clean resource cleanup
        and prevent connection leaks.
        """
        if self._pool:
            await self._pool.close()
            self._pool = None

    def _get_column_names(self) -> list[str]:
        """Get list of column names in the correct order for database operations.

        TimescaleDB hypertables require the time column to be first for optimal performance
        and proper partitioning. This method ensures the correct column ordering while
        preventing duplicate column names.

        Returns:
            list[str]: Column names with time column first, followed by data columns
        """
        # Start with time column
        column_names = [self._time_col]

        # Add other columns, but skip time_col if it's already in the list
        # This prevents duplicate column errors in the database
        for col in self._columns:
            if col != self._time_col:
                column_names.append(col)

        return column_names
