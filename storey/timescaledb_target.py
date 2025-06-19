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

from typing import Optional

from storey.targets import _Batching, _Writer


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
        configured as a hypertable before writing data. If not specified, the table name should be provided through
        other means (e.g., via batching configuration).
    :param max_events: Maximum number of events to write in a single batch. If None (default), all events will be
        written on flow termination, or after flush_after_seconds (if flush_after_seconds is set). Larger batches
        improve write performance but increase memory usage.
    :param flush_after_seconds: Maximum number of seconds to hold events before they are written. If None (default),
        events will be written on flow termination, or after max_events are accumulated (if max_events is set).

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
    """

    def __init__(
        self,
        dsn: str,
        time_col: str,
        columns: list[str],
        time_format: Optional[str] = None,
        table: Optional[str] = None,
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

        # Database connection configuration
        self._dsn = dsn
        self._pool = None  # Connection pool will be created lazily during first use
        self._column_names = self._get_column_names()

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
        if self._pool is None:
            import asyncpg

            self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=1)

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

        # Convert dictionaries to tuples for copy_records_to_table
        # PostgreSQL's COPY protocol requires data in tuple format with consistent column ordering

        records = []
        for item in batch:
            if not isinstance(item, dict):
                # Only dictionaries are supported as input
                raise TypeError(f"TimescaleDBTarget only supports dictionary data, got {type(item)}")

            # Convert dict to tuple in correct column order
            # This ensures time column is first, followed by data columns
            record = tuple(item.get(col) for col in self._column_names)
            records.append(record)
        # Write data using connection pool
        async with self._pool.acquire() as conn:
            # Use PostgreSQL's COPY protocol for optimal performance
            # This is significantly faster than individual INSERT statements
            await conn.copy_records_to_table(self._table, records=records, columns=self._column_names)

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
