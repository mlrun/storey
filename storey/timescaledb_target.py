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
from typing import Optional

import asyncpg

from storey.targets import _Batching, _Writer


class TimescaleDBTarget(_Batching, _Writer):
    """Writes incoming events to a TimescaleDB hypertable.

    TimescaleDB is a time-series database built on PostgreSQL that provides automatic partitioning and optimization
    for time-series data. This target leverages TimescaleDB's hypertables for efficient storage and querying of
    time-series events.

    :param dsn: PostgreSQL/TimescaleDB connection string in the format:
        postgresql://user:password@host:port/database or postgres://user:password@host:port/database
    :type dsn: str
    :param time_col: Name of the time column that will be used as the primary time dimension for the hypertable.
        This column must contain timestamp data and will be used for time-based partitioning.
    :type time_col: str
    :param columns: list of column names to be written to the hypertable. Will be extracted from events when an event
        is a dictionary. Use = notation for renaming fields (e.g. write_this=event_field). Use $ notation to refer to
        metadata ($key, event_time=$time). The time column should not be included in this list as it's specified
        separately via time_col parameter.
    :type columns: list[str]
    :param time_format: If time_col contains string timestamps, this parameter specifies the format for parsing.
        If not provided, timestamps will be parsed according to ISO-8601 format. Common formats include:
        "%Y-%m-%d %H:%M:%S", "%d/%m/%y %H:%M:%S UTC%z", etc.
    :type time_format: Optional[str]
    :param table: Name of the TimescaleDB hypertable where events will be written. The table must exist and be
        configured as a hypertable before writing data. If not specified, the table name should be provided through
        other means (e.g., via batching configuration).
    :type table: Optional[str]
    :param max_connections: Maximum number of connections in the asyncpg connection pool. Higher values allow for
        better concurrency but consume more database resources. Defaults to 10.
    :type max_connections: int
    :param min_connections: Minimum number of connections in the asyncpg connection pool. Defaults to 10.
    :type min_connections: int
    :param max_events: Maximum number of events to write in a single batch. If None (default), all events will be
        written on flow termination, or after flush_after_seconds (if flush_after_seconds is set). Larger batches
        improve write performance but increase memory usage.
    :type max_events: int
    :param flush_after_seconds: Maximum number of seconds to hold events before they are written. If None (default),
        events will be written on flow termination, or after max_events are accumulated (if max_events is set).
    :type flush_after_seconds: int

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
        - The time column must be of TIMESTAMPTZ type for proper time-series functionality
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
        max_connections: int = 10,
        min_connections: int = 10,
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
        self._max_connections = max_connections
        self._min_connections = min_connections
        self._pool = None  # Connection pool will be created lazily during first use

    def _test_connection_sync(self) -> None:
        """Test database connection synchronously during initialization.

        This method validates that the TimescaleDB instance is accessible and properly configured
        before any events are processed. It performs the following checks:
        1. Basic connectivity to PostgreSQL/TimescaleDB
        2. Table existence validation (if table name is provided)
        3. TimescaleDB extension availability (optional check)

        Raises:
            ConnectionError: If connection fails, table doesn't exist, or other database issues occur
        """
        try:
            # Get or create event loop for sync context
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # No event loop exists, create one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Test connection synchronously
            loop.run_until_complete(self._test_connection_async())

        except Exception as e:
            raise ConnectionError(f"Failed to connect to TimescaleDB: {e}") from e

    async def _test_connection_async(self) -> None:
        """Async helper for connection testing.

        Creates a temporary connection to validate database accessibility and configuration.
        This connection is separate from the main connection pool and is closed immediately
        after testing.
        """
        # Create a temporary connection to test
        conn = await asyncpg.connect(dsn=self._dsn)

        try:
            # Test basic connectivity
            await conn.execute("SELECT 1")

            # Check if TimescaleDB extension is installed
            result = await conn.fetchrow("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';")
            if not result:
                raise ConnectionError("TimescaleDB extension is not installed")

            # Test if table exists (if specified)
            if self._table:
                table_exists = await conn.fetchrow(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = $1 LIMIT 1", self._table
                )
                if not table_exists:
                    raise ConnectionError(f"Table '{self._table}' does not exist")

        finally:
            await conn.close()

    async def _create_pool_async(self) -> asyncpg.Pool:
        """Create asyncpg connection pool asynchronously.

        Establishes a connection pool with the specified DSN and connection limits.
        The pool is configured for optimal performance with TimescaleDB's time-series workloads.

        Returns:
            asyncpg.Pool: Configured connection pool ready for use
        """
        return await asyncpg.create_pool(
            dsn=self._dsn,
            min_size=self._min_connections,
            max_size=self._max_connections,
        )

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

        # Test database connection during initialization
        # This ensures early failure detection if the database is unreachable
        self._test_connection_sync()

    async def _async_init(self):
        """Initialize async components.

        Creates the connection pool on first use (lazy initialization).
        This approach avoids creating database connections during synchronous initialization
        while ensuring the pool is available when needed for data operations.
        """
        if self._pool is None:
            self._pool = await self._create_pool_async()

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
        column_names = self._get_column_names()

        records = []
        for item in batch:
            if isinstance(item, dict):
                # Convert dict to tuple in correct column order
                # This ensures time column is first, followed by data columns
                record = tuple(item.get(col) for col in column_names)
                records.append(record)
            else:
                # Handle pre-processed tuple/list data
                records.append(item)

        # Write data using connection pool
        async with self._pool.acquire() as conn:
            # Use PostgreSQL's COPY protocol for optimal performance
            # This is significantly faster than individual INSERT statements
            await conn.copy_records_to_table(self._table, records=records, columns=column_names)

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
