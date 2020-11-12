import asyncio
import csv
import io
import json
import random

import aiofiles

from .dtypes import V3ioError
from .flow import Flow, _termination_obj, _split_path


class WriteToCSV(Flow):
    """
    Writes events to a CSV file.

    :param path: path where CSV file will be written.
    :type path: string
    :param columns: fields to be written to CSV. Will be written as the file header if write_header is True. Will be extracted from
    events when an event is a dictionary (lists will be written as is). Optional. Defaults to None (will be inferred if event is
    dictionary).
    :type columns: list of string
    :param metadata_columns: Map from column name to metadata field name (e.g. {'event_time': 'time'}). Optional. Default to
    None (all columns will be taken from data, none from metadata).
    :type metadata_columns: dict
    :param write_header: Whether to write the columns as a CSV header.
    :type write_header: boolean
    :param name: Name of this step, as it should appear in logs. Defaults to class name (WriteToCSV).
    :type name: string
    :param full_event: Whether user functions should receive and/or return Event objects (when True), or only the payload (when False).
    Defaults to False.
    :type full_event: boolean
    """

    def __init__(self, path, columns=None, write_header=False, metadata_columns=None, **kwargs):
        super().__init__(**kwargs)
        self._path = path
        self._columns = columns
        self._write_header = write_header
        self._metadata_columns = metadata_columns
        self._open_file = None
        self._first_event = True

    async def _do(self, event):
        if event is _termination_obj:
            if self._open_file:
                await self._open_file.close()
            return await self._do_downstream(_termination_obj)
        try:
            if not self._open_file:
                self._open_file = await aiofiles.open(self._path, mode='w')
            data = event.body
            if isinstance(data, dict):
                if self._first_event and not self._columns:
                    self._columns = list(data.keys())
                    if self._metadata_columns:
                        self._columns.extend(self._metadata_columns.keys())
                    self._columns.sort()
                if self._columns:
                    new_data = []
                    for column in self._columns:
                        if self._metadata_columns and column in self._metadata_columns:
                            metadata_attr = self._metadata_columns[column]
                            new_value = getattr(event, metadata_attr)
                        else:
                            new_value = data[column]
                        new_data.append(new_value)
                    data = new_data
            elif isinstance(data, list) and self._columns and self._metadata_columns:
                new_data = []
                data_cursor = 0
                for column in self._columns:
                    if column in self._metadata_columns:
                        metadata_attr = self._metadata_columns[column]
                        new_value = getattr(event, metadata_attr)
                        new_data.append(new_value)
                    else:
                        new_data.append(data[data_cursor])
                        data_cursor += 1
                data = new_data
            if self._first_event:
                if not self._columns and self._write_header:
                    raise ValueError('columns must be defined when write_header is True and events type is not dictionary')
                linebuf = io.StringIO()
                csv_writer = csv.writer(linebuf)
                if self._write_header:
                    csv_writer.writerow(self._columns)
                line = linebuf.getvalue()
                await self._open_file.write(line)
                self._first_event = False
            linebuf = io.StringIO()
            csv_writer = csv.writer(linebuf)
            csv_writer.writerow(data)
            line = linebuf.getvalue()
            await self._open_file.write(line)
        except BaseException as ex:
            if self._open_file:
                await self._open_file.close()
            raise ex


class WriteToV3IOStream(Flow):
    """Writes all incoming events into a V3IO stream.

    :param storage: V3IO driver.
    :type storage: V3ioDriver
    :param stream_path: Path to the V3IO stream.
    :type stream_path: string
    :param sharding_func: Function for determining the shard ID to which to write each event.
    :type sharding_func: Function (Event=>int)
    :param batch_size: Batch size for each write request.
    :type batch_size: int
    """

    def __init__(self, storage, stream_path, sharding_func=None, batch_size=8, **kwargs):
        Flow.__init__(self, **kwargs)

        self._storage = storage

        if sharding_func is not None and not callable(sharding_func):
            raise TypeError(f'Expected a callable, got {type(sharding_func)}')

        self._container, self._stream_path = _split_path(stream_path)

        self._sharding_func = sharding_func

        self._batch_size = batch_size

        self._shard_count = None

    @staticmethod
    async def _handle_response(request):
        if request:
            response = await request
            if response.output.failed_record_count == 0:
                return
            raise V3ioError(f'Failed to put records to V3IO. Got {response.status_code} response: {response.body}')

    def _build_request_put_records(self, shard_id, events):
        record_list_for_json = []
        for event in events:
            record = event.body
            if isinstance(record, dict):
                record = json.dumps(record).encode("utf-8")
            record_list_for_json.append({'shard_id': shard_id, 'data': record})

        return record_list_for_json

    def _send_batch(self, buffers, in_flight_reqs, shard_id):
        buffer = buffers[shard_id]
        buffers[shard_id] = []
        request_body = self._build_request_put_records(shard_id, buffer)
        request = self._storage._put_records(self._container, self._stream_path, request_body)
        in_flight_reqs[shard_id] = asyncio.get_running_loop().create_task(request)

    async def _worker(self):
        try:
            buffers = []
            in_flight_reqs = []
            for _ in range(self._shard_count):
                buffers.append([])
                in_flight_reqs.append(None)
            while True:
                for shard_id in range(self._shard_count):
                    if self._q.empty():
                        req = in_flight_reqs[shard_id]
                        in_flight_reqs[shard_id] = None
                        await self._handle_response(req)
                        if len(buffers[shard_id]) >= self._batch_size:
                            self._send_batch(buffers, in_flight_reqs, shard_id)
                event = await self._q.get()
                if event is _termination_obj:  # handle outstanding batches and in flight requests on termination
                    for req in in_flight_reqs:
                        await self._handle_response(req)
                    for shard_id in range(self._shard_count):
                        if buffers[shard_id]:
                            self._send_batch(buffers, in_flight_reqs, shard_id)
                    for req in in_flight_reqs:
                        await self._handle_response(req)
                    break
                shard_id = self._sharding_func(event) % self._shard_count
                buffers[shard_id].append(event)
                if len(buffers[shard_id]) >= self._batch_size:
                    if in_flight_reqs[shard_id]:
                        req = in_flight_reqs[shard_id]
                        in_flight_reqs[shard_id] = None
                        await self._handle_response(req)
                    self._send_batch(buffers, in_flight_reqs, shard_id)
        except BaseException as ex:
            if not self._q.empty():
                await self._q.get()
            raise ex
        finally:
            await self._storage.close()

    async def _lazy_init(self):
        if not self._shard_count:
            response = await self._storage._describe(self._container, self._stream_path)

            self._shard_count = response.shard_count
            if self._sharding_func is None:
                def f(_):
                    return random.randint(0, self._shard_count - 1)

                self._sharding_func = f

            self._q = asyncio.queues.Queue(self._batch_size * self._shard_count)
            self._worker_awaitable = asyncio.get_running_loop().create_task(self._worker())

    async def _do(self, event):
        await self._lazy_init()

        if self._worker_awaitable.done():
            await self._worker_awaitable
            raise AssertionError("WriteToV3IOStream worker has already terminated")

        if event is _termination_obj:
            await self._q.put(_termination_obj)
            await self._worker_awaitable
            return await self._do_downstream(_termination_obj)
        else:
            await self._q.put(event)
            if self._worker_awaitable.done():
                await self._worker_awaitable
