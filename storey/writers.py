import asyncio
import csv
import io
import json
import random

import aiofiles

from .dtypes import V3ioError
from .flow import Flow, _termination_obj, _split_path


class WriteCSV(Flow):
    """
    Writes events to a CSV file.

    :param path: path where CSV file will be written.
    :type path: string
    :param event_to_line: function to transform an event to a CSV line (represented as a list).
    :type event_to_line: Function (Event=>list of string)
    :param header: a header for the output file.
    :type header: list of string
    :param name: Name of this step, as it should appear in logs. Defaults to class name (WriteCSV).
    :type name: string
    :param full_event: Whether user functions should receive and/or return Event objects (when True), or only the payload (when False).
    Defaults to False.
    :type full_event: boolean
    """

    def __init__(self, path, event_to_line, header=None, **kwargs):
        super().__init__(**kwargs)
        self._path = path
        self._header = header
        self._event_to_line = event_to_line
        self._open_file = None

    async def _do(self, event):
        if event is _termination_obj:
            if self._open_file:
                await self._open_file.close()
            return await self._do_downstream(_termination_obj)
        try:
            if not self._open_file:
                self._open_file = await aiofiles.open(self._path, mode='w')
                linebuf = io.StringIO()
                csv_writer = csv.writer(linebuf)
                if self._header:
                    csv_writer.writerow(self._header)
                line = linebuf.getvalue()
                await self._open_file.write(line)
            line_arr = self._event_to_line(self._get_safe_event_or_body(event))
            linebuf = io.StringIO()
            csv_writer = csv.writer(linebuf)
            csv_writer.writerow(line_arr)
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
            raise AssertionError("JoinWithHttp worker has already terminated")

        if event is _termination_obj:
            await self._q.put(_termination_obj)
            await self._worker_awaitable
            return await self._do_downstream(_termination_obj)
        else:
            await self._q.put(event)
            if self._worker_awaitable.done():
                await self._worker_awaitable
