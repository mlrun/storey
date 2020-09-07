import asyncio
import base64
import collections
import csv
import json
import os
import queue
import re
import threading
import time
from datetime import datetime
import random
import pickle

import aiofiles
import aiohttp

from .utils import schema_file_name

_termination_obj = object()


class FlowError(Exception):
    pass


class V3ioError(Exception):
    pass


class Flow:
    def __init__(self, termination_result_fn=lambda x, y: None):
        self._outlets = []
        self._termination_result_fn = termination_result_fn

    def to(self, outlet):
        self._outlets.append(outlet)
        return outlet

    def run(self):
        for outlet in self._outlets:
            outlet.run()

    async def _do(self, event):
        raise NotImplementedError

    async def _do_downstream(self, event):
        if not self._outlets:
            return
        if event is _termination_obj:
            termination_result = await self._outlets[0]._do(_termination_obj)
            for i in range(1, len(self._outlets)):
                termination_result = self._termination_result_fn(termination_result, await self._outlets[i]._do(_termination_obj))
            return termination_result
        tasks = []
        for i in range(len(self._outlets)):
            tasks.append(asyncio.get_running_loop().create_task(self._outlets[i]._do(event)))
        for task in tasks:
            await task


Event = collections.namedtuple('Event', 'element key time awaitable_result')


class AwaitableResult:
    def __init__(self):
        self._q = queue.Queue(1)

    def await_result(self):
        return self._q.get()

    def _set_result(self, element):
        self._q.put(element)


class FlowController:
    def __init__(self, emit_fn, await_termination_fn):
        self._emit_fn = emit_fn
        self._await_termination_fn = await_termination_fn

    def emit(self, element, key=None, event_time=None, return_awaitable_result=False):
        if event_time is None:
            event_time = time.time()
        awaitable_result = None
        if return_awaitable_result:
            awaitable_result = AwaitableResult()
        self._emit_fn(Event(element, key, event_time, awaitable_result))
        return awaitable_result

    def terminate(self):
        self._emit_fn(_termination_obj)

    def await_termination(self):
        return self._await_termination_fn()


class FlowAwaiter:
    def __init__(self, await_termination_fn):
        self._await_termination_fn = await_termination_fn

    def await_termination(self):
        return self._await_termination_fn()


class Source(Flow):
    def __init__(self, buffer_size=1, **kwargs):
        super().__init__(**kwargs)
        if buffer_size <= 0:
            raise ValueError('Buffer size must be positive')
        self._q = queue.Queue(buffer_size)
        self._termination_q = queue.Queue(1)
        self._ex = None

    async def _run_loop(self):
        loop = asyncio.get_running_loop()
        self._termination_future = asyncio.futures.Future()

        while True:
            event = await loop.run_in_executor(None, self._q.get)
            try:
                termination_result = await self._do_downstream(event)
                if event is _termination_obj:
                    self._termination_future.set_result(termination_result)
            except BaseException as ex:
                self._ex = ex
                if not self._q.empty():
                    self._q.get()
                self._termination_future.set_result(None)
                break
            if event is _termination_obj:
                break

    def _loop_thread_main(self):
        asyncio.run(self._run_loop())
        self._termination_q.put(self._ex)

    def _raise_on_error(self, ex):
        if ex:
            raise FlowError('Flow execution terminated due to an error') from self._ex

    def _emit(self, event):
        self._raise_on_error(self._ex)
        self._q.put(event)
        self._raise_on_error(self._ex)

    def run(self):
        super().run()

        thread = threading.Thread(target=self._loop_thread_main)
        thread.start()

        def raise_error_or_return_termination_result():
            self._raise_on_error(self._termination_q.get())
            return self._termination_future.result()

        return FlowController(self._emit, raise_error_or_return_termination_result)


class ReadCSV(Flow):
    def __init__(self, paths, skip_header=False, **kwargs):
        super().__init__(**kwargs)
        if isinstance(paths, str):
            paths = [paths]
        self._paths = paths
        self._skip_header = skip_header

        self._termination_q = queue.Queue(1)
        self._ex = None

    async def _run_loop(self):
        self._termination_future = asyncio.futures.Future()

        try:
            for path in self._paths:
                async with aiofiles.open(path, mode='r') as f:
                    if self._skip_header:
                        await f.readline()
                    async for line in f:
                        parsed_line = next(csv.reader([line]))
                        await self._do_downstream(Event(parsed_line, None, datetime.now(), None))
            termination_result = await self._do_downstream(_termination_obj)
            self._termination_future.set_result(termination_result)
        except BaseException as ex:
            self._ex = ex
            self._termination_future.set_result(None)

    def _loop_thread_main(self):
        asyncio.run(self._run_loop())
        self._termination_q.put(self._ex)

    def _raise_on_error(self, ex):
        if ex:
            raise FlowError('Flow execution terminated due to an error') from self._ex

    def run(self):
        super().run()

        thread = threading.Thread(target=self._loop_thread_main)
        thread.start()

        def raise_error_or_return_termination_result():
            self._raise_on_error(self._termination_q.get())
            return self._termination_future.result()

        return FlowAwaiter(raise_error_or_return_termination_result)


class UnaryFunctionFlow(Flow):
    def __init__(self, fn, **kwargs):
        super().__init__(**kwargs)
        if not callable(fn):
            raise TypeError(f'Expected a callable, got {type(fn)}')
        self._is_async = asyncio.iscoroutinefunction(fn)
        self._fn = fn

    async def _call(self, element):
        res = self._fn(element)
        if self._is_async:
            res = await res
        return res

    async def _do_internal(self, element, fn_result):
        raise NotImplementedError()

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            element = event.element
            fn_result = await self._call(element)
            await self._do_internal(event, fn_result)


class Map(UnaryFunctionFlow):
    async def _do_internal(self, event, mapped_element):
        mapped_event = Event(mapped_element, event.key, event.time, event.awaitable_result)
        await self._do_downstream(mapped_event)


class Filter(UnaryFunctionFlow):
    async def _do_internal(self, event, keep):
        if keep:
            await self._do_downstream(event)


class FlatMap(UnaryFunctionFlow):
    async def _do_internal(self, event, mapped_elements):
        for mapped_element in mapped_elements:
            mapped_event = Event(mapped_element, event.key, event.time, event.awaitable_result)
            await self._do_downstream(mapped_event)


class FunctionWithStateFlow(Flow):
    def __init__(self, initial_state, fn, **kwargs):
        super().__init__(**kwargs)
        if not callable(fn):
            raise TypeError(f'Expected a callable, got {type(fn)}')
        self._is_async = asyncio.iscoroutinefunction(fn)
        self._state = initial_state
        self._fn = fn

    async def _call(self, element):
        res, self._state = self._fn(element, self._state)
        if self._is_async:
            res = await res
        return res

    async def _do_internal(self, element, fn_result):
        raise NotImplementedError()

    async def _do(self, event):
        if event is _termination_obj:
            return await self._do_downstream(_termination_obj)
        else:
            element = event.element
            fn_result = await self._call(element)
            await self._do_internal(event, fn_result)


class MapWithState(FunctionWithStateFlow):
    async def _do_internal(self, event, mapped_element):
        mapped_event = Event(mapped_element, event.key, event.time, event.awaitable_result)
        await self._do_downstream(mapped_event)


class Complete(Flow):
    async def _do(self, event):
        await self._do_downstream(event)
        if event is not _termination_obj:
            event.awaitable_result._set_result(event.element)


class Reduce(Flow):
    def __init__(self, inital_value, fn):
        super().__init__()
        if not callable(fn):
            raise TypeError(f'Expected a callable, got {type(fn)}')
        self._is_async = asyncio.iscoroutinefunction(fn)
        self._fn = fn
        self._result = inital_value

    def to(self, outlet):
        raise ValueError("Reduce is a terminal step. It cannot be piped further.")

    async def _do(self, event):
        if event is _termination_obj:
            return self._result
        else:
            element = event.element
            res = self._fn(self._result, element)
            if self._is_async:
                res = await res
            self._result = res


class NeedsV3ioAccess:
    def __init__(self, webapi=None, access_key=None):
        webapi = webapi or os.getenv('V3IO_API')
        if not webapi:
            raise ValueError('Missing webapi parameter or V3IO_API environment variable')

        if not webapi.startswith('http://') and not webapi.startswith('https://'):
            webapi = f'http://{webapi}'

        self._webapi_url = webapi

        access_key = access_key or os.getenv('V3IO_ACCESS_KEY')
        if not access_key:
            raise ValueError('Missing access_key parameter or V3IO_ACCESS_KEY environment variable')

        self._get_item_headers = {
            'X-v3io-function': 'GetItem',
            'X-v3io-session-key': access_key
        }

        self._get_items_headers = {
            'X-v3io-function': 'GetItems',
            'X-v3io-session-key': access_key
        }

        self._put_item_headers = {
            'X-v3io-function': 'PutItem',
            'X-v3io-session-key': access_key
        }

        self._update_item_headers = {
            'X-v3io-function': 'UpdateItem',
            'X-v3io-session-key': access_key
        }

        self._put_records_headers = {
            'X-v3io-function': 'PutRecords',
            'X-v3io-session-key': access_key
        }

        self._create_stream_headers = {
            'X-v3io-function': 'CreateStream',
            'X-v3io-session-key': access_key
        }

        self._describe_stream_headers = {
            'X-v3io-function': 'DescribeStream',
            'X-v3io-session-key': access_key
        }

        self._seek_headers = {
            'X-v3io-function': 'Seek',
            'X-v3io-session-key': access_key
        }

        self._get_records_headers = {
            'X-v3io-function': 'GetRecords',
            'X-v3io-session-key': access_key
        }

        self._get_put_file_headers = {
            'X-v3io-session-key': access_key
        }


class HttpRequest:
    def __init__(self, method, url, body, headers=None):
        self.method = method
        self.url = url
        self.body = body
        if headers is None:
            headers = {}
        self.headers = headers


class HttpResponse:
    def __init__(self, status, body):
        self.status = status
        self.body = body


class JoinWithHttp(Flow):
    def __init__(self, request_builder, join_from_response, max_in_flight=8, **kwargs):
        Flow.__init__(self, **kwargs)
        self._request_builder = request_builder
        self._join_from_response = join_from_response
        self._max_in_flight = max_in_flight

        self._client_session = None

    async def _worker(self):
        try:
            while True:
                job = await self._q.get()
                if job is _termination_obj:
                    break
                event = job[0]
                request = job[1]
                response = await request
                response_body = await response.text()
                joined_element = self._join_from_response(event.element, HttpResponse(response.status, response_body))
                if joined_element is not None:
                    await self._do_downstream(Event(joined_element, event.key, event.time, event.awaitable_result))
        except BaseException as ex:
            if not self._q.empty():
                await self._q.get()
            raise ex
        finally:
            await self._client_session.close()

    def _lazy_init(self):
        connector = aiohttp.TCPConnector()
        self._client_session = aiohttp.ClientSession(connector=connector)
        self._q = asyncio.queues.Queue(self._max_in_flight)
        self._worker_awaitable = asyncio.get_running_loop().create_task(self._worker())

    async def _do(self, event):
        if not self._client_session:
            self._lazy_init()

        if self._worker_awaitable.done():
            await self._worker_awaitable
            raise FlowError("JoinWithHttp worker has already terminated")

        if event is _termination_obj:
            await self._q.put(_termination_obj)
            await self._worker_awaitable
            return await self._do_downstream(_termination_obj)
        else:
            req = self._request_builder(event)
            request = self._client_session.request(req.method, req.url, headers=req.headers, data=req.body, ssl=False)
            await self._q.put((event, asyncio.get_running_loop().create_task(request)))
            if self._worker_awaitable.done():
                await self._worker_awaitable


_non_int_char_pattern = re.compile(r"[^-0-9]")


def _v3io_parse_get_item_response(response_body):
    response_object = json.loads(response_body)["Item"]
    for name, type_to_value in response_object.items():
        val = None
        for typ, value in type_to_value.items():
            val = _convert_nginx_to_python_type(typ, value)
        response_object[name] = val
    return response_object


def _convert_nginx_to_python_type(typ, value):
    if typ == 'S' or typ == 'BOOL':
        return value
    elif typ == 'N':
        if _non_int_char_pattern.search(value):
            return float(value)
        else:
            return int(value)
    elif typ == 'B':
        return base64.b64decode(value)
    elif typ == 'TS':
        splits = value.split(':', 1)
        secs = int(splits[0])
        nanosecs = int(splits[1])
        return datetime.utcfromtimestamp(secs + nanosecs / 1000000000)
    else:
        raise V3ioError(f'Type {typ} in get item response is not supported')


def _v3io_parse_get_items_response(response_body):
    response_object = json.loads(response_body)
    i = 0
    for item in response_object['Items']:
        parsed_item = {}
        for name, type_to_value in item.items():
            for typ, value in type_to_value.items():
                val = _convert_nginx_to_python_type(typ, value)
                parsed_item[name] = val
        response_object['Items'][i] = parsed_item
        i = i + 1
    return response_object


class JoinWithV3IOTable(JoinWithHttp, NeedsV3ioAccess):

    def __init__(self, key_extractor, join_function, table_path, attributes='*', webapi=None, access_key=None, **kwargs):
        NeedsV3ioAccess.__init__(self, webapi, access_key)
        request_body = json.dumps({'AttributesToGet': attributes})

        def request_builder(event):
            key = key_extractor(event)
            return HttpRequest('PUT', f'{self._webapi_url}/{table_path}/{key}', request_body, self._get_item_headers)

        def join_from_response(element, response):
            if response.status == 200:
                response_object = _v3io_parse_get_item_response(response.body)
                return join_function(element, response_object)
            elif response.status == 404:
                return None
            else:
                raise V3ioError(f'Failed to get item. Response status code was {response.status}: {response.body}')

        JoinWithHttp.__init__(self, request_builder, join_from_response, **kwargs)


def _build_request_put_records(shard_id, events):
    record_list_for_json = []
    for event in events:
        record = event.element
        if isinstance(record, bytes):
            record_as_bytes = record
        elif isinstance(record, str):
            record_as_bytes = record.encode("utf-8")
        elif isinstance(record, dict):
            record_as_bytes = json.dumps(record).encode("utf-8")
        else:
            raise Exception(f'Unsupported record type {type(record)}')
        record_as_b64_string = str(base64.b64encode(record_as_bytes), "utf-8")
        record_list_for_json.append({'ShardId': shard_id, 'Data': record_as_b64_string})

    payload_obj = {
        'Records': record_list_for_json
    }
    return json.dumps(payload_obj)


class WriteToV3IOStream(Flow, NeedsV3ioAccess):

    def __init__(self, stream_path, sharding_func=None, batch_size=8, webapi=None, access_key=None, **kwargs):
        Flow.__init__(self, **kwargs)
        NeedsV3ioAccess.__init__(self, webapi, access_key)

        if sharding_func is not None and not callable(sharding_func):
            raise TypeError(f'Expected a callable, got {type(sharding_func)}')

        self.stream_path = stream_path
        self._sharding_func = sharding_func

        self._batch_size = batch_size

        self._client_session = None

    @staticmethod
    async def _handle_response(request):
        if request:
            response = await request
            response_body = await response.text()
            if response.status == 200:
                response_obj = json.loads(response_body)
                if response_obj['FailedRecordCount'] == 0:
                    return
            raise V3ioError(f'Failed to put records to V3IO. Got {response.status} response: {response_body}')

    def _send_batch(self, buffers, in_flight_reqs, shard_id):
        buffer = buffers[shard_id]
        buffers[shard_id] = []
        request_body = _build_request_put_records(shard_id, buffer)
        request = self._client_session.request('POST', f'{self._webapi_url}/{self.stream_path}/',
                                               headers=self._put_records_headers,
                                               data=request_body, ssl=False)
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
            await self._client_session.close()

    async def _lazy_init(self):
        connector = aiohttp.TCPConnector()
        self._client_session = aiohttp.ClientSession(connector=connector)
        req = self._client_session.request("PUT", f'{self._webapi_url}/{self.stream_path}/', headers=self._describe_stream_headers,
                                           data='{}', ssl=False)
        response = await req
        response_body = await response.text()
        if response.status != 200:
            raise V3ioError(f'Failed to get number of shards. Got {response.status} response: {response_body}')

        self._shard_count = int(json.loads(response_body)['ShardCount'])
        if self._sharding_func is None:
            def f(_):
                return random.randint(0, self._shard_count - 1)

            self._sharding_func = f

        self._q = asyncio.queues.Queue(self._batch_size * self._shard_count)
        self._worker_awaitable = asyncio.get_running_loop().create_task(self._worker())

    async def _do(self, event):
        if not self._client_session:
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


def build_flow(steps):
    if len(steps) == 0:
        raise ValueError('Cannot build an empty flow')
    cur_step = steps[0]
    for next_step in steps[1:]:
        if isinstance(next_step, list):
            cur_step.to(build_flow(next_step))
        else:
            cur_step.to(next_step)
            cur_step = next_step
    return steps[0]


class V3ioTable(NeedsV3ioAccess):
    def __init__(self, table, webapi=None, access_key=None):
        NeedsV3ioAccess.__init__(self, webapi, access_key)
        self.table = table
        self.client_session = None

    def lazy_init(self):
        connector = aiohttp.TCPConnector()
        self.client_session = aiohttp.ClientSession(connector=connector)

    async def save_schema(self, schema):
        if not self.client_session:
            self.lazy_init()

        response = await self.client_session.put(f'{self._webapi_url}/{self.table}/{schema_file_name}',
                                                 headers=self._get_put_file_headers, data=json.dumps(schema), ssl=False)
        if not response.status == 200:
            body = await response.text()
            raise V3ioError(f'Failed to save schema file. Response status code was {response.status}: {body}')

    async def load_schema(self):
        if not self.client_session:
            self.lazy_init()

        connector = aiohttp.TCPConnector()
        self.client_session = aiohttp.ClientSession(connector=connector)

        schema_path = f'{self._webapi_url}/{self.table}/{schema_file_name}'
        response = await self.client_session.get(schema_path, headers=self._get_put_file_headers, ssl=False)
        body = await response.text()
        if response.status == 404:
            schema = None
        elif response.status == 200:
            schema = json.loads(body)
        else:
            raise V3ioError(f'Failed to get schema at {schema_path}. Response status code was {response.status}: {body}')

        return schema

    async def save_store(self, aggr_store):
        if not self.client_session:
            self.lazy_init()

        for key, aggregation_element in aggr_store.items():
            data = {'Item': self._get_attributes_as_blob(aggregation_element), 'UpdateMode': 'CreateOrReplaceAttributes'}
            response = await self.client_session.put(f'{self._webapi_url}/{self.table}/{key}',
                                                     headers=self._put_item_headers, data=json.dumps(data), ssl=False)
            if not response.status == 200:
                body = await response.text()
                raise V3ioError(f'Failed to save aggregation for key: {key}. Response status code was {response.status}: {body}')

    async def save_key(self, key, aggr_item):
        if not self.client_session:
            self.lazy_init()

        data = {'Item': self._get_attributes_as_blob(aggr_item), 'UpdateMode': 'CreateOrReplaceAttributes'}
        response = await self.client_session.put(f'{self._webapi_url}/{self.table}/{key}',
                                                 headers=self._put_item_headers, data=json.dumps(data), ssl=False)
        if not response.status == 200:
            body = await response.text()
            raise V3ioError(f'Failed to save aggregation for key: {key}. Response status code was {response.status}: {body}')

    def _get_attributes_as_blob(self, aggregation_element):
        data = {}
        for name, bucket in aggregation_element.aggregation_buckets.items():
            # Only save raw aggregates, not virtual
            if bucket.should_persist:
                blob = pickle.dumps(bucket.to_dict())
                data[name] = {'B': base64.b64encode(blob).decode('ascii')}

        return data

    # Loads a specific key from the store, and returns it in the following format
    # {
    #   'feature_name_1': {'first_bucket_time': <time> 'values': []},
    #   'feature_name_2': {'first_bucket_time': <time> 'values': []}
    # }
    async def load_key(self, key):
        if not self.client_session:
            self.lazy_init()

        request_body = json.dumps({'AttributesToGet': '*'})

        response = await self.client_session.put(f'{self._webapi_url}/{self.table}/{key}',
                                                 headers=self._get_item_headers, data=request_body, ssl=False)
        body = await response.text()
        if response.status == 404:
            return None
        elif response.status == 200:
            parsed_response = _v3io_parse_get_item_response(body)
            for name, blob in parsed_response.items():
                parsed_response[name] = pickle.loads(blob)

            return parsed_response
        else:
            raise V3ioError(f'Failed to get item. Response status code was {response.status}: {body}')


class NoopTable:
    async def save_schema(self, schema):
        pass

    async def load_schema(self):
        pass

    async def save_store(self, aggr_store):
        pass

    async def save_key(self, key, aggr_item):
        pass

    async def load_key(self, key):
        pass


def new_storage_table(typ, table, kw):
    if typ == 'v3io':
        return V3ioTable(table, kw.get('webapi', None), kw.get('access_key', None))
    if typ == 'noop':
        return NoopTable()
    else:
        raise TypeError(f'storage {typ} is not supported')
