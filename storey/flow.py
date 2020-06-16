import asyncio
import collections
import json
import os
import queue
import re
import threading
import time

import aiohttp

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


Event = collections.namedtuple('Event', 'element key time')


class FlowController:
    def __init__(self, emit_fn, await_termination_fn):
        self._emit_fn = emit_fn
        self._await_termination_fn = await_termination_fn

    def emit(self, element, key=None, event_time=None):
        if event_time is None:
            event_time = time.time()
        self._emit_fn(Event(element, key, event_time))

    def terminate(self):
        self._emit_fn(_termination_obj)

    def await_termination(self):
        return self._await_termination_fn()


class Source(Flow):
    def __init__(self, buffer_size=1, **kwargs):
        super().__init__(**kwargs)
        assert buffer_size > 0, 'Buffer size must be positive'
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


class UnaryFunctionFlow(Flow):
    def __init__(self, fn, **kwargs):
        super().__init__(**kwargs)
        assert callable(fn), f'Expected a callable, got {type(fn)}'
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
            return await self._do_downstream(event)
        else:
            element = event.element
            fn_result = await self._call(element)
            await self._do_internal(event, fn_result)


class Map(UnaryFunctionFlow):
    async def _do_internal(self, event, mapped_element):
        mapped_event = Event(mapped_element, event.key, event.time)
        await self._do_downstream(mapped_event)


class Filter(UnaryFunctionFlow):
    async def _do_internal(self, event, keep):
        if keep:
            await self._do_downstream(event)


class FlatMap(UnaryFunctionFlow):
    async def _do_internal(self, event, mapped_elements):
        for mapped_element in mapped_elements:
            mapped_event = Event(mapped_element, event.key, event.time)
            await self._do_downstream(mapped_event)


class Reduce(Flow):
    def __init__(self, inital_value, fn):
        super().__init__()
        assert callable(fn), f'Expected a callable, got {type(fn)}'
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
        if not webapi:
            webapi = os.getenv('V3IO_API')
        assert webapi, 'Missing webapi parameter or V3IO_API environment variable'

        if not webapi.startswith('http://') and not webapi.startswith('https://'):
            webapi = f'http://{webapi}'

        self._webapi_url = webapi

        if not access_key:
            access_key = os.getenv('V3IO_ACCESS_KEY')
        assert access_key, 'Missing access_key parameter or V3IO_ACCESS_KEY environment variable'

        self._get_item_headers = {
            'X-v3io-function': 'GetItem',
            'X-v3io-session-key': access_key
        }


class JoinWithTable(Flow, NeedsV3ioAccess):
    _non_int_char_pattern = re.compile(r"[^-0-9]")

    def __init__(self, key_extractor, join_function, table_path, attributes='*', webapi=None, access_key=None, **kwargs):
        Flow.__init__(self, **kwargs)
        NeedsV3ioAccess.__init__(self, webapi, access_key)
        self._key_extractor = key_extractor
        self._join_function = join_function
        self._table_path = table_path
        self._body = json.dumps({'AttributesToGet': attributes})

        self._client_session = None

    def _parse_response(self, response_body):
        response_object = json.loads(response_body)["Item"]
        for name, type_to_value in response_object.items():
            val = None
            for typ, value in type_to_value.items():
                if typ == 'S' or typ == 'BOOL':
                    val = value
                elif typ == 'N':
                    if self._non_int_char_pattern.search(value):
                        val = float(value)
                    else:
                        val = int(value)
                else:
                    raise V3ioError(f'Type {typ} in get item response is not supported')
            response_object[name] = val
        return response_object

    async def _worker(self):
        try:
            while True:
                response_object = None
                job = await self._q.get()
                if job is _termination_obj:
                    break
                event = job[0]
                request = job[1]
                response = await request
                response_body = await response.text()
                if response.status == 200:
                    response_object = self._parse_response(response_body)
                elif response.status == 404:
                    pass
                else:
                    raise V3ioError(f'Failed to get item. Response status code was {response.status}: {response_body}')
                if response_object:
                    joined_element = self._join_function(event.element, response_object)
                    await self._do_downstream(Event(joined_element, event.key, event.time))
        except BaseException as ex:
            if not self._q.empty():
                await self._q.get()
            raise ex
        finally:
            await self._client_session.close()

    def _lazy_init(self):
        connector = aiohttp.TCPConnector()
        self._client_session = aiohttp.ClientSession(connector=connector)
        self._q = asyncio.queues.Queue(8)
        self._worker_awaitable = asyncio.get_running_loop().create_task(self._worker())

    async def _do(self, event):
        if not self._client_session:
            self._lazy_init()

        if self._worker_awaitable.done():
            await self._worker_awaitable
            raise AssertionError("JoinWithTable worker has already terminated")

        if event is _termination_obj:
            await self._q.put(_termination_obj)
            await self._worker_awaitable
        else:
            element = event.element
            key = self._key_extractor(element)
            request = self._client_session.put(f'{self._webapi_url}/{self._table_path}/{key}',
                                               headers=self._get_item_headers, data=self._body, verify_ssl=False)
            await self._q.put((event, asyncio.get_running_loop().create_task(request)))
            if self._worker_awaitable.done():
                await self._worker_awaitable


def build_flow(steps):
    if len(steps) == 0:
        print('Cannot build an empty flow')
    cur_step = steps[0]
    for next_step in steps[1:]:
        if isinstance(next_step, list):
            cur_step.to(build_flow(next_step))
        else:
            cur_step.to(next_step)
            cur_step = next_step
    return steps[0]
