import asyncio
import json
import os
import queue
import re
import threading

import aiohttp

_termination_obj = object()


class FlowException(Exception):
    pass


class Flow:
    def __init__(self):
        self._outlet = None

    def to(self, outlet):
        self._outlet = outlet
        return outlet

    def run(self):
        if self._outlet:
            return self._outlet.run()

    async def do(self, element):
        if self._outlet:
            return await self._outlet.do(self, element)


class Broadcast(Flow):
    def __init__(self, matfn):
        super().__init__()
        self._outlets = []
        self._run_count = 0
        self._matfn = matfn

    def to(self, outlet):
        self._outlets.append(outlet)
        return outlet

    async def do(self, element):
        if element is _termination_obj:
            mat_result = await self._outlets[0].do(_termination_obj)
            for i in range(1, len(self._outlets)):
                mat_result = self._matfn(mat_result, await self._outlets[i].do(_termination_obj))
            return mat_result
        tasks = []
        for i in range(len(self._outlets)):
            tasks.append(asyncio.get_running_loop().create_task(self._outlets[i].do(element)))
        for task in tasks:
            await task


class MaterializedFlow:
    def __init__(self, emit_fn, await_termination_fn):
        self._emit_fn = emit_fn
        self._await_termination_fn = await_termination_fn

    def emit(self, element):
        self._emit_fn(element)

    def terminate(self):
        self.emit(_termination_obj)

    def await_termination(self):
        return self._await_termination_fn()


class Source(Flow):
    def __init__(self, buffer_size=1):
        super().__init__()
        assert buffer_size > 0, 'Buffer size must be positive'
        self._q = queue.Queue(buffer_size)
        self._termination_q = queue.Queue(1)
        self._ex = None

    async def _run_loop(self):
        loop = asyncio.get_running_loop()
        self._termination_future = asyncio.futures.Future()

        while True:
            element = await loop.run_in_executor(None, self._q.get)
            if self._outlet:
                try:
                    mat_result = await self._outlet.do(element)
                    if element is _termination_obj:
                        self._termination_future.set_result(mat_result)
                except BaseException as ex:
                    self._ex = ex
                    if not self._q.empty():
                        self._q.get()
                    self._termination_future.set_result(None)
                    break
            if element is _termination_obj:
                break

    def _loop_thread_main(self):
        asyncio.run(self._run_loop())
        self._termination_q.put(self._ex)

    def _raise_on_error(self, ex):
        if ex:
            raise FlowException('Flow execution terminated due to an error') from self._ex

    def _emit(self, element):
        self._raise_on_error(self._ex)
        self._q.put(element)
        self._raise_on_error(self._ex)

    def run(self):
        super().run()

        thread = threading.Thread(target=self._loop_thread_main)
        thread.start()

        def raise_error_or_materialize_value():
            self._raise_on_error(self._termination_q.get())
            return self._termination_future.result()

        return MaterializedFlow(self._emit, raise_error_or_materialize_value)


class UnaryFunctionFlow(Flow):
    def __init__(self, fn):
        super().__init__()
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

    async def do(self, element):
        if element is _termination_obj:
            if self._outlet:
                return await self._outlet.do(_termination_obj)
        else:
            fn_result = await self._call(element)
            if self._outlet:
                await self._do_internal(element, fn_result)


class Map(UnaryFunctionFlow):
    async def _do_internal(self, element, mapped_elem):
        await self._outlet.do(mapped_elem)


class Filter(UnaryFunctionFlow):
    async def _do_internal(self, element, keep):
        if keep:
            await self._outlet.do(element)


class FlatMap(UnaryFunctionFlow):
    async def _do_internal(self, element, result_elements):
        for result_element in result_elements:
            await self._outlet.do(result_element)


class Reduce(Flow):
    def __init__(self, inital_value, fn):
        super().__init__()
        assert callable(fn), f'Expected a callable, got {type(fn)}'
        self._is_async = asyncio.iscoroutinefunction(fn)
        self._fn = fn
        self._result = inital_value

    def to(self, outlet):
        raise Exception("Reduce is a terminal step. It cannot be piped further.")

    async def do(self, element):
        if element is _termination_obj:
            return self._result
        else:
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

    def __init__(self, key_extractor, join_function, table_path, attributes='*', webapi=None, access_key=None):
        Flow.__init__(self)
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
                    raise Exception(f'Type {typ} in get item response is not supported')
            response_object[name] = val
        return response_object

    async def _worker(self):
        try:
            while True:
                response_object = None
                job = await self._q.get()
                if job is _termination_obj:
                    break
                element = job[0]
                request = job[1]
                response = await request
                response_body = await response.text()
                if response.status == 200:
                    response_object = self._parse_response(response_body)
                elif response.status == 404:
                    pass
                else:
                    raise Exception(f'Failed to get item. Response status code was {response.status}: {response_body}')
                if self._outlet and response_object:
                    joined_element = self._join_function(element, response_object)
                    await self._outlet.do(joined_element)
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

    async def do(self, element):
        if not self._client_session:
            self._lazy_init()

        if self._worker_awaitable.done():
            await self._worker_awaitable
            raise Exception("JoinWithTable worker has already terminated")

        if element is _termination_obj:
            await self._q.put(_termination_obj)
            await self._worker_awaitable
        else:
            key = self._key_extractor(element)
            request = self._client_session.put(f'{self._webapi_url}/{self._table_path}/{key}',
                                               headers=self._get_item_headers, data=self._body, verify_ssl=False)
            await self._q.put((element, asyncio.get_running_loop().create_task(request)))
            if self._worker_awaitable.done():
                await self._worker_awaitable


def build_flow(steps):
    if len(steps) == 0:
        print('Cannot build an empty flow')
    cur_step = steps[0]
    for next_step in steps[1:]:
        cur_step = cur_step.to(next_step)
    return cur_step
