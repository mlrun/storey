import asyncio
import json
import os
import queue
import re
import threading
import time

import aiohttp


class Flow:
    def __init__(self):
        self._inlet = None
        self._outlet = None

    def to(self, outlet):
        self._outlet = outlet
        outlet._inlet = self
        return outlet

    def run(self):
        return self._inlet.run()

    async def _teardown(self):
        if self._outlet:
            await self._outlet._teardown()


class MaterializedFlow:
    def __init__(self, emit_fn):
        self._emit_fn = emit_fn

    def emit(self, element):
        self._emit_fn(element)


class Source(Flow):
    def __init__(self, buffer_size=1):
        super().__init__()
        assert buffer_size > 0, 'Buffer size must be positive'
        self._q = queue.Queue(buffer_size)

    async def _run_loop(self):
        loop = asyncio.get_running_loop()

        while True:
            element = await loop.run_in_executor(None, self._q.get)
            if element is None:
                break
            if self._outlet:
                await self._outlet.do(element)

        await self._teardown()

    def _loop_thread_main(self):
        asyncio.run(self._run_loop())

    def run(self):
        thread = threading.Thread(target=self._loop_thread_main)
        thread.start()
        return MaterializedFlow(self._q.put)


class Map(Flow):
    def __init__(self, fn):
        super().__init__()
        assert callable(fn), f'Expected a callable, got {type(fn)}'
        self._is_async = asyncio.iscoroutinefunction(fn)
        self._fn = fn

    async def do(self, element):
        mapped_elem = self._fn(element)
        if self._is_async:
            mapped_elem = await mapped_elem
        if self._outlet:
            await self._outlet.do(mapped_elem)


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
    non_int_char_pattern = re.compile(r"[^-0-9]")

    def __init__(self, key_extractor, join_function, table_path, attributes='*', webapi=None, access_key=None):
        Flow.__init__(self)
        NeedsV3ioAccess.__init__(self, webapi, access_key)
        self._key_extractor = key_extractor
        self._join_function = join_function
        self._table_path = table_path
        self._body = json.dumps({'AttributesToGet': attributes})

        self._client_session = None

    async def _worker(self):
        response_object = None
        while True:
            request = await self._q.get()
            response = await request
            response_body = await response.text()
            if response.status == 200:
                response_object = json.loads(response_body)["Item"]
                for name, type_to_value in response_object.items():
                    val = None
                    for typ, value in type_to_value.items():
                        if typ == 'S':
                            val = value
                        elif typ == 'N':
                            if self.non_int_char_pattern.search(value):
                                val = float(value)
                            else:
                                val = int(value)
                        elif typ == 'BOOL':
                            val = bool(value)
                        else:
                            raise Exception(f'Type {typ} in get item response is not supported')
                    response_object[name] = val
            elif response.status == 404:
                pass
            else:
                print(f'Failed to get item. Response status code was {response.status}: {response_body}')
            if self._outlet and response_object:
                await self._outlet.do(response_object)

    async def do(self, element):
        if not self._client_session:
            connector = aiohttp.TCPConnector()
            self._client_session = aiohttp.ClientSession(connector=connector)
            self._q = asyncio.queues.Queue(8)
            asyncio.get_running_loop().create_task(self._worker())

        key = self._key_extractor(element)
        request = self._client_session.put(f'{self._webapi_url}/{self._table_path}/{key}', headers=self._get_item_headers, data=self._body, verify_ssl=False)
        await self._q.put(asyncio.get_running_loop().create_task(request))

    async def _teardown(self):
        await super()._teardown()
        if self._client_session:
            await self._client_session.close()


def build_flow(steps):
    if len(steps) == 0:
        print('Cannot build an empty flow')
    cur_step = steps[0]
    for next_step in steps[1:]:
        cur_step = cur_step.to(next_step)
    return cur_step


async def aprint(element):
    print(element)


flow = build_flow([
    Source(),
    Map(lambda x: x + 1),
    JoinWithTable(lambda x: x, lambda x, y: y['secret'], '/bigdata/gal'),
    Map(aprint)
])

# mat.emit(1)
# mat.emit(2)
# mat.emit(None)

start = time.time()

mat = flow.run()
for outer in range(100):
    for i in range(10):
        mat.emit(i)
mat.emit(None)

end = time.time()
print(end - start)
