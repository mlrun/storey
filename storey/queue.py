# Copyright 2020 Iguazio
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
#
import asyncio
import collections


class AsyncQueue(asyncio.Queue):
    """
    asyncio.Queue with a peek method added.
    """

    async def peek(self):
        while self.empty():
            getter = self._loop.create_future()
            self._getters.append(getter)
            try:
                await getter
            except BaseException:
                getter.cancel()  # Just in case getter is not done yet.
                try:
                    # Clean self._getters from canceled getters.
                    self._getters.remove(getter)
                except ValueError:
                    # The getter could be removed from self._getters by a
                    # previous put_nowait call.
                    pass
                if not self.empty() and not getter.cancelled():
                    # We were woken up by put_nowait(), but can't take
                    # the call.  Wake up the next in line.
                    self._wakeup_next(self._getters)
                raise
        return self.peek_nowait()

    def peek_nowait(self):
        if self.empty():
            raise asyncio.QueueEmpty
        item = self._peek()
        self._wakeup_next(self._putters)
        return item

    def _peek(self):
        return self._queue[0]


def _release_waiter(waiter):
    if not waiter.done():
        waiter.set_result(False)


class SimpleAsyncQueue:
    """
    A simple async queue with built-in timeout.
    """

    def __init__(self, capacity):
        self._capacity = capacity
        self._deque = collections.deque()
        self._not_empty_futures = collections.deque()
        self._loop = asyncio.get_running_loop()

    async def get(self, timeout=None):
        if not self._deque:
            not_empty_future = asyncio.get_running_loop().create_future()
            self._not_empty_futures.append(not_empty_future)
            if timeout is None:
                await not_empty_future
            else:
                self._loop.call_later(timeout, _release_waiter, not_empty_future)
                got_result = await not_empty_future
                if not got_result:
                    raise TimeoutError(f"Queue get() timed out after {timeout} seconds")

        result = self._deque.popleft()
        return result

    async def put(self, item):
        while self._not_empty_futures:
            not_empty_future = self._not_empty_futures.popleft()
            if not not_empty_future.done():
                not_empty_future.set_result(True)
                break

        return self._deque.append(item)

    def empty(self):
        return len(self._deque) == 0
