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
            getter = asyncio.get_running_loop().create_future()
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
    """A bounded async queue with built-in timeout on get().

    Replaces asyncio.Queue + asyncio.wait_for, which can silently swallow
    items on timeout in Python < 3.12. See
    https://github.com/python/cpython/pull/98518
    """

    def __init__(self, capacity):
        self._capacity = capacity
        self._deque = collections.deque()
        self._getters = collections.deque()
        self._putters = collections.deque()
        self._size = 0
        self._loop = asyncio.get_running_loop()

    async def get(self, timeout=None):
        if not self._deque:
            getter = self._loop.create_future()
            self._getters.append(getter)
            if timeout is None:
                await getter
            else:
                self._loop.call_later(timeout, _release_waiter, getter)
                got_result = await getter
                if not got_result:
                    raise TimeoutError(f"Queue get() timed out after {timeout} seconds")

        result = self._deque.popleft()

        if self._putters:
            putter = self._putters.popleft()
            putter.set_result(True)
        else:
            self._size -= 1

        return result

    async def put(self, item):
        if self._size >= self._capacity:
            putter = self._loop.create_future()
            self._putters.append(putter)
            await putter
        else:
            self._size += 1

        self._deque.append(item)

        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                getter.set_result(True)
                break

    def empty(self):
        return len(self._deque) == 0
