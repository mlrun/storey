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
