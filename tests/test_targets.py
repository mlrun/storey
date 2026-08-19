# Copyright 2026 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from storey import AsyncEmitSource, StreamTarget, build_flow


def test_stream_target_hashes_string_sharding_key():
    async def run_flow():
        storage = MagicMock()
        storage._create_stream = AsyncMock(return_value=204)
        storage._put_records = AsyncMock(return_value=SimpleNamespace(output=SimpleNamespace(failed_record_count=0)))
        storage.close = AsyncMock()

        controller = build_flow(
            [
                AsyncEmitSource(),
                StreamTarget(
                    storage,
                    "container/stream",
                    sharding_func=lambda _: "sharding-key",
                    batch_size=1,
                    shards=2,
                ),
            ]
        ).run()

        await controller.emit({"value": 1})
        await controller.terminate()
        await controller.await_termination()

        storage._put_records.assert_awaited_once()

    asyncio.run(run_flow())
