import asyncio

import pytest

from storey.queue import SimpleAsyncQueue


async def async_test_simple_async_queue():
    q = SimpleAsyncQueue(2)

    with pytest.raises(TimeoutError):
        await q.get(0)

    get_task = asyncio.create_task(q.get(1))
    await q.put("x")
    assert await get_task == "x"

    await q.put("x")
    await q.put("y")
    put_task = asyncio.create_task(q.put("z"))
    assert await q.get() == "x"
    await put_task
    assert await q.get() == "y"
    assert await q.get() == "z"


def test_simple_async_queue():
    asyncio.run(async_test_simple_async_queue())
