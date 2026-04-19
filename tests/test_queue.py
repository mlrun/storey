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
    await asyncio.sleep(0)
    assert not put_task.done(), "put() should block when queue is at capacity"
    assert await q.get() == "x"
    await put_task
    assert await q.get() == "y"
    assert await q.get() == "z"


def test_simple_async_queue():
    asyncio.run(async_test_simple_async_queue())


async def async_test_put_blocks_at_capacity():
    q = SimpleAsyncQueue(2)
    await q.put("a")
    await q.put("b")

    put_task = asyncio.create_task(q.put("c"))
    await asyncio.sleep(0)
    assert not put_task.done(), "put() should block when queue is at capacity"

    assert await q.get() == "a"
    await asyncio.sleep(0)
    assert put_task.done(), "put() should resume after get() frees a slot"

    assert await q.get() == "b"
    assert await q.get() == "c"


def test_put_blocks_at_capacity():
    asyncio.run(async_test_put_blocks_at_capacity())
