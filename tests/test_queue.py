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


async def async_test_put_fifo_no_starvation():
    q = SimpleAsyncQueue(2)
    await q.put("a")
    await q.put("b")

    p1 = asyncio.create_task(q.put("c"))
    await asyncio.sleep(0)
    assert not p1.done()

    # get() wakes p1; new put must not steal the slot before p1 resumes
    assert await q.get() == "a"
    p_new = asyncio.create_task(q.put("e"))
    await asyncio.sleep(0)
    assert p1.done(), "waiting putter must not be starved by a new put()"
    assert not p_new.done(), "new put() must wait behind the existing waiter"

    assert await q.get() == "b"
    await asyncio.sleep(0)
    assert p_new.done()

    assert await q.get() == "c"
    assert await q.get() == "e"


def test_put_fifo_no_starvation():
    asyncio.run(async_test_put_fifo_no_starvation())
