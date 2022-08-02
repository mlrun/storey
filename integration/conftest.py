import os

import fakeredis
import redis as r
import pytest

from storey import RedisDriver

REDIS_URL = os.environ.get('REDIS_URL')


@pytest.fixture()
def redis():
    if REDIS_URL:
        yield r.Redis.from_url(REDIS_URL)
    else:
        yield fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture()
def redis_key_prefix():
    yield "storey-test:"


@pytest.fixture()
def redis_driver(redis, redis_key_prefix):
    yield RedisDriver(redis, key_prefix=redis_key_prefix)
