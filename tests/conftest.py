import os

import fakeredis
import redis as r
import pytest

from storey import RedisDriver

REDIS_ENDPOINT = os.environ.get('REDIS_URL')


@pytest.fixture()
def redis():
    if REDIS_ENDPOINT:
        yield r.Redis.from_url(REDIS_ENDPOINT)
    else:
        yield fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture()
def redis_driver(redis):
    yield RedisDriver(redis, key_prefix="storey-test:")
