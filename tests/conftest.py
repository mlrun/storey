import os
import fakeredis
import pytest

@pytest.fixture
def redis():
    return fakeredis.FakeRedis()
