import os
import pytest

@pytest.fixture
def redis_url():
    return os.environ.get('REDIS_CONNECTION', 'redis://localhost:6379')
