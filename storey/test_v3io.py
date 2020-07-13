import base64
import json
from datetime import datetime

from .flow import _v3io_parse_response


def test_v3io_parse_response():
    request = json.dumps({'Item': {
        'int': {'N': '55'},
        'float': {'N': '55.4'},
        'string': {'S': 'der die das'},
        'boolean': {'BOOL': True},
        'blob': {'B': base64.b64encode(b'message in a bottle').decode('ascii')},
        'timestamp': {'TS': '1594289596:123456'}
    }})
    response = _v3io_parse_response(request)
    expected = {
        'int': 55,
        'float': 55.4,
        'string': 'der die das',
        'boolean': True,
        'blob': b'message in a bottle',
        'timestamp': datetime(2020, 7, 9, 10, 13, 16, 124)
    }
    assert response == expected
