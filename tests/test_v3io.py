# Copyright 2018 Iguazio
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
import base64
import json
from datetime import datetime

from integration.integration_test_utils import _v3io_parse_get_item_response


def test_v3io_parse_get_item_response():
    request = json.dumps({'Item': {
        'int': {'N': '55'},
        'float': {'N': '55.4'},
        'string': {'S': 'der die das'},
        'boolean': {'BOOL': True},
        'blob': {'B': base64.b64encode(b'message in a bottle').decode('ascii')},
        'timestamp': {'TS': '1594289596:123456'}
    }})
    response = _v3io_parse_get_item_response(request)
    expected = {
        'int': 55,
        'float': 55.4,
        'string': 'der die das',
        'boolean': True,
        'blob': b'message in a bottle',
        'timestamp': datetime(2020, 7, 9, 10, 13, 16, 124)
    }
    assert response == expected
