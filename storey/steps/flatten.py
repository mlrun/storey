# Copyright 2020 Iguazio
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
from storey import FlatMap


def Flatten(**kwargs):
    """Flatten is equivalent to FlatMap(lambda x: x)."""

    # Please note that Flatten forces full_event=False, since otherwise we can't iterate the body of the event
    if kwargs:
        kwargs["full_event"] = False
    return FlatMap(lambda x: x, **kwargs)
