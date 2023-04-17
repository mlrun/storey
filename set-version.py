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
from os import environ


def set_version():
    version = environ.get("GITHUB_REF")
    assert version, "GITHUB_REF is not defined"

    version = version.replace("refs/tags/v", "")

    lines = []
    init_py = "storey/__init__.py"
    with open(init_py) as fp:
        for line in fp:
            if "__version__" in line:
                line = f'__version__ = "{version}"\n'
            lines.append(line)

    with open(init_py, "w") as out:
        out.write("".join(lines))


set_version()
