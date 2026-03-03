# Copyright 2026 Iguazio
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


class MockLogger:
    def __init__(self):
        self.logs = []

    def error(self, *args, **kwargs):
        self.logs.append(("error", args, kwargs))

    def warn(self, *args, **kwargs):
        self.logs.append(("warn", args, kwargs))

    def info(self, *args, **kwargs):
        self.logs.append(("info", args, kwargs))

    def debug(self, *args, **kwargs):
        self.logs.append(("debug", args, kwargs))


class MockContext:
    def __init__(self, logger, verbose):
        self.logger = logger
        self.verbose = verbose
