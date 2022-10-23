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
__version__ = "0.0.0+unstable"

# Importing supported filesystems explicitly so that they will get registered as an fsspec filesystem
import v3iofs  # noqa: F401

from .aggregations import AggregateByKey, QueryByKey  # noqa: F401
from .dataframe import ReduceToDataFrame, ToDataFrame  # noqa: F401
from .drivers import Driver, NoopDriver, V3ioDriver  # noqa: F401
from .dtypes import (EmissionType, EmitAfterDelay,  # noqa: F401
                     EmitAfterMaxEvent, EmitAfterPeriod, EmitAfterWindow,
                     EmitEveryEvent, EmitPolicy, Event, FieldAggregator,
                     FixedWindows, FixedWindowType, LateDataHandling,
                     SlidingWindows)
from .flow import (Batch, Choice, Complete, Context, Extend,  # noqa: F401
                   Filter, FlatMap, Flow, FlowError, HttpRequest, HttpResponse,
                   JoinWithTable, JoinWithV3IOTable, Map, MapClass,
                   MapWithState, Recover, Reduce, Rename, SendToHttp,
                   build_flow)
from .sources import (AsyncEmitSource, CSVSource,  # noqa: F401
                      DataframeSource, ParquetSource, SyncEmitSource)
from .table import Table  # noqa: F401
from .targets import (CSVTarget, KafkaTarget, NoSqlTarget,  # noqa: F401
                      ParquetTarget, StreamTarget, TSDBTarget)

# clear module namespace
del v3iofs
