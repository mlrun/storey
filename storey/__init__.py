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
__version__ = '0.0.0+unstable'

# Importing supported filesystems explicitly so that they will get registered as an fsspec filesystem
import v3iofs  # noqa: F401

from .aggregations import (  # noqa: F401
    AggregateByKey, QueryByKey
)
from .dataframe import (  # noqa: F401
    ToDataFrame, ReduceToDataFrame
)
from .drivers import (  # noqa: F401
    Driver, NoopDriver, V3ioDriver
)
from .dtypes import (  # noqa: F401
    Event, FieldAggregator, SlidingWindows, FixedWindows, EmissionType, EmitPolicy, EmitAfterPeriod, EmitAfterWindow, EmitAfterMaxEvent,
    EmitAfterDelay, EmitEveryEvent, LateDataHandling, FixedWindowType
)
from .flow import (  # noqa: F401
    Filter, FlatMap, Flow, FlowError, JoinWithV3IOTable, SendToHttp, JoinWithTable, Map, Extend, Rename, Reduce, Batch,
    MapWithState, MapClass, Complete, Choice, Recover, HttpRequest, HttpResponse, build_flow, Context
)
from .sources import (  # noqa: F401
    SyncEmitSource, AsyncEmitSource, DataframeSource, CSVSource, ParquetSource
)
from .table import (  # noqa: F401
    Table
)
from .targets import (  # noqa: F401
    StreamTarget, CSVTarget, ParquetTarget, TSDBTarget, NoSqlTarget, KafkaTarget
)

# clear module namespace
del v3iofs
