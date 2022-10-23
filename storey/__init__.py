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
from .drivers import Driver, NoopDriver, V3ioDriver, SQLDriver  # noqa: F401
from .dtypes import EmissionType  # noqa: F401
from .dtypes import EmitAfterDelay  # noqa: F401
from .dtypes import EmitAfterMaxEvent  # noqa: F401
from .dtypes import EmitAfterPeriod  # noqa: F401
from .dtypes import EmitAfterWindow  # noqa: F401
from .dtypes import EmitEveryEvent  # noqa: F401
from .dtypes import EmitPolicy  # noqa: F401
from .dtypes import Event  # noqa: F401
from .dtypes import FieldAggregator  # noqa: F401
from .dtypes import FixedWindows  # noqa: F401
from .dtypes import FixedWindowType  # noqa: F401
from .dtypes import LateDataHandling  # noqa: F401
from .dtypes import SlidingWindows  # noqa: F401
from .flow import Batch  # noqa: F401
from .flow import Choice  # noqa: F401
from .flow import Complete  # noqa: F401
from .flow import Context  # noqa: F401
from .flow import Extend  # noqa: F401
from .flow import Filter  # noqa: F401
from .flow import FlatMap  # noqa: F401
from .flow import Flow  # noqa: F401
from .flow import FlowError  # noqa: F401
from .flow import HttpRequest  # noqa: F401
from .flow import HttpResponse  # noqa: F401
from .flow import JoinWithTable  # noqa: F401
from .flow import JoinWithV3IOTable  # noqa: F401
from .flow import Map  # noqa: F401
from .flow import MapClass  # noqa: F401
from .flow import MapWithState  # noqa: F401
from .flow import Recover  # noqa: F401
from .flow import Reduce  # noqa: F401
from .flow import Rename  # noqa: F401
from .flow import SendToHttp  # noqa: F401
from .flow import build_flow  # noqa: F401
from .sources import AsyncEmitSource  # noqa: F401
from .sources import CSVSource  # noqa: F401
from .sources import DataframeSource  # noqa: F401
from .sources import ParquetSource  # noqa: F401
from .sources import SyncEmitSource  # noqa: F401
from .sources import SQLSource # noqa: F401
from .table import Table  # noqa: F401
from .targets import CSVTarget  # noqa: F401
from .targets import KafkaTarget  # noqa: F401
from .targets import NoSqlTarget  # noqa: F401
from .targets import ParquetTarget  # noqa: F401
from .targets import StreamTarget  # noqa: F401
from .targets import TSDBTarget  # noqa: F401

# clear module namespace
del v3iofs
