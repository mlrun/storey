from .aggregations import (  # noqa: F401
    AggregateByKey, QueryByKey
)
from .dataframe import (  # noqa: F401
    ToDataFrame, ReduceToDataFrame
)
from .drivers import (  # noqa: F401
    V3ioDriver, NoopDriver
)
from .dtypes import (  # noqa: F401
    Event, FieldAggregator
)
from .flow import (  # noqa: F401
    Filter, FlatMap, Flow, FlowError, JoinWithV3IOTable, SendToHttp, JoinWithTable, Map, Extend, Reduce, Batch,
    MapWithState, MapClass, Complete, Choice, HttpRequest, HttpResponse, build_flow, Context
)
from .sources import (  # noqa: F401
    Source, AsyncSource, DataframeSource, ReadCSV
)
from .writers import (  # noqa: F401
    WriteToV3IOStream, WriteToCSV, WriteToParquet, WriteToTSDB, WriteToTable
)
from .table import (  # noqa: F401
    Table
)


def get_version():
    import os
    ref = os.getenv('GITHUB_REF')
    if ref:
        return ref.rsplit('/', 1)[-1]
    return 'unknown'


__version__ = get_version()
