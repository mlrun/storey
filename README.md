# Storey

[![CI](https://github.com/mlrun/storey/workflows/CI/badge.svg)](https://github.com/mlrun/storey/actions?query=workflow%3ACI)

Storey is an asynchronous streaming library, for real time event processing and feature extraction.

#### In This Document

- [API Walkthrough](#api-walkthrough)
- [Usage Examples](#examples)

&#x25B6; For more information, see the [Storey Python package documentation](https://storey.readthedocs.io).

<a id="api-walkthrough"></a>
## API Walkthrough
A Storey flow consist of steps linked together by the `build_flow` function, each doing it's designated work.

### Supported Steps
#### Input Steps
* `Source` 
* `AsyncSource` 
* `CSVSource`  
* `ParquetSource` 
* `DataframeSource`  

#### Processing Steps
* `Filter` 
* `Map` 
* `FlatMap`
* `MapWithState`
* `Batch(max_events, timeout)` - Batches events. This step emits a batch every max_events events, or when timeout seconds have passed since the first event in the batch was received.
* `Choice`  
* `JoinWithV3IOTable` 
* `SendToHttp` 
* `AggregateByKey(aggregations,cache, key=None, emit_policy=EmitEveryEvent(), augmentation_fn=None)` - This step aggregates the data into the cache object provided for later persistence, and outputs an event enriched with the requested aggregation features.
* `QueryByKey(features, cache, key=None, augmentation_fn=None, aliases=None)` - Similar to to `AggregateByKey`, but this step is for serving only and does not aggregate the event.
* `NoSqlTarget(table)` - Persists the data in `table` to its associated storage by key.
* `Extend`
* `JoinWithTable`  

#### Output Steps
* `Complete`  
* `Reduce` 
* `StreamTarget` 
* `CSVTarget`
* `ReduceToDataFrame`
* `TSDBTarget`
* `ParquetTarget`     


<a id="examples"></a>
## Usage Examples

### Using Aggregates
The following example reads user data, creates features using Storey's aggregates, persists the data to V3IO and emits events containing the features to a V3IO Stream for further processing.

```python
from storey import build_flow, Source, Table, V3ioDriver, AggregateByKey, FieldAggregator, NoSqlTarget
from storey.dtypes import SlidingWindows

v3io_web_api = 'https://webapi.change-me.com'
v3io_acceess_key = '1284ne83-i262-46m6-9a23-810n41f169ea'
table_object = Table('/projects/my_features', V3ioDriver(v3io_web_api, v3io_acceess_key))

def enrich(event, state):
    if 'first_activity' not in state:
        state['first_activity'] = event.time
    event.body['time_since_activity'] = (event.time - state['first_activity']).seconds
    state['last_event'] = event.time
    event.body['total_activities'] = state['total_activities'] = state.get('total_activities', 0) + 1
    return event, state

controller = build_flow([
    Source(),
    MapWithState(table_object, enrich, group_by_key=True, full_event=True),
    AggregateByKey([FieldAggregator("number_of_clicks", "click", ["count"],
                                    SlidingWindows(['1h','2h', '24h'], '10m')),
                    FieldAggregator("purchases", "purchase_amount", ["avg", "min", "max"],
                                    SlidingWindows(['1h','2h', '24h'], '10m')),
                    FieldAggregator("failed_activities", "activity", ["count"],
                                    SlidingWindows(['1h'], '10m'),
                                    aggr_filter=lambda element: element['activity_status'] == 'fail'))],
                   table_object),
    NoSqlTarget(table_object),
    StreamTarget(V3ioDriver(v3io_web_api, v3io_acceess_key), 'features_stream')
]).run()
```

We can also create a serving function, which sole purpose is to read data from the feature store and emit it further

```python
controller = build_flow([
    Source(),
    QueryAggregationByKey([FieldAggregator("number_of_clicks", "click", ["count"],
                                           SlidingWindows(['1h','2h', '24h'], '10m')),
                           FieldAggregator("purchases", "purchase_amount", ["avg", "min", "max"],
                                           SlidingWindows(['1h','2h', '24h'], '10m')),
                           FieldAggregator("failed_activities", "activity", ["count"],
                                           SlidingWindows(['1h'], '10m'),
                                           aggr_filter=lambda element: element['activity_status'] == 'fail'))],
                           table_object)
]).run()
```
