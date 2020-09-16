# Storey

[![CI](https://github.com/v3io/storey/workflows/CI/badge.svg)](https://github.com/v3io/storey/actions?query=workflow%3ACI)

Storey is an asynchronous flow library, for real time event processing and feature extraction.

#### In This Document

- [API Walkthrough](#api-walkthrough)
- [Usage Examples](#examples)


<a id="api-walkthrough"></a>
## API Walkthrough
A Storey flow consist of steps linked together by the `build_flow` function, each doing it's designated work.

### Supported Steps
#### Input Steps
* `Source` - 
* `AsyncSource` - 
* `ReadCSV` - 

#### Processing Steps
* `Filter` - 
* `Map` -  
* `FlatMap` -
* `MapWithState` -
* `Batch(max_events, timeout)` - Batches multiple events and emits one event, containing all the batch data, when reaching `max_events` or `timeout` has passed since the first event in the batch.
* `Choice` - 
* `JoinWithV3IOTable` - 
* `JoinWithHttp` - 
* `AggregateByKey(aggregations,cache, key=None, emit_policy=EmitEveryEvent(), augmentation_fn=None)` - Aggregates data in the event according to the requested aggregates and time windows.
This step will aggregate the data into the cache object provided, for later persistence and outputs an event which is enriched with the requested aggregation features.
* `QueryAggregationByKey(aggregations,cache, key=None, emit_policy=EmitEveryEvent(), augmentation_fn=None)` - Similar to to `AggregateByKey`, but this step is for serving only and does not aggregate the event.
* `Persist(cache)` - Persists the data for the relevant key to the associated storage.

#### Output Steps
* `Complete` -  
* `Reduce` -
* `WriteToV3IOStream` - 


<a id="examples"></a>
## Usage Examples

### Using Aggregates
The following example reads user data, creates features using Storey's aggregates, persist the data to V3IO and emits the event containing the features to a V3IO Steam for further processing.

```python
from storey import build_flow, Source, Cache, V3ioDriver
from storey.aggregations import AggregateByKey, FieldAggregator, Persist
from storey.dtypes import SlidingWindows

v3io_web_api = '****'
v3io_acceess_key = '****'
cache = Cache('/bigdata/my_features', V3ioDriver(v3io_web_api, v3io_acceess_key))

def enrich(event, state):
    if 'first_activity' not in state:
        state['first_activity'] = event.time
    event.body['time_since_activity'] = (event.time - state['first_activity']).seconds
    state['last_event'] = event.time
    event.body['total_activities'] = state['total_activities'] = state.get('total_activities', 0) + 1
    return event, state

controller = build_flow([
    Source(),
    MapWithState(cache, enrich, group_by_key=True, full_event=True),
    AggregateByKey([FieldAggregator("number_of_clicks", "click", ["count"],
                                    SlidingWindows(['1h','2h', '24h'], '10m')),
                    FieldAggregator("purchases", "purchase_amount", ["avg", "min", "max"],
                                    SlidingWindows(['1h','2h', '24h'], '10m')),
                    FieldAggregator("failed_activities", "activity", ["count"],
                                    SlidingWindows(['1h'], '10m'),
                                    aggr_filter=lambda element: element['activity_status'] == 'fail'))],
                   cache),
    Persist(cache),
    WriteToV3IOStream('features_stream', webapi=v3io_web_api, access_key=v3io_acceess_key)
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
                           cache)
]).run()
```