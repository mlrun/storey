from storey import build_flow, SyncEmitSource, Table, V3ioDriver, AggregateByKey, FieldAggregator, NoSqlTarget, \
    MapWithState, StreamTarget
from storey.drivers import RedisDriver
from storey.dtypes import SlidingWindows

driver = RedisDriver()
table_object = Table('/test/my_features', driver)


def enrich(event, state):
    if 'first_activity' not in state:
        state['first_activity'] = event.time
    event.body['time_since_activity'] = (event.time - state['first_activity']).seconds
    state['last_event'] = event.time
    event.body['total_activities'] = state['total_activities'] = state.get('total_activities', 0) + 1
    return event, state


if __name__ == '__main__':
    controller = build_flow([
        SyncEmitSource(),
        MapWithState(table_object, enrich, group_by_key=True, full_event=True),
        AggregateByKey([FieldAggregator("number_of_clicks", "click", ["count"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m')),
                        FieldAggregator("purchases", "purchase_amount", ["avg", "min", "max"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m')),
                        FieldAggregator("failed_activities", "activity", ["count"],
                                        SlidingWindows(['1h'], '10m'),
                                        aggr_filter=lambda element: element['activity_status'] == 'fail')],
                       table_object),
        NoSqlTarget(table_object)
    ]).run()

    controller.emit({'clicks': 1, 'purchase_amount': 1.99, 'activity': 1, 'activity_status': 'success'}, 'key')
    controller.emit({'clicks': 1, 'purchase_amount': 3.99, 'activity': 1, 'activity_status': 'success'}, 'key')
    controller.emit({'clicks': 1, 'purchase_amount': 10.99, 'activity': 1, 'activity_status': 'fail'}, 'key')
    controller.terminate()
    termination_result = controller.await_termination()
    print(termination_result)
