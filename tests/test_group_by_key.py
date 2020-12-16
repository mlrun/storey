from storey import build_flow, Source, Reduce, GroupByKey

import queue


def test_group_by_key():
    def append_and_return(lst, x):
        lst.append(x)
        return lst

    controller = build_flow(
        [
            Source(),
            GroupByKey("value", 4, 100),
            Reduce([], lambda acc, x: append_and_return(acc, x)),
        ]
    ).run()

    values_1 = [i for i in range(3)]
    values_2 = [i for i in range(3)]
    values_3 = [i for i in range(3)]
    for i in range(3):
        controller.emit({"value": values_1[i], "data": values_1[i]})
        controller.emit({"value": values_2[i], "data": values_2[i]})
        controller.emit({"value": values_3[i], "data": values_3[i]})

    controller.terminate()
    termination_result = controller.await_termination()

    assert termination_result == [
        [{"value": 0, "data": 0}, {"value": 0, "data": 0}, {"value": 0, "data": 0}],
        [{"value": 1, "data": 1}, {"value": 1, "data": 1}, {"value": 1, "data": 1}],
        [{"value": 2, "data": 2}, {"value": 2, "data": 2}, {"value": 2, "data": 2}],
    ]


def test_group_by_key_full_event():
    def append_body_and_return(lst, x):
        ll = []
        for item in x:
            ll.append(item.body)
        lst.append(ll)
        return lst

    controller = build_flow(
        [
            Source(),
            GroupByKey("value", 4, 100, full_event=True),
            Reduce([], lambda acc, x: append_body_and_return(acc, x)),
        ]
    ).run()

    values_1 = [i for i in range(3)]
    values_2 = [i for i in range(3)]
    values_3 = [i for i in range(3)]
    for i in range(3):
        controller.emit({"value": values_1[i], "data": values_1[i]})
        controller.emit({"value": values_2[i], "data": values_2[i]})
        controller.emit({"value": values_3[i], "data": values_3[i]})

    controller.terminate()
    termination_result = controller.await_termination()

    assert termination_result == [
        [{"value": 0, "data": 0}, {"value": 0, "data": 0}, {"value": 0, "data": 0}],
        [{"value": 1, "data": 1}, {"value": 1, "data": 1}, {"value": 1, "data": 1}],
        [{"value": 2, "data": 2}, {"value": 2, "data": 2}, {"value": 2, "data": 2}],
    ]


def test_batch_with_timeout():
    q = queue.Queue(1)

    def reduce_fn(acc, x):
        if x[0] == 0:
            q.put(None)
        acc.append(x)
        return acc

    controller = build_flow(
        [
            Source(),
            GroupByKey("value", 4, 1),
            Reduce([], reduce_fn),
        ]
    ).run()

    values_1 = [i for i in range(3)]
    values_2 = [i for i in range(3)]
    values_3 = [i for i in range(3)]
    for i in range(3):
        if i == 3:
            q.get()
        controller.emit({"value": values_1[i], "data": values_1[i]})
        controller.emit({"value": values_2[i], "data": values_2[i]})
        controller.emit({"value": values_3[i], "data": values_3[i]})

    controller.terminate()
    termination_result = controller.await_termination()

    assert termination_result == [
        [{"value": 0, "data": 0}, {"value": 0, "data": 0}, {"value": 0, "data": 0}],
        [{"value": 1, "data": 1}, {"value": 1, "data": 1}, {"value": 1, "data": 1}],
        [{"value": 2, "data": 2}, {"value": 2, "data": 2}, {"value": 2, "data": 2}],
    ]

