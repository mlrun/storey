Storey examples
============================

Example showing aggregation by key

.. code-block:: python

    table = Table(setup_teardown_test, V3ioDriver(), partitioned_by_key=partitioned_by_key)

    controller = build_flow([
        Source(),
        AggregateByKey([FieldAggregator("number_of_stuff", "col1", ["sum", "avg", "min", "max", "sqr"],
                                        SlidingWindows(['1h', '2h', '24h'], '10m'))],
                       table),
        NoSqlTarget(table),
        Reduce([], lambda acc, x: append_return(acc, x)),
    ]).run()

    items_in_ingest_batch = 10
    for i in range(items_in_ingest_batch):
        data = {'col1': i}
        controller.emit(data, 'tal', test_base_time + timedelta(minutes=25 * i))

    controller.terminate()
    result = controller.await_termination()


Example showing join with V3IO table:

.. code-block:: python

    table_path = "path_to_table"
    controller = build_flow([
        Source(),
        Map(lambda x: x + 1),
        Filter(lambda x: x < 8),
        JoinWithV3IOTable(V3ioDriver(), lambda x: x, lambda x, y: y['age'], table_path),
        Reduce(0, lambda x, y: x + y)
    ]).run()
    for i in range(10):
        controller.emit(i)

    controller.terminate()
    result = controller.await_termination()


