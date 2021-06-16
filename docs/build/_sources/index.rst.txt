Storey Package Documentation
==================================
Introduction
================
.. _asyncio: https://docs.python.org/3/library/asyncio.html

Storey is streaming library for real time event processing and feature extraction. It's based on asyncio_ and offers both synchronous and
asynchronous APIs. Storey flows are graphs of steps that perform computational and IO tasks. A basic synchronous flow is created and run as
such:

.. code-block:: python

    from storey import build_flow, EmitSource, CSVTarget

    controller = build_flow([
        EmitSource(),
        CSVTarget('myfile.csv', columns=['n', 'n*10'], header=True)
    ]).run()

    for i in range(10):
        controller.emit({'n': i, 'n*10': 10 * i})

    controller.terminate()
    controller.await_termination()

This example constructs a flow that writes events to a CSV file, runs it, then pushes events into that flow.

The same example can also be run from within an async context:

.. code-block:: python

    from storey import build_flow, EmitSource, CSVTarget

    controller = build_flow([
        EmitSource(),
        CSVTarget('myfile.csv', columns=['n', 'n*10'], header=True)
    ]).run()

    for i in range(10):
        await controller.emit({'n': i, 'n*10': 10 * i})

    await controller.terminate()
    await controller.await_termination()

The following more interesting example takes a dataframe, aggregates its data using a sliding window, and persists the result to a
V3IO key-value store.

.. code-block:: python

    from storey import build_flow, DataframeSource, AggregateByKey, FieldAggregator, SlidingWindows, NoSqlTarget, V3ioDriver, Table

    table = Table(f'users/me/destination', V3ioDriver())

    controller = build_flow([
        DataframeSource(df, key_column='user_id', time_column='timestamp'),
        AggregateByKey([
            FieldAggregator("feature1", "field1", ["avg", "min", "max"],
                            SlidingWindows(['1h', '2h'], '10m')),
            FieldAggregator("feature2", "field2", ["avg", "min", "max"],
                            SlidingWindows(['1h', '2h'], '10m')),
            FieldAggregator("feature3", "field3", ["avg", "min", "max"],
                            SlidingWindows(['1h', '2h'], '10m'))
        ],
            table),
        NoSqlTarget(table, columns=['feature1', 'feature2', 'feature3']),
    ]).run()

    controller.await_termination()


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   examples
   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
