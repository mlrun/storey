import json
import os
from time import sleep

import pytest

from storey import SyncEmitSource, build_flow, Event
from storey.targets import KafkaTarget

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
topic = "test_kafka_integration"

if bootstrap_servers:
    import kafka


@pytest.fixture()
def kafka_topic_setup_teardown():
    # Setup
    kafka_admin_client = kafka.KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    kafka_consumer = kafka.KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')
    try:
        kafka_admin_client.delete_topics([topic])
        sleep(1)
    except kafka.errors.UnknownTopicOrPartitionError:
        pass
    kafka_admin_client.create_topics([kafka.admin.NewTopic(topic, 1, 1)])

    # Test runs
    yield kafka_consumer

    # Teardown
    kafka_admin_client.delete_topics([topic])
    kafka_admin_client.close()
    kafka_consumer.close()


@pytest.mark.skipif(not bootstrap_servers, reason='KAFKA_BOOTSTRAP_SERVERS must be defined to run kafka tests')
def test_kafka_target(kafka_topic_setup_teardown):
    kafka_consumer = kafka_topic_setup_teardown

    controller = build_flow([
        SyncEmitSource(),
        KafkaTarget(bootstrap_servers, topic, sharding_func=lambda event: 0, shard_count=1)
    ]).run()
    events = []
    for i in range(100):
        key = None
        if i > 0:
            key = f'key{i}'
        event = Event({'hello': i}, key)
        events.append(event)
        controller.emit(event)

    controller.terminate()
    controller.await_termination()

    kafka_consumer.subscribe([topic])
    for event in events:
        record = next(kafka_consumer)
        if event.key is None:
            if event.key is None:
                assert record.key is None
            else:
                assert record.key.decode('UTF-8') == event.key
        assert record.value.decode('UTF-8') == json.dumps(event.body)
