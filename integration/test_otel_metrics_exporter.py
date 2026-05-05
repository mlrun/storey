# Copyright 2026 Iguazio
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
# Self-contained: uses an embedded in-process OTLP gRPC receiver.
# No external collector or Docker required.
# Run with: pytest integration/test_otel_metrics_exporter.py

import asyncio
from concurrent import futures

import grpc
import pytest

pytest.importorskip("opentelemetry.sdk.metrics")

from opentelemetry.proto.collector.metrics.v1 import (  # noqa: E402
    metrics_service_pb2,
    metrics_service_pb2_grpc,
)

from storey import AsyncEmitSource, Event, build_flow  # noqa: E402
from storey.otel_metrics_exporter import OTelMetricsExporter  # noqa: E402

_PORT = 14317
_ENDPOINT = f"localhost:{_PORT}"


# ─── Embedded receiver ────────────────────────────────────────────────────────


class _CapturingMetricsServicer(metrics_service_pb2_grpc.MetricsServiceServicer):
    def __init__(self):
        self.requests = []
        self.metadata_per_call = []

    def Export(self, request, context):
        self.requests.append(request)
        self.metadata_per_call.append(dict(context.invocation_metadata()))
        return metrics_service_pb2.ExportMetricsServiceResponse()

    def metric_names(self):
        return {
            metric.name
            for req in self.requests
            for rm in req.resource_metrics
            for sm in rm.scope_metrics
            for metric in sm.metrics
        }

    def data_points(self, metric_name):
        """Return (value, attrs_dict) for all data points of a given metric (any type)."""
        result = []
        for req in self.requests:
            for rm in req.resource_metrics:
                for sm in rm.scope_metrics:
                    for metric in sm.metrics:
                        if metric.name != metric_name:
                            continue
                        for dp in metric.gauge.data_points:
                            attrs = {kv.key: kv.value.string_value for kv in dp.attributes}
                            result.append((dp.as_double, attrs))
                        for dp in metric.sum.data_points:
                            attrs = {kv.key: kv.value.string_value for kv in dp.attributes}
                            result.append((dp.as_double, attrs))
                        for dp in metric.histogram.data_points:
                            attrs = {kv.key: kv.value.string_value for kv in dp.attributes}
                            result.append((dp.sum, attrs))
        return result


@pytest.fixture()
def otel_receiver():
    """In-process OTLP gRPC receiver; fresh servicer per test."""
    servicer = _CapturingMetricsServicer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    metrics_service_pb2_grpc.add_MetricsServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{_PORT}")
    server.start()
    yield servicer
    server.stop(grace=1)


# ─── Tests ────────────────────────────────────────────────────────────────────


def test_e2e_periodic_export(otel_receiver):
    """N events in periodic mode are received by the embedded collector."""
    asyncio.run(_e2e_periodic_export())
    assert "e2e.gauge" in otel_receiver.metric_names()


async def _e2e_periodic_export():
    controller = build_flow([
        AsyncEmitSource(),
        OTelMetricsExporter(endpoint=_ENDPOINT, insecure=True, flush_mode="periodic", export_interval_millis=500),
    ]).run()

    for i in range(5):
        await controller.emit(Event({"metric_name": "e2e.gauge", "value": float(i), "attributes": {}}))

    await asyncio.sleep(1.5)
    await controller.terminate()
    await controller.await_termination()


def test_e2e_immediate_export(otel_receiver):
    """Immediate mode: correct value and attributes reach the collector."""
    asyncio.run(_e2e_immediate_export())
    assert "e2e.immediate" in otel_receiver.metric_names()
    points = otel_receiver.data_points("e2e.immediate")
    assert any(v == 42.0 for v, _ in points), f"Expected value 42.0, got {points}"
    assert any(a.get("env") == "test" for _, a in points), f"Expected env=test, got {points}"


async def _e2e_immediate_export():
    controller = build_flow([
        AsyncEmitSource(),
        OTelMetricsExporter(endpoint=_ENDPOINT, insecure=True, flush_mode="immediate"),
    ]).run()

    await controller.emit(Event({"metric_name": "e2e.immediate", "value": 42.0, "attributes": {"env": "test"}}))
    await controller.terminate()
    await controller.await_termination()


def test_e2e_custom_headers(otel_receiver):
    """Custom auth headers are forwarded as gRPC metadata."""
    asyncio.run(_e2e_custom_headers())
    received = {k: v for meta in otel_receiver.metadata_per_call for k, v in meta.items()}
    assert received.get("x-custom-header") == "test-value", f"Got {received}"


async def _e2e_custom_headers():
    controller = build_flow([
        AsyncEmitSource(),
        OTelMetricsExporter(
            endpoint=_ENDPOINT, insecure=True,
            headers={"x-custom-header": "test-value"},
            flush_mode="immediate",
        ),
    ]).run()

    await controller.emit(Event({"metric_name": "e2e.headers", "value": 1.0, "attributes": {}}))
    await controller.terminate()
    await controller.await_termination()


def test_e2e_multi_metric_per_event(otel_receiver):
    """Multi-metric event: both metrics arrive at the collector with correct values."""
    asyncio.run(_e2e_multi_metric_per_event())
    names = otel_receiver.metric_names()
    assert "e2e.latency" in names
    assert "e2e.throughput" in names
    assert otel_receiver.data_points("e2e.latency")[0][1] == {"endpoint_id": "ep1"}
    assert otel_receiver.data_points("e2e.throughput")[0][0] == 420.0


async def _e2e_multi_metric_per_event():
    controller = build_flow([
        AsyncEmitSource(),
        OTelMetricsExporter(endpoint=_ENDPOINT, insecure=True, flush_mode="immediate"),
    ]).run()

    await controller.emit(Event({
        "metrics": [
            {"metric_name": "e2e.latency",    "value": 0.12,  "attributes": {"endpoint_id": "ep1"}},
            {"metric_name": "e2e.throughput", "value": 420.0, "attributes": {"endpoint_id": "ep1"}},
        ]
    }))
    await controller.terminate()
    await controller.await_termination()


def test_e2e_custom_field_mapping(otel_receiver):
    """Custom metric_name_field/value_field/attribute_fields produce correct wire data."""
    asyncio.run(_e2e_custom_field_mapping())
    assert "sensor.temp" in otel_receiver.metric_names()
    points = otel_receiver.data_points("sensor.temp")
    assert any(v == 22.5 for v, _ in points), f"Expected 22.5, got {points}"
    assert any(a.get("host") == "rack-1" for _, a in points), f"Expected host=rack-1, got {points}"


async def _e2e_custom_field_mapping():
    controller = build_flow([
        AsyncEmitSource(),
        OTelMetricsExporter(
            endpoint=_ENDPOINT,
            insecure=True,
            flush_mode="immediate",
            metric_name_field="name",
            value_field="reading",
            attribute_fields=["host"],
        ),
    ]).run()

    await controller.emit(Event({"name": "sensor.temp", "reading": 22.5, "host": "rack-1"}))
    await controller.terminate()
    await controller.await_termination()


def test_e2e_mixed_types_in_one_event(otel_receiver):
    """A single event with all 4 instrument types produces data points for each on the wire."""
    asyncio.run(_e2e_mixed_types_in_one_event())
    names = otel_receiver.metric_names()
    assert "e2e.mix.gauge" in names
    assert "e2e.mix.counter" in names
    assert "e2e.mix.updown" in names
    assert "e2e.mix.hist" in names
    assert len(otel_receiver.data_points("e2e.mix.gauge")) > 0
    assert len(otel_receiver.data_points("e2e.mix.counter")) > 0
    assert len(otel_receiver.data_points("e2e.mix.updown")) > 0
    assert len(otel_receiver.data_points("e2e.mix.hist")) > 0


async def _e2e_mixed_types_in_one_event():
    controller = build_flow([
        AsyncEmitSource(),
        OTelMetricsExporter(endpoint=_ENDPOINT, insecure=True, flush_mode="immediate"),
    ]).run()

    await controller.emit(Event({
        "metrics": [
            {"metric_name": "e2e.mix.gauge",   "value": 0.75, "type": "gauge",          "attributes": {}},
            {"metric_name": "e2e.mix.counter", "value": 3.0,  "type": "counter",        "attributes": {}},
            {"metric_name": "e2e.mix.updown",  "value": -1.0, "type": "updown_counter", "attributes": {}},
            {"metric_name": "e2e.mix.hist",    "value": 0.05, "type": "histogram",      "attributes": {}},
        ]
    }))
    await controller.terminate()
    await controller.await_termination()


def test_e2e_periodic_termination_flush(otel_receiver):
    """Periodic mode: buffered events are flushed on terminate() with no sleep."""
    asyncio.run(_e2e_periodic_termination_flush())
    assert "e2e.termflush" in otel_receiver.metric_names()
    points = otel_receiver.data_points("e2e.termflush")
    assert any(v == 7.0 for v, _ in points), f"Expected 7.0, got {points}"


async def _e2e_periodic_termination_flush():
    controller = build_flow([
        AsyncEmitSource(),
        OTelMetricsExporter(
            endpoint=_ENDPOINT, insecure=True,
            flush_mode="periodic",
            export_interval_millis=600_000,
        ),
    ]).run()

    await controller.emit(Event({"metric_name": "e2e.termflush", "value": 7.0, "attributes": {}}))
    await controller.terminate()
    await controller.await_termination()


@pytest.mark.parametrize("flush_mode", ["immediate", "periodic"])
@pytest.mark.parametrize("itype,metric_name,value", [
    ("gauge",          "e2e.type.gauge",    5.0),
    ("counter",        "e2e.type.counter",  3.0),
    ("updown_counter", "e2e.type.updown",   2.0),
    ("histogram",      "e2e.type.hist",     0.05),
])
def test_e2e_all_instrument_types(otel_receiver, flush_mode, itype, metric_name, value):
    """All 4 instrument types × both flush modes reach the collector."""
    asyncio.run(_e2e_instrument_type(itype, metric_name, value, flush_mode))
    assert metric_name in otel_receiver.metric_names(), (
        f"Metric {metric_name!r} ({flush_mode}) not found; received: {otel_receiver.metric_names()}"
    )
    points = otel_receiver.data_points(metric_name)
    assert len(points) > 0, f"No data points for {metric_name} in {flush_mode} mode"


async def _e2e_instrument_type(itype, metric_name, value, flush_mode):
    kwargs = {"flush_mode": flush_mode}
    if flush_mode == "periodic":
        kwargs["export_interval_millis"] = 600_000

    controller = build_flow([
        AsyncEmitSource(),
        OTelMetricsExporter(endpoint=_ENDPOINT, insecure=True, instrument_type=itype, **kwargs),
    ]).run()

    await controller.emit(Event({"metric_name": metric_name, "value": value, "attributes": {}}))
    await controller.terminate()
    await controller.await_termination()
