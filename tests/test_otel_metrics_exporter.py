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

import asyncio
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("opentelemetry.sdk.metrics")

from opentelemetry.sdk.metrics.export import MetricExportResult  # noqa: E402

from storey import AsyncEmitSource, Event, Map, build_flow  # noqa: E402
from storey.flow import _termination_obj  # noqa: E402
from storey.otel_metrics_exporter import OTelMetricsExporter, _validate_otel_metric_name  # noqa: E402

# ─── Helpers ──────────────────────────────────────────────────────────────────


def _mock_exporter():
    m = MagicMock()
    m.export.return_value = MetricExportResult.SUCCESS
    m.force_flush.return_value = True
    m.shutdown.return_value = None
    m.preferred_temporality = {}
    m.preferred_aggregation = {}
    return m


async def _init_step(flush_mode="immediate", **kwargs):
    """Build and lazy-init OTelMetricsExporter with a mock OTLP exporter."""
    with patch("storey.otel_metrics_exporter.OTLPMetricExporter", return_value=_mock_exporter()):
        step = OTelMetricsExporter(endpoint="localhost:4317", insecure=True, flush_mode=flush_mode, **kwargs)
        await step._lazy_init()
    return step


def _make_event(metric_name="my.metric", value=1.0, attributes=None, instrument_type=None):
    body = {"metric_name": metric_name, "value": value, "attributes": attributes or {}}
    if instrument_type is not None:
        body["type"] = instrument_type
    return Event(body)


# ─── Instrument type dispatch ─────────────────────────────────────────────────


@pytest.mark.parametrize("flush_mode", ["immediate", "periodic"])
@pytest.mark.parametrize(
    "itype,method",
    [
        ("gauge", "set"),
        ("histogram", "record"),
        ("counter", "add"),
        ("updown_counter", "add"),
    ],
)
def test_instrument_method_dispatch(flush_mode, itype, method):
    asyncio.run(_instrument_method_dispatch(flush_mode, itype, method))


async def _instrument_method_dispatch(flush_mode, itype, method):
    step = await _init_step(flush_mode=flush_mode, instrument_type=itype, export_interval_millis=600_000)
    # First call registers the instrument
    await step._do(_make_event(value=1.0))
    instrument, stored_type = step._instruments["my.metric"]
    assert stored_type == itype

    # Second call hits the already-registered instrument — patch its method
    with patch.object(instrument, method) as mock_method:
        await step._do(_make_event(value=7.0))
    mock_method.assert_called_once_with(7.0, {})


def test_per_event_type_field_override():
    """instrument_type_field on the event body overrides the step-level default."""
    asyncio.run(_per_event_type_field_override())


async def _per_event_type_field_override():
    step = await _init_step(instrument_type="gauge")
    await step._do(_make_event(instrument_type="counter"))
    _, stored_type = step._instruments["my.metric"]
    assert stored_type == "counter"


def test_type_conflict_raises():
    """Registering the same metric name with a different type raises ValueError."""
    asyncio.run(_type_conflict_raises())


async def _type_conflict_raises():
    step = await _init_step(instrument_type="gauge")
    await step._do(_make_event())
    with pytest.raises(ValueError, match="already registered"):
        await step._do(_make_event(instrument_type="counter"))


def test_all_four_types_registered_from_multi_metric():
    """A single multi-metric event registers all 4 instrument types with correct stored types."""
    asyncio.run(_all_four_types_registered_from_multi_metric())


async def _all_four_types_registered_from_multi_metric():
    step = await _init_step()
    event = Event(
        {
            "metrics": [
                {"metric_name": "cpu.usage", "value": 0.75, "type": "gauge", "attributes": {}},
                {"metric_name": "req.count", "value": 1.0, "type": "counter", "attributes": {}},
                {"metric_name": "active.conn", "value": 5.0, "type": "updown_counter", "attributes": {}},
                {"metric_name": "req.latency", "value": 0.01, "type": "histogram", "attributes": {}},
            ]
        }
    )
    await step._do(event)
    assert step._instruments["cpu.usage"][1] == "gauge"
    assert step._instruments["req.count"][1] == "counter"
    assert step._instruments["active.conn"][1] == "updown_counter"
    assert step._instruments["req.latency"][1] == "histogram"


def test_mixed_types_in_one_event_correct_sdk_method():
    """A multi-metric event with all 4 types calls the correct SDK method for each."""
    asyncio.run(_mixed_types_in_one_event_correct_sdk_method())


async def _mixed_types_in_one_event_correct_sdk_method():
    step = await _init_step()
    # Register all 4 instruments with a first event
    await step._do(
        Event(
            {
                "metrics": [
                    {"metric_name": "cpu.usage", "value": 0.1, "type": "gauge", "attributes": {}},
                    {"metric_name": "req.count", "value": 1.0, "type": "counter", "attributes": {}},
                    {"metric_name": "active.conn", "value": 5.0, "type": "updown_counter", "attributes": {}},
                    {"metric_name": "req.latency", "value": 0.5, "type": "histogram", "attributes": {}},
                ]
            }
        )
    )

    gauge_inst, _ = step._instruments["cpu.usage"]
    counter_inst, _ = step._instruments["req.count"]
    updown_inst, _ = step._instruments["active.conn"]
    hist_inst, _ = step._instruments["req.latency"]

    # Second event — patch every instrument and confirm the right method is called
    with patch.object(gauge_inst, "set") as mock_set, patch.object(counter_inst, "add") as mock_add_c, patch.object(
        updown_inst, "add"
    ) as mock_add_u, patch.object(hist_inst, "record") as mock_record:
        await step._do(
            Event(
                {
                    "metrics": [
                        {"metric_name": "cpu.usage", "value": 0.75, "type": "gauge", "attributes": {}},
                        {"metric_name": "req.count", "value": 2.0, "type": "counter", "attributes": {}},
                        {"metric_name": "active.conn", "value": -1.0, "type": "updown_counter", "attributes": {}},
                        {"metric_name": "req.latency", "value": 0.12, "type": "histogram", "attributes": {}},
                    ]
                }
            )
        )

    mock_set.assert_called_once_with(0.75, {})
    mock_add_c.assert_called_once_with(2.0, {})
    mock_add_u.assert_called_once_with(-1.0, {})
    mock_record.assert_called_once_with(0.12, {})


# ─── Validation ───────────────────────────────────────────────────────────────


def test_invalid_flush_mode_raises():
    with pytest.raises(ValueError, match="flush_mode"):
        OTelMetricsExporter(endpoint="localhost:4317", flush_mode="batch")


def test_invalid_instrument_type_raises():
    with pytest.raises(ValueError, match="instrument_type"):
        OTelMetricsExporter(endpoint="localhost:4317", instrument_type="summary")


def test_max_instruments_raises():
    asyncio.run(_max_instruments_raises())


async def _max_instruments_raises():
    step = await _init_step(max_instruments=2)
    await step._do(_make_event("metric.one"))
    await step._do(_make_event("metric.two"))
    with pytest.raises(ValueError, match="max_instruments"):
        await step._do(_make_event("metric.three"))


@pytest.mark.parametrize("name", ["1starts", "has space", "", "a" * 256])
def test_invalid_metric_names(name):
    with pytest.raises(ValueError, match="Invalid OTel metric name"):
        _validate_otel_metric_name(name)


@pytest.mark.parametrize("name", ["my.metric", "req-count/v2", "A" * 255])
def test_valid_metric_names(name):
    _validate_otel_metric_name(name)


def test_invalid_metric_name_in_event_raises():
    asyncio.run(_invalid_metric_name_in_event_raises())


async def _invalid_metric_name_in_event_raises():
    step = await _init_step()
    with pytest.raises(ValueError, match="Invalid OTel metric name"):
        await step._do(Event({"metric_name": "1bad", "value": 1.0, "attributes": {}}))


# ─── Pass-through behavior ────────────────────────────────────────────────────


def test_event_passes_downstream():
    """Events are forwarded downstream unchanged after recording the metric."""
    asyncio.run(_event_passes_downstream())


async def _event_passes_downstream():
    step = await _init_step()
    received = []
    original = step._do_downstream

    async def capture(event):
        if event is not _termination_obj:
            received.append(event)
        return await original(event)

    step._do_downstream = capture
    e1 = _make_event(value=1.0)
    e2 = _make_event(value=2.0)
    await step._do(e1)
    await step._do(e2)

    assert received == [e1, e2]


def test_event_passes_downstream_in_flow():
    """End-to-end: events reach a downstream Map step."""
    asyncio.run(_event_passes_downstream_in_flow())


async def _event_passes_downstream_in_flow():
    received = []
    with patch("storey.otel_metrics_exporter.OTLPMetricExporter", return_value=_mock_exporter()):
        controller = build_flow(
            [
                AsyncEmitSource(),
                OTelMetricsExporter(endpoint="localhost:4317", insecure=True, flush_mode="immediate"),
                Map(lambda body: received.append(body) or body),
            ]
        ).run()

        for i in range(3):
            await controller.emit(Event({"metric_name": "m", "value": float(i), "attributes": {}}))
        await controller.terminate()
        await controller.await_termination()

    assert len(received) == 3


def test_same_metric_reuses_instrument():
    """Two events with the same metric_name share one instrument."""
    asyncio.run(_same_metric_reuses_instrument())


async def _same_metric_reuses_instrument():
    step = await _init_step()
    await step._do(_make_event(value=1.0))
    await step._do(_make_event(value=2.0))
    assert len(step._instruments) == 1


# ─── Termination ──────────────────────────────────────────────────────────────


def test_termination_triggers_flush_and_shutdown():
    asyncio.run(_termination_triggers_flush_and_shutdown())


async def _termination_triggers_flush_and_shutdown():
    step = await _init_step()
    provider = step._provider

    with patch("storey.otel_metrics_exporter._flush_and_shutdown") as mock_fs:
        await step._do(_termination_obj)

    mock_fs.assert_called_once_with(provider)


def test_termination_before_lazy_init():
    """_termination_obj when provider was never initialised is a no-op."""

    async def _test():
        step = OTelMetricsExporter(endpoint="localhost:4317")
        assert step._provider is None
        await step._do(_termination_obj)

    asyncio.run(_test())


# ─── Lazy init ────────────────────────────────────────────────────────────────


def test_lazy_init_idempotent():
    asyncio.run(_lazy_init_idempotent())


async def _lazy_init_idempotent():
    step = await _init_step()
    first = step._provider
    await step._lazy_init()
    assert step._provider is first


def test_import_error_when_otel_not_installed():
    with patch("storey.otel_metrics_exporter.OTLPMetricExporter", None):
        step = OTelMetricsExporter(endpoint="localhost:4317")
        with pytest.raises(ImportError, match="pip install"):
            asyncio.run(step._lazy_init())


# ─── Headers ──────────────────────────────────────────────────────────────────


def test_headers_forwarded_to_exporter():
    asyncio.run(_headers_forwarded_to_exporter())


async def _headers_forwarded_to_exporter():
    mock_cls = MagicMock(return_value=_mock_exporter())
    with patch("storey.otel_metrics_exporter.OTLPMetricExporter", mock_cls):
        step = OTelMetricsExporter(endpoint="localhost:4317", headers={"x-auth": "tok"}, insecure=True)
        await step._lazy_init()
    mock_cls.assert_called_once_with(endpoint="localhost:4317", headers={"x-auth": "tok"}, insecure=True)


# ─── Field mapping ────────────────────────────────────────────────────────────


def test_custom_metric_name_field():
    asyncio.run(_custom_metric_name_field())


async def _custom_metric_name_field():
    step = await _init_step(metric_name_field="name")
    await step._do(Event({"name": "sensor.temp", "value": 22.5, "attributes": {}}))
    assert "sensor.temp" in step._instruments


def test_custom_value_field():
    asyncio.run(_custom_value_field())


async def _custom_value_field():
    step = await _init_step(value_field="reading")
    await step._do(Event({"metric_name": "sensor.temp", "reading": 22.5, "attributes": {}}))
    assert "sensor.temp" in step._instruments


def test_attribute_fields_extracted_from_body():
    asyncio.run(_attribute_fields_extracted_from_body())


async def _attribute_fields_extracted_from_body():
    step = await _init_step(attribute_fields=["host", "env"])
    await step._do(Event({"metric_name": "cpu.load", "value": 0.5, "host": "r1", "env": "prod"}))
    instrument, _ = step._instruments["cpu.load"]
    with patch.object(instrument, "set") as mock_set:
        await step._do(Event({"metric_name": "cpu.load", "value": 0.6, "host": "r2", "env": "dev"}))
    mock_set.assert_called_once_with(0.6, {"host": "r2", "env": "dev"})


def test_default_attributes_dict():
    asyncio.run(_default_attributes_dict())


async def _default_attributes_dict():
    step = await _init_step()
    await step._do(Event({"metric_name": "cpu.load", "value": 0.5, "attributes": {"host": "r1"}}))
    instrument, _ = step._instruments["cpu.load"]
    with patch.object(instrument, "set") as mock_set:
        await step._do(Event({"metric_name": "cpu.load", "value": 0.6, "attributes": {"host": "r2"}}))
    mock_set.assert_called_once_with(0.6, {"host": "r2"})


def test_missing_attributes_key_defaults_to_empty():
    asyncio.run(_missing_attributes_key_defaults_to_empty())


async def _missing_attributes_key_defaults_to_empty():
    step = await _init_step()
    await step._do(Event({"metric_name": "cpu.load", "value": 0.5}))
    instrument, _ = step._instruments["cpu.load"]
    with patch.object(instrument, "set") as mock_set:
        await step._do(Event({"metric_name": "cpu.load", "value": 0.6}))
    mock_set.assert_called_once_with(0.6, {})


def test_attribute_fields_missing_key_silently_skipped():
    asyncio.run(_attribute_fields_missing_key_silently_skipped())


async def _attribute_fields_missing_key_silently_skipped():
    step = await _init_step(attribute_fields=["host", "region"])
    await step._do(Event({"metric_name": "cpu.load", "value": 0.5, "host": "r1"}))
    instrument, _ = step._instruments["cpu.load"]
    with patch.object(instrument, "set") as mock_set:
        await step._do(Event({"metric_name": "cpu.load", "value": 0.6, "host": "r1"}))
    mock_set.assert_called_once_with(0.6, {"host": "r1"})


# ─── Multi-metric events ──────────────────────────────────────────────────────


def test_multi_metric_per_event():
    asyncio.run(_multi_metric_per_event())


async def _multi_metric_per_event():
    step = await _init_step()
    event = Event(
        {
            "metrics": [
                {"metric_name": "latency.p99", "value": 0.12, "attributes": {}},
                {"metric_name": "throughput.rps", "value": 420.0, "attributes": {}},
            ]
        }
    )
    await step._do(event)
    assert "latency.p99" in step._instruments
    assert "throughput.rps" in step._instruments
    assert len(step._instruments) == 2


def test_multi_metric_custom_metrics_field():
    asyncio.run(_multi_metric_custom_metrics_field())


async def _multi_metric_custom_metrics_field():
    step = await _init_step(metrics_field="readings")
    event = Event(
        {
            "readings": [
                {"metric_name": "temp.cpu", "value": 72.0, "attributes": {}},
                {"metric_name": "temp.gpu", "value": 85.0, "attributes": {}},
            ]
        }
    )
    await step._do(event)
    assert "temp.cpu" in step._instruments
    assert "temp.gpu" in step._instruments
