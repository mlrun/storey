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
import re
import sys
from typing import Literal, Optional

from storey.flow import Flow, _termination_obj

_OTEL_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_./-]{0,254}$")
_SUPPORTED_INSTRUMENT_TYPES = frozenset({"gauge", "counter", "updown_counter", "histogram"})


def _validate_otel_metric_name(name: str) -> None:
    if not _OTEL_NAME_RE.match(name):
        raise ValueError(
            f"Invalid OTel metric name {name!r}: must start with a letter and contain only "
            "letters, digits, underscores, dots, hyphens, or forward slashes (max 255 chars)."
        )


try:
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
        OTLPMetricExporter,
    )
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
except ImportError:
    OTLPMetricExporter = None
    MeterProvider = None
    PeriodicExportingMetricReader = None


def _flush_and_shutdown(provider) -> None:
    """Blocking flush + shutdown — always run via run_in_executor."""
    try:
        provider.force_flush(timeout_millis=10_000)
        provider.shutdown(timeout_millis=30_000)
    except Exception:
        pass


class OTelMetricsExporter(Flow):
    """Pass-through Storey step that exports OTel metrics as a side-effect.

    Each event is forwarded downstream unchanged after the metric is recorded.
    Export is best-effort: the OTel SDK handles delivery, and a failed export
    cycle does not block or retry the event — this matches the standard OTel
    contract used by every other SDK integration.

    Concurrency is handled entirely by the OTel SDK. All instrument types are
    internally thread-safe; no additional locking is needed in this step.

    All four OTel synchronous instrument types are supported:

    - ``"gauge"`` *(default)* — last-value semantics (e.g. current CPU usage).
    - ``"counter"`` — monotonically increasing sum (e.g. request count).
    - ``"updown_counter"`` — sum that can decrease (e.g. active connections).
    - ``"histogram"`` — value distribution (e.g. request latency).

    The instrument type can be set globally via ``instrument_type``, or
    overridden per-metric via a field in the metric dict (default key ``"type"``).

    Single metric per event (default field names)::

        {"metric_name": "request.count", "value": 1.0, "attributes": {"endpoint": "/predict"}}

    Multiple metrics per event, mixed types::

        {"metrics": [
            {"metric_name": "latency.p99",    "value": 0.12, "type": "histogram",      "attributes": {}},
            {"metric_name": "request.count",  "value": 1.0,  "type": "counter",        "attributes": {}},
            {"metric_name": "model.drift",    "value": 0.03, "type": "gauge",          "attributes": {}},
        ]}

    :param endpoint: OTLP gRPC endpoint URL (e.g. ``"localhost:4317"``).
    :param headers: Optional dict of gRPC metadata headers (e.g. auth tokens).
    :param export_interval_millis: Export period for "periodic" flush mode. Default 60_000 (60 s).
        Ignored in "immediate" mode.
    :param insecure: Use a plaintext (non-TLS) gRPC channel. Default False.
    :param flush_mode: ``"periodic"`` — background timer exports at ``export_interval_millis``
        cadence; ``"immediate"`` — every event triggers a synchronous flush. Default "periodic".
    :param instrument_type: Default instrument type. One of ``"gauge"``, ``"counter"``,
        ``"updown_counter"``, ``"histogram"``. Default ``"gauge"``.
    :param instrument_type_field: Event body field that overrides ``instrument_type`` per metric.
        Default ``"type"``. Absent → falls back to ``instrument_type``.
    :param max_instruments: Cap on distinct metric names; raises ValueError when exceeded.
        Default 100.
    :param metric_name_field: Event body field containing the metric name. Default ``"metric_name"``.
        Names must start with a letter and contain only letters, digits, ``_``, ``.``, ``-``,
        or ``/`` (max 255 chars); invalid names raise ``ValueError``.
    :param value_field: Event body field containing the numeric value. Default ``"value"``.
    :param attribute_fields: List of event body field names to use as OTel attributes.
        If None (default), reads a dict from ``event.body["attributes"]``.
    :param metrics_field: Key that holds a list of metric dicts for multi-metric events.
        Default ``"metrics"``. If absent from event body, single-metric mode is used.
    """

    def __init__(
        self,
        endpoint: str,
        headers: Optional[dict] = None,
        export_interval_millis: int = 60_000,
        insecure: bool = False,
        flush_mode: Literal["periodic", "immediate"] = "periodic",
        instrument_type: Literal["gauge", "counter", "updown_counter", "histogram"] = "gauge",
        instrument_type_field: str = "type",
        max_instruments: int = 100,
        metric_name_field: str = "metric_name",
        value_field: str = "value",
        attribute_fields: Optional[list] = None,
        metrics_field: str = "metrics",
        **kwargs,
    ):
        super().__init__(**kwargs)
        if flush_mode not in ("periodic", "immediate"):
            raise ValueError(
                f"OTelMetricsExporter: flush_mode {flush_mode!r} is not supported. Use 'periodic' or 'immediate'."
            )
        if instrument_type not in _SUPPORTED_INSTRUMENT_TYPES:
            raise ValueError(
                f"OTelMetricsExporter: instrument_type {instrument_type!r} is not supported. "
                f"Use one of {sorted(_SUPPORTED_INSTRUMENT_TYPES)}."
            )
        if export_interval_millis <= 0:
            raise ValueError(
                f"OTelMetricsExporter: export_interval_millis must be positive, got {export_interval_millis}."
            )
        if max_instruments <= 0:
            raise ValueError(f"OTelMetricsExporter: max_instruments must be positive, got {max_instruments}.")
        self._endpoint = endpoint
        self._headers = headers or {}
        self._export_interval_millis = export_interval_millis
        self._insecure = insecure
        self._flush_mode = flush_mode
        self._instrument_type = instrument_type
        self._instrument_type_field = instrument_type_field
        self._max_instruments = max_instruments
        self._metric_name_field = metric_name_field
        self._value_field = value_field
        self._attribute_fields = attribute_fields
        self._metrics_field = metrics_field
        self._provider = None
        self._meter = None
        # Accessed only from the asyncio event loop thread — no lock needed.
        # OTel SDK instruments are internally thread-safe for add()/record()/set() calls.
        self._instruments: dict[str, tuple[object, str]] = {}

    def _extract_attributes(self, item: dict) -> dict:
        if self._attribute_fields is not None:
            return {f: item[f] for f in self._attribute_fields if f in item}
        return item.get("attributes", {})

    def _register_instrument(self, name: str, instrument_type: str) -> None:
        if instrument_type not in _SUPPORTED_INSTRUMENT_TYPES:
            raise ValueError(
                f"OTelMetricsExporter: instrument_type {instrument_type!r} is not supported. "
                f"Use one of {sorted(_SUPPORTED_INSTRUMENT_TYPES)}."
            )
        if len(self._instruments) >= self._max_instruments:
            raise ValueError(f"OTelMetricsExporter: exceeded max_instruments={self._max_instruments}.")
        _validate_otel_metric_name(name)
        if instrument_type == "gauge":
            inst = self._meter.create_gauge(name=name)
        elif instrument_type == "counter":
            inst = self._meter.create_counter(name=name)
        elif instrument_type == "updown_counter":
            inst = self._meter.create_up_down_counter(name=name)
        else:  # histogram
            inst = self._meter.create_histogram(name=name)
        self._instruments[name] = (inst, instrument_type)

    async def _lazy_init(self):
        if self._provider is not None:
            return
        if OTLPMetricExporter is None:
            raise ImportError("Install with: pip install storey[otel]")
        interval = sys.maxsize if self._flush_mode == "immediate" else self._export_interval_millis
        reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(
                endpoint=self._endpoint,
                headers=self._headers,
                insecure=self._insecure,
            ),
            export_interval_millis=interval,
        )
        self._provider = MeterProvider(metric_readers=[reader])
        self._meter = self._provider.get_meter("storey")

    async def _do(self, event):
        if event is _termination_obj:
            if self._provider:
                await asyncio.get_running_loop().run_in_executor(None, _flush_and_shutdown, self._provider)
            return await self._do_downstream(_termination_obj)

        await self._lazy_init()

        body = event.body
        items = body[self._metrics_field] if self._metrics_field in body else [body]

        for item in items:
            name = item[self._metric_name_field]
            value = float(item[self._value_field])
            attrs = self._extract_attributes(item)
            itype = item.get(self._instrument_type_field, self._instrument_type)

            if name not in self._instruments:
                self._register_instrument(name, itype)
            else:
                _, existing_type = self._instruments[name]
                if existing_type != itype:
                    raise ValueError(
                        f"OTelMetricsExporter: metric {name!r} already registered as {existing_type!r}, "
                        f"cannot re-register as {itype!r}."
                    )

            instrument, _ = self._instruments[name]
            if itype == "gauge":
                instrument.set(value, attrs)
            elif itype == "histogram":
                instrument.record(value, attrs)
            else:  # counter, updown_counter
                instrument.add(value, attrs)

        if self._flush_mode == "immediate":
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: self._provider.force_flush(timeout_millis=5_000)
            )

        return await self._do_downstream(event)
