"""OpenTelemetry tracing configuration.

Sets up the OTel SDK with:
- FastAPI auto-instrumentation (HTTP server spans)
- httpx auto-instrumentation (HTTP client spans, e.g. TiTiler proxy)
- OTLP/gRPC exporter to a collector
- Route-aware sampling: low rate for high-volume tile endpoints,
  full sampling for pipeline/admin operations
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional, Sequence

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_ON,
    ParentBased,
    Sampler,
    SamplingResult,
    TraceIdRatioBased,
)

if TYPE_CHECKING:
    from opentelemetry.context import Context
    from opentelemetry.trace import Link, SpanKind
    from opentelemetry.util.types import Attributes


# ---------------------------------------------------------------------------
# Custom sampler: route-aware sampling
# ---------------------------------------------------------------------------

# URL path prefixes that get reduced sampling (high-volume endpoints)
_LOW_RATE_PREFIXES: tuple[str, ...] = ("/tiles/",)


class RouteAwareSampler(Sampler):
    """Samples tile requests at a reduced rate, everything else at full rate.

    Uses the ``http.target`` or ``url.path`` span attribute (set by the
    FastAPI/ASGI instrumentor) to decide which sampler to delegate to.
    """

    def __init__(self, low_rate: float = 0.01) -> None:
        self._low = TraceIdRatioBased(low_rate)
        self._high = ALWAYS_ON

    def should_sample(
        self,
        parent_context: Optional["Context"],
        trace_id: int,
        name: str,
        kind: Optional["SpanKind"] = None,
        attributes: "Attributes" = None,
        links: Optional[Sequence["Link"]] = None,
    ) -> SamplingResult:
        path = _extract_path(attributes)
        delegate = self._low if _is_low_rate(path) else self._high
        return delegate.should_sample(
            parent_context, trace_id, name, kind, attributes, links
        )

    def get_description(self) -> str:
        return f"RouteAwareSampler(low={self._low.get_description()})"


def _extract_path(attributes: "Attributes") -> str | None:
    if not attributes:
        return None
    # OTel semantic conventions: stable is `url.path`, legacy is `http.target`
    for key in ("url.path", "http.target"):
        val = attributes.get(key)
        if val is not None:
            return str(val)
    return None


def _is_low_rate(path: str | None) -> bool:
    if path is None:
        return False
    return any(path.startswith(prefix) for prefix in _LOW_RATE_PREFIXES)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def setup_tracing(
    service_name: str = "weatherman",
    *,
    otlp_endpoint: str | None = None,
    tile_sample_rate: float | None = None,
) -> TracerProvider:
    """Initialise the OTel TracerProvider and auto-instrument FastAPI + httpx.

    Parameters
    ----------
    service_name:
        The ``service.name`` resource attribute.
    otlp_endpoint:
        OTLP/gRPC collector endpoint.  Falls back to the
        ``OTEL_EXPORTER_OTLP_ENDPOINT`` env var, then ``http://localhost:4317``.
    tile_sample_rate:
        Fraction of tile requests to sample (0.0–1.0).  Falls back to the
        ``WEATHERMAN_TILE_SAMPLE_RATE`` env var, then 0.01 (1 %).
    """
    if tile_sample_rate is None:
        tile_sample_rate = float(
            os.environ.get("WEATHERMAN_TILE_SAMPLE_RATE", "0.01")
        )

    endpoint = otlp_endpoint or os.environ.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
    )

    resource = Resource.create({"service.name": service_name})

    sampler = ParentBased(root=RouteAwareSampler(low_rate=tile_sample_rate))

    provider = TracerProvider(resource=resource, sampler=sampler)

    # OTLP/gRPC exporter — import lazily so tests can skip if collector is down
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )

    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))

    trace.set_tracer_provider(provider)

    # Auto-instrument FastAPI (ASGI) and httpx
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    FastAPIInstrumentor.instrument()
    HTTPXClientInstrumentor().instrument()

    return provider


def get_tracer(name: str = "weatherman") -> trace.Tracer:
    """Return a tracer from the global TracerProvider.

    Use this to create custom spans::

        tracer = get_tracer()
        with tracer.start_as_current_span("process_cog"):
            ...
    """
    return trace.get_tracer(name)
