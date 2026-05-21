from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

JAEGER_ENDPOINT = "http://jaeger:4318/v1/traces"


def setup_tracing(service_name: str) -> None:
    provider = TracerProvider(
        resource=Resource(attributes={"service.name": service_name}),
    )
    exporter = OTLPSpanExporter(endpoint=JAEGER_ENDPOINT)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
