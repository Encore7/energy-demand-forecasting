from platform.common.config import get_settings

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

_METRICS_CONFIGURED = False


def setup_metrics() -> None:
    global _METRICS_CONFIGURED
    if _METRICS_CONFIGURED:
        return

    settings = get_settings()

    resource = Resource.create({"service.name": settings.otel_service_name})

    exporter = OTLPMetricExporter(
        endpoint=settings.otel_exporter_otlp_endpoint,
        insecure=True,
    )

    reader = PeriodicExportingMetricReader(exporter)

    provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(provider)

    _METRICS_CONFIGURED = True


def get_meter(name: str):
    return metrics.get_meter(name)
