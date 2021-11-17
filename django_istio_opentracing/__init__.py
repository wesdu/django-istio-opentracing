__name__ = "django_istio_opentracing"
from opentracing.scope_managers import ThreadLocalScopeManager
from opentracing.propagation import Format
from jaeger_client import Config
import os

# deployment.yaml will set the following env for each service
project_name = os.getenv("PROJECT_NAME", "PROJECT_NAME")
namespace = os.getenv("NAMESPACE", "NAMESPACE")

config = Config(
    config={
        "sampler": {"type": "const", "param": 1},
        "logging": False,
        "reporter_queue_size": 2000,
        "propagation": "b3",  # Compatible with istio
        "generate_128bit_trace_id": True,  # Compatible with istio
    },
    service_name=f"{project_name}.{namespace}",
    scope_manager=ThreadLocalScopeManager(),
    validate=True,
)

tracer = config.initialize_tracer()


def get_opentracing_span_headers():
    scope = tracer.scope_manager.active
    carrier = {}
    if scope is not None:
        span = scope.span
        tracer.inject(
            span_context=span, format=Format.HTTP_HEADERS, carrier=carrier
        )
        for k, v in getattr(span, "extra_headers", {}).items():
            carrier[k] = v
    return carrier


def get_current_span():
    scope = tracer.scope_manager.active
    if scope is not None:
        return scope.span
    return None
