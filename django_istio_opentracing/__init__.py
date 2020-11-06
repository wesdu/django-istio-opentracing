__name__ = "django_istio_opentracing"
from jaeger_client import Tracer, ConstSampler
from jaeger_client.reporter import NullReporter
from jaeger_client.codecs import B3Codec
from opentracing.scope_managers import ThreadLocalScopeManager
from opentracing.propagation import Format


tracer = Tracer(
    one_span_per_rpc=True,
    service_name='wk_ms',
    reporter=NullReporter(),
    sampler=ConstSampler(decision=True),
    extra_codecs={Format.HTTP_HEADERS: B3Codec()},
    scope_manager=ThreadLocalScopeManager(),
)


def get_opentracing_span_headers():
    scope = tracer.scope_manager.active
    carrier = {}
    if scope is not None:
        span = scope.span
        tracer.inject(
            span_context=span,
            format=Format.HTTP_HEADERS,
            carrier=carrier)
        for k, v in getattr(span, 'extra_headers', {}).items():
            carrier[k] = v
    return carrier
