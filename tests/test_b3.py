import unittest

from jaeger_client.span_context import SpanContext
from opentracing.propagation import Format

from django_istio_opentracing import get_opentracing_span_headers, tracer
from django_istio_opentracing.b3 import B3Codec128Bit


class B3Codec128BitTest(unittest.TestCase):
    @staticmethod
    def make_context(trace_id):
        return SpanContext(
            trace_id=int(trace_id, 16),
            span_id=int("b22dc3b19e556f6b", 16),
            parent_id=None,
            flags=0,
        )

    def test_preserves_leading_zero_in_128bit_trace_id(self):
        trace_id = "0116eda0cb1685d5b98cc6d9e556b2fa"
        context = self.make_context(trace_id)
        carrier = {}

        B3Codec128Bit().inject(context, carrier)

        self.assertEqual(trace_id, carrier["X-B3-TraceId"])
        self.assertEqual("b22dc3b19e556f6b", carrier["X-B3-SpanId"])

    def test_package_tracer_uses_fixed_http_header_codec(self):
        codec = tracer.codecs[Format.HTTP_HEADERS]
        trace_id = "0116eda0cb1685d5b98cc6d9e556b2fa"

        self.assertIsInstance(codec, B3Codec128Bit)
        span = tracer.start_span(
            "test-leading-zero",
            child_of=self.make_context(trace_id),
        )
        scope = tracer.scope_manager.activate(span, True)
        try:
            carrier = get_opentracing_span_headers()
        finally:
            scope.close()

        self.assertEqual(trace_id, carrier["X-B3-TraceId"])


if __name__ == "__main__":
    unittest.main()
