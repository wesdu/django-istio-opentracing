from jaeger_client.codecs import B3Codec


class B3Codec128Bit(B3Codec):
    """B3 codec that always serializes trace IDs as 128-bit hex strings.

    Keep the base codec behavior for all other B3 headers and explicitly
    serialize X-B3-TraceId at the 128-bit width.  This is defensive for
    deployments whose Jaeger client still uses a 16-character pad width.
    """

    def inject(self, span_context, carrier):
        super(B3Codec128Bit, self).inject(span_context, carrier)
        carrier[self.trace_header] = format(
            span_context.trace_id, "x"
        ).zfill(32)
