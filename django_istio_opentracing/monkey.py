from . import get_opentracing_span_headers
try:
    import requests.adapters
except ImportError:
    pass
else:
    _HTTPAdapter_send = requests.adapters.HTTPAdapter.send


def requests_send_wrapper(http_adapter, request, **kwargs):
    """Wraps HTTPAdapter.send"""
    headers = get_opentracing_span_headers()
    for k, v in headers.items():
        request.headers[k] = v
    response = _HTTPAdapter_send(http_adapter, request, **kwargs)
    return response


def patch_requests():
    if '_HTTPAdapter_send' not in globals():
        raise Exception("requests not installed.")
    requests.adapters.HTTPAdapter.send = requests_send_wrapper
