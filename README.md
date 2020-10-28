# Django-istio-opentracing
Django opentracing middleware works with k8s and istio

install:


```
pip install django-istio-opentracing
```

example:

Add a middleware to your Django middleware.

```python
MIDDLEWARE += [
    'django_istio_opentracing.middleware.Middleware'
]
```

And if you using [requests](https://requests.readthedocs.io/en/master/),
jusing using the patch in maybe your `__init__.py` file.
**Hint**: make sure the patch line before your code.

```python
from django_istio_opentracing import monkey
monkey.patch_requests()
```

Then use [requests](https://requests.readthedocs.io/en/master/) whatever you want,
every request you make will carry the b3 code in header without extra coding.

Also you can use it directly in view:

```python
from django_istio_opentracing import get_opentracing_span_headers
def index(request):
    print(get_opentracing_span_headers())
    return HttpResponse('ok')
```
