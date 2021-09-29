try:
    import redis
except ImportError:
    pass
else:
    _execute_command = redis.Redis.execute_command
    _pipeline = redis.Redis.pipeline
    _pipe_execute_command = redis.client.Pipeline.execute_command
    _pipe_execute = redis.client.Pipeline.execute

from ._db_span import redis_span
from ._const import REDIS
from django_istio_opentracing import get_current_span
from django_istio_opentracing import tracer

try:
    import ujson as json
except Exception:
    import json

items = {
    '_execute_command',
    '_pipeline',
    '_pipe_execute_command',
    '_pipe_execute'
}


def execute_command_wrapper(self, *args, **options):
    span = get_current_span()
    if span is None:
        return _execute_command(self, *args, **options)
    try:
        cmd = str(args[0])
        statement = json.dumps(dict(cmd=cmd, args=list(map(str, args))))
    except Exception as e:
        print(f"opentracing-error {repr(e)}")
        return _execute_command(self, *args, **options)
    else:
        with redis_span(
            self, span, operation=cmd, statement=statement, db_instance=REDIS
        ):
            return _execute_command(self, *args, **options)


def pipeline_wrapper(self, transaction=True, shard_hint=None):
    pipe = _pipeline(self, transaction, shard_hint)
    span = get_current_span()
    if span is None:
        return pipe
    pipe._span = tracer.start_span(operation_name="PIPELINE", child_of=span)
    return pipe


def pipe_execute_command_wrapper(self, *args, **kwargs):
    cmd = str(args[0])
    statement = json.dumps(dict(cmd=cmd, args=list(map(str, args))))
    if hasattr(self, "_span"):
        with redis_span(
            self,
            self._span,
            operation=cmd,
            statement=statement,
            db_instance=REDIS,
        ):
            return _pipe_execute_command(self, *args, **kwargs)
    return _pipe_execute_command(self, *args, **kwargs)


def pipe_execute_wrapper(self, raise_on_error=True):
    res = _pipe_execute(self, raise_on_error)
    if hasattr(self, "_span"):
        if self._span:
            self._span.finish()
    return res


def install_patch():
    if any(item not in globals() for item in items):
        raise Exception("redis install fail")

    redis.Redis.execute_command = execute_command_wrapper
    redis.Redis.pipeline = pipeline_wrapper
    redis.client.Pipeline.execute_command = pipe_execute_command_wrapper
    redis.client.Pipeline.execute = pipe_execute_wrapper
