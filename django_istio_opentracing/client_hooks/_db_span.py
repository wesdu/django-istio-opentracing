from opentracing.span import Span
from django_istio_opentracing import tracer
from ._const import TRANS_TAGS
import opentracing
from opentracing.ext import tags
from typing import Union


def db_span(
    self, span: Union[Span, None], query: str, span_tag={}, db_type="SQL"
) -> Span:
    """
    Span for database
    """
    statement = query.strip()
    spance_idx = statement.find(" ")
    if query in TRANS_TAGS:
        operation = query
    else:
        if spance_idx == -1:
            operation = " "
        else:
            operation = statement[0:spance_idx]

    span_tag[tags.SPAN_KIND] = tags.SPAN_KIND_RPC_CLIENT
    span_tag[tags.DATABASE_STATEMENT] = statement
    span_tag[tags.DATABASE_TYPE] = db_type
    span_tag[tags.DATABASE_INSTANCE] = self._module_name

    if self._conn_params:
        span_tag[tags.DATABASE_USER] = self._conn_params["safe_kwargs"].get(
            "user", " "
        )
        host = self._conn_params["safe_kwargs"].get("host", " ")
        port = self._conn_params["safe_kwargs"].get("port", " ")
        db = self._conn_params["safe_kwargs"].get("db", " ")
        span_tag[
            tags.PEER_ADDRESS
        ] = f"{self._module_name}://{host}:{port}/{db}"

    return start_child_span(
        operation_name=operation, tracer=tracer, parent=span, span_tag=span_tag
    )


def redis_span(
    self, span, operation, statement, db_instance, db_type="redis"
) -> Span:
    """
    Span for redis
    """
    span_tag = {tags.SPAN_KIND: tags.SPAN_KIND_RPC_CLIENT}
    span_tag[tags.DATABASE_STATEMENT] = statement
    span_tag[tags.DATABASE_TYPE] = db_type
    span_tag[tags.DATABASE_INSTANCE] = db_instance
    host, port, db, username, max_connections, = (
        " ",
        " ",
        " ",
        " ",
        " ",
    )
    try:
        if hasattr(self.connection_pool, "connection_kwargs"):
            conn_kwargs = self.connection_pool.connection_kwargs
            if conn_kwargs.get("host", None) is not None:
                host = conn_kwargs["host"]
            if conn_kwargs.get("port", None) is not None:
                port = conn_kwargs["port"]
            if conn_kwargs.get("db", None) is not None:
                db = conn_kwargs["db"]
            if conn_kwargs.get("username", None) is not None:
                username = conn_kwargs["username"]
        if hasattr(self.connection_pool, "max_connections"):
            max_connections = self.connection_pool.max_connections
    except Exception as e:
        span_tag["event"] = "error"
        print("redis_span_errors", repr(e))

    span_tag[tags.PEER_ADDRESS] = f"redis://:{host}:{port}/{db}"
    span_tag["redis.username"] = username
    span_tag["redis.max_connections"] = max_connections

    return start_child_span(
        operation_name=operation, tracer=tracer, parent=span, span_tag=span_tag
    )


def start_child_span(
    operation_name: str, tracer=None, parent=None, span_tag=None
):
    """
    Start a new span as a child of parent_span. If parent_span is None,
    start a new root span.
    :param operation_name: operation name
    :param tracer: Tracer or None (defaults to opentracing.tracer)
    :param parent: parent or None
    :param span_tag: optional tags
    :return: new span
    """
    tracer = tracer or opentracing.tracer
    return tracer.start_span(
        operation_name=operation_name, child_of=parent, tags=span_tag
    )
