try:
    import MySQLdb
except ImportError:
    pass
else:
    _MySQLdb_connect = MySQLdb.connect

from ._db_span import db_span
from ._const import BEGIN, COMMIT, ROLLBACK, MYSQLDB
from django_istio_opentracing import get_current_span
import wrapt


ignore_query = [
    "SELECT @@SQL_AUTO_IS_NULL",
    "SET SESSION TRANSACTION ISOLATION LEVEL",
    "SHOW FULL TABLES",
    "SELECT `django_migrations`",
]


class CursorWrapper(wrapt.ObjectProxy):
    __slots__ = ("_conn_params", "_module_name")

    def __init__(self, cursor, module_name, conn_params):
        super(CursorWrapper, self).__init__(wrapped=cursor)
        self._module_name = module_name
        self._conn_params = conn_params

    def execute(self, query, args=None):
        """Ignore part of the query"""
        for value in ignore_query:
            if str(query).startswith(value):
                return self.__wrapped__.execute(query, args)

        """Span is None is usually not a legal request operation"""
        span = get_current_span()
        if span is None:
            return self.__wrapped__.execute(query, args)

        db = self.connection
        statement = query
        try:
            if args is not None:
                if isinstance(args, dict):
                    nargs = {}
                    for key, item in args.items():
                        if isinstance(key, str):
                            key = key.encode(db.encoding)
                        nargs[key] = db.literal(item)
                    args = nargs
                else:
                    args = tuple(map(db.literal, args))
                statement = query % args
        except Exception:
            span_tag = {"event": "error"}
            with db_span(self, span=span, query=statement, span_tag=span_tag):
                return self.__wrapped__.execute(query, args)
        else:
            with db_span(self, span=span, query=statement):
                return self.__wrapped__.execute(query, args)


class ConnectionWrapper(wrapt.ObjectProxy):
    __slots__ = ("_cursor_wrapper", "_conn_params", "_module_name")

    def __init__(
        self,
        connection,
        module_name,
        conn_params,
        cursor_wrapper=CursorWrapper,
    ):
        super(ConnectionWrapper, self).__init__(wrapped=connection)
        self._cursor_wrapper = cursor_wrapper
        self._module_name = module_name
        self._conn_params = conn_params

    def cursor(self, *args, **kwargs):
        return self._cursor_wrapper(
            cursor=self.__wrapped__.cursor(*args, **kwargs),
            module_name=self._module_name,
            conn_params=self._conn_params,
        )

    def begin(self):
        span = get_current_span()
        if span is None:
            return self.__wrapped__.begin()
        with db_span(self, span=span, query=BEGIN):
            return self.__wrapped__.begin()

    def commit(self):
        span = get_current_span()
        if span is None:
            return self.__wrapped__.commit()
        with db_span(self, span=span, query=COMMIT):
            return self.__wrapped__.commit()

    def rollback(self):
        span = get_current_span()
        if span is None:
            return self.__wrapped__.rollback()
        with db_span(self, span=span, query=ROLLBACK):
            return self.__wrapped__.rollback()


class ConnectionFactory(object):
    __slots__ = ("_connect_func", "_connect_wrapper", "_module_name")

    def __init__(self, conn_func, module_name=" "):
        self._connect_func = conn_func
        self._connect_wrapper = ConnectionWrapper
        self._module_name = module_name

    def __call__(self, *args, **kwargs):
        safe_kwargs = kwargs
        if "passwd" in kwargs or "password" in kwargs or "conv" in kwargs:
            safe_kwargs = dict(kwargs)
            if "passwd" in safe_kwargs:
                del safe_kwargs["passwd"]
            if "password" in safe_kwargs:
                del safe_kwargs["password"]
            if "conv" in safe_kwargs:
                del safe_kwargs["conv"]
        conn_params = (
            dict(args=args, safe_kwargs=safe_kwargs)
            if args or safe_kwargs
            else None
        )
        return self._connect_wrapper(
            connection=self._connect_func(*args, **kwargs),
            module_name=self._module_name,
            conn_params=conn_params,
        )


factory = ConnectionFactory(
    conn_func=_MySQLdb_connect,
    module_name=MYSQLDB,
)


def install_patch():
    if "_MySQLdb_connect" not in globals():
        raise Exception("MySQLdb install fail")

    MySQLdb.connect = factory
    if hasattr(MySQLdb, "Connect"):
        MySQLdb.Connect = factory
