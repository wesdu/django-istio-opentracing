try:
    import MySQLdb
except ImportError:
    pass
else:
    _MySQLdb_connect = MySQLdb.connect

from ._db_span import db_span
from ._const import BEGIN, COMMIT, ROLLBACK, MYSQLDB

import wrapt

try:
    import ujson as json
except Exception:
    import json

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

    def execute(self, query, args=()):
        for value in ignore_query:
            if str(query).startswith(value):
                if args:
                    return self.__wrapped__.execute(query, args)
                else:
                    return self.__wrapped__.execute(query)
        try:
            statement = query % args
        except Exception as e:
            print(f"opentracing-error {repr(e)}")
            if args:
                statement = json.dumps(
                    dict(query=str(query), args=list(map(str, args)))
                )
                with db_span(
                    self, query=statement, db_instance=self._module_name
                ):
                    return self.__wrapped__.execute(query, args)
            else:
                statement = json.dumps(dict(query=str(query)))
                with db_span(
                    self, query=statement, db_instance=self._module_name
                ):
                    return self.__wrapped__.execute(query)
        else:
            with db_span(self, query=statement, db_instance=self._module_name):
                return self.__wrapped__.execute(query, args)

    def executemany(self, query, args=()):
        if args:
            statement = json.dumps(
                dict(query=str(query), args=list(map(str, args)))
            )
        else:
            statement = json.dumps(dict(query=str(query)))
        with db_span(self, query=statement, db_instance=self._module_name):
            return self.__wrapped__.executemany(query, args)

    def callproc(self, procname, args=()):
        return self.__wrapped__.callproc(procname, args)


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
        with db_span(self, query=BEGIN, db_instance=self._module_name):
            return self.__wrapped__.begin()

    def commit(self):
        with db_span(self, query=COMMIT, db_instance=self._module_name):
            return self.__wrapped__.commit()

    def rollback(self):
        with db_span(self, query=ROLLBACK, db_instance=self._module_name):
            return self.__wrapped__.rollback()


class ConnectionFactory(object):
    __slots__ = ("_connect_func", "_connect_wrapper", "_module_name")

    def __init__(self, conn_func, module_name):
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
