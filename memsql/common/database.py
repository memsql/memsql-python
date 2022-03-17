"""A lightweight wrapper around _mysql."""

from MySQLdb import _mysql
import MySQLdb
import time
import operator

try:
    from _thread import get_ident as _get_ident
except ImportError:
    from thread import get_ident as _get_ident

from memsql.common.conversions import CONVERSIONS

MySQLError = _mysql.MySQLError
OperationalError = _mysql.OperationalError
DatabaseError = _mysql.DatabaseError

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

class Connection(object):
    """A lightweight wrapper around _mysql DB-API connections.

    The main value we provide is wrapping rows in a dict/object so that
    columns can be accessed by name. Typical usage::

        db = database.Connection("localhost", "mydatabase")
        for article in db.query("SELECT * FROM articles"):
            print article.title

    Cursors are hidden by the implementation, but other than that, the methods
    are very similar to the DB-API.

    We explicitly set the timezone to UTC and the character encoding to
    UTF-8 on all connections to avoid time zone and encoding errors.
    """

    def __init__(self, host, port=3306, database="information_schema", user=None, password=None,
                 max_idle_time=7 * 3600, _version=0, options=None):
        self.max_idle_time = max_idle_time

        args = {
            "db": database,
            "conv": CONVERSIONS
        }

        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        args["host"] = host
        args["port"] = int(port)

        if options is not None:
            assert isinstance(options, dict), "Options to database.Connection must be an dictionary of { str: value } pairs."
            args.update(options)

        # Fix for parameter name changes in mysqlclient v2.1.0
        if MySQLdb.version_info[:2] >= (2, 1):
            if "db" in args:
                args["database"] = args.pop("db")
            if "passwd" in args:
                args["password"] = args.pop("passwd")

        self._db = None
        self._db_args = args

        self._last_use_time = time.time()
        self.reconnect()
        self._db.set_character_set("utf8")

        self._version = _version

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def connected(self):
        if self._db is not None:
            try:
                self.ping()
                return True
            except _mysql.InterfaceError:
                return False
        return False

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        conn = _mysql.connect(**self._db_args)
        if conn is not None:
            self.close()
            self._db = conn

    def select_db(self, database):
        self._db.select_db(database)

        # Fix for parameter name changes in mysqlclient v2.1.0
        if MySQLdb.version_info[:2] >= (2, 1):
            self._db_args['database'] = database
        else:
            self._db_args['db'] = database

    def ping(self):
        """ Ping the server """
        return self._db.ping()

    def thread_id(self):
        """ Retrieve the thread id for the current connection """
        return self._db.thread_id()

    def debug_query(self, query, *parameters, **kwparameters):
        return self._query(query, parameters, kwparameters, debug=True)

    def query(self, query, *parameters, **kwparameters):
        """
        Query the connection and return the rows (or affected rows if not a
        select query).  Mysql errors will be propogated as exceptions.
        """
        return self._query(query, parameters, kwparameters)

    def get(self, query, *parameters, **kwparameters):
        """Returns the first row returned for the given query."""
        rows = self._query(query, parameters, kwparameters)
        if not rows:
            return None
        elif not isinstance(rows, list):
            raise MySQLError("Query is not a select query")
        elif len(rows) > 1:
            raise MySQLError("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    # rowcount is a more reasonable default return value than lastrowid,
    # but for historical compatibility execute() must return lastrowid.
    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_lastrowid(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        self._execute(query, parameters, kwparameters)
        self._result = self._db.store_result()
        return self._db.insert_id()

    def _query(self, query, parameters, kwparameters, debug=False):
        self._execute(query, parameters, kwparameters, debug)

        self._result = self._db.store_result()
        if self._result is None:
            return self._rowcount

        fields = [ f[0] for f in self._result.describe() ]
        rows = self._result.fetch_row(0)
        return SelectResult(fields, rows)

    def _execute(self, query, parameters, kwparameters, debug=False):
        if parameters and kwparameters:
            raise ValueError('database.py querying functions can receive *args or **kwargs, but not both')

        query = escape_query(query, parameters or kwparameters)
        if debug:
            print(query)

        self._ensure_connected()
        self._db.query(query)
        self._rowcount = self._db.affected_rows()

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if (self._db is None or (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()


class Row(object):
    """A fast, ordered, partially-immutable dictlike object (or objectlike dict)."""

    def __init__(self, fields, values):
        self._fields = fields
        self._values = values

    def __getattr__(self, name):
        try:
            return self._values[self._fields.index(name)]
        except (ValueError, IndexError):
            raise AttributeError(name)

    def __getitem__(self, name):
        try:
            return self._values[self._fields.index(name)]
        except (ValueError, IndexError):
            raise KeyError(name)

    def __setitem__(self, name, value):
        try:
            self._values[self._fields.index(name)] = value
        except (ValueError, IndexError):
            self._fields += (name,)
            self._values += (value,)

    def __contains__(self, name):
        return name in self._fields

    has_key = __contains__

    def __sizeof__(self, name):
        return len(self._fields)

    def __iter__(self):
        return self._fields.__iter__()

    def __len__(self):
        return self._fields.__len__()

    def get(self, name, default=None):
        try:
            return self.__getitem__(name)
        except KeyError:
            return default

    def keys(self):
        for field in iter(self._fields):
            yield field

    def values(self):
        for value in iter(self._values):
            yield value

    def items(self):
        for item in zip(self._fields, self._values):
            yield item

    def __eq__(self, other):
        if isinstance(other, Row):
            return dict.__eq__(dict(self.items()), other) and all(map(operator.eq, self, other))
        else:
            return dict.__eq__(dict(self.items()), other)

    def __ne__(self, other):
        return not self == other

    def __repr__(self, _repr_running={}):
        call_key = id(self), _get_ident()
        if call_key in _repr_running:
            return '...'
        _repr_running[call_key] = 1
        try:
            if not self:
                return '%s()' % (self.__class__.__name__,)
            return '%s(%r)' % (self.__class__.__name__, dict(self.items()))
        finally:
            del _repr_running[call_key]

    # for simplejson.dumps()
    def _asdict(self):
        return dict(self)

    def nope(self, *args, **kwargs):
        raise NotImplementedError('This object is partially immutable. To modify it, call "foo = dict(foo)" first.')

    update = nope
    pop = nope
    setdefault = nope
    fromkeys = nope
    clear = nope
    __delitem__ = nope
    __reversed__ = nope

class SelectResult(list):
    def __init__(self, fieldnames, rows):
        self.fieldnames = tuple(fieldnames)
        self.rows = rows

        data = [Row(self.fieldnames, row) for row in self.rows]
        list.__init__(self, data)

    def width(self):
        return len(self.fieldnames)

    def __getitem__(self, i):
        if isinstance(i, slice):
            return SelectResult(self.fieldnames, self.rows[i])
        return list.__getitem__(self, i)

def escape_query(query, parameters):
    if parameters:
        if isinstance(parameters, (list, tuple)):
            query = query % tuple(map(_escape, parameters))
        elif isinstance(parameters, dict):
            params = {}
            for key, val in parameters.items():
                params[key] = _escape(val)

            query = query % params
        else:
            assert False, 'not sure what to do with parameters of type %s' % type(parameters)

    return query

def _escape(param):
    def _bytes_to_utf8(b):
        return b.decode("utf-8") if isinstance(b, bytes) else b

    if isinstance(param, (list, tuple)):
        return ','.join(_bytes_to_utf8(_mysql.escape(p, CONVERSIONS)) for p in param)
    else:
        return _bytes_to_utf8(_mysql.escape(param, CONVERSIONS))
