"""A lightweight wrapper around _mysql."""

import _mysql
import itertools
import time

try:
    from collections import OrderedDict
except:
    from ordereddict import OrderedDict

from MySQLdb.converters import conversions

MySQLError = _mysql.MySQLError

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
                 max_idle_time=7 * 3600):
        self.database = database
        self.max_idle_time = max_idle_time

        sys_vars = {
            "character_set_server":  "utf8",
            "collation_server":      "utf8_general_ci"
        }
        args = {
            "db": database,
            "conv": conversions
        }

        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        args["host"] = host
        args["port"] = int(port)
        args["init_command"] = 'set names "utf8" collate "utf8_bin"' + ''.join([', @@%s = "%s"' % t for t in sys_vars.items()])

        self._db = None
        self._db_args = args
        self.encoders = dict([ (k, v) for k, v in conversions.items()
                               if type(k) is not int ])
        self._last_use_time = time.time()
        self.reconnect()
        self._db.set_character_set("utf8")

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
                self._db.ping()
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

    def debug_query(self, query, *parameters):
        print query % tuple(["'%s'" % p for p in parameters])
        return self.query(query, *parameters)

    def query(self, query, *parameters):
        """
        Query the connection and return the rows (or affected rows if not a
        select query).  Mysql errors will be propogated as exceptions.
        """
        self._execute(query, *parameters)
        self._result = self._db.use_result()
        if self._result is None:
            return self._rowcount
        fields = zip(*self._result.describe())[0]
        rows = list(self._result.fetch_row(0))
        ret = SelectResult(fields, rows)
        return ret

    def get(self, query, *parameters):
        """Returns the first row returned for the given query."""
        rows = self.query(query, *parameters)
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
    def execute(self, query, *parameters):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *parameters)

    def execute_lastrowid(self, query, *parameters):
        """Executes the given query, returning the lastrowid from the query."""
        self._execute(query, *parameters)
        self._result = self._db.store_result()
        return self._db.insert_id()

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if (self._db is None or (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _execute(self, query, *parameters):
        if parameters is not None and parameters != ():
            params = []
            for param in parameters:
                if isinstance(param, unicode):
                    params.append(param.encode(self._db.character_set_name()))
                else:
                    params.append(param)

            query = query % self._db.escape(params, self.encoders)

        if isinstance(query, unicode):
            query = query.encode(self._db.character_set_name())

        self._db.query(query)
        self._rowcount = self._db.affected_rows()

class Row(OrderedDict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

class SelectResult(list):
    def __init__(self, fieldnames, rows):
        self.fieldnames = fieldnames
        self.rows = rows

        data = [Row(itertools.izip(self.fieldnames, row)) for row in self.rows]
        list.__init__(self, data)

    def width(self):
        return len(self.fieldnames)

    def __getitem__(self, i):
        if isinstance(i, slice):
            return SelectResult(self.fieldnames, self.rows[i])
        return list.__getitem__(self, i)
