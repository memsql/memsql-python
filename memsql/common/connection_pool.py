import _mysql, errno
import multiprocessing
import logging
from memsql.common import database

try:
    import queue
except ImportError:
    import Queue as queue

MySQLError = database.MySQLError
QUEUE_SIZE = 128

class HashableDict(dict):
    def __hash__(self):
        return hash(frozenset(self.items()))

class PoolConnectionException(IOError):
    """ This exception consolidates all connection exceptions into one thing """

    def __init__(self, errno, message, connection_key):
        IOError.__init__(self, errno, message)
        (self.host, self.port, self.user, self.password, self.db_name, self.options, self.pid) = connection_key

    def _get_message(self):
        return self.args[1]

    message = property(_get_message)

class ConnectionPool(object):
    def __init__(self):
        self.logger = logging.getLogger('memsql.connection_pool')
        self._connections = {}
        self._fairies = {}

    def connect(self, host, port, user, password, database, options=None):
        current_proc = multiprocessing.current_process()
        key = (host, port, user, password, database, HashableDict(options) if options else None, current_proc.pid)

        if key not in self._connections:
            self._connections[key] = queue.Queue(maxsize=QUEUE_SIZE)

        fairy = _PoolConnectionFairy(key, self)
        fairy.connect()
        self._fairies[fairy] = 1
        return fairy

    def checkin(self, fairy, key, conn, expire_connection=False):
        if key not in self._connections:
            self._connections[key] = queue.Queue(maxsize=QUEUE_SIZE)

        if expire_connection:
            try:
                conn.close()
            except Exception:
                self.logger.error("Could not close connection after fairy expired")
        else:
            try:
                self._connections[key].put_nowait(conn)
            except queue.Full:
                conn.close()

        if fairy in self._fairies:
            del(self._fairies[fairy])

    def close(self):
        for fairy in list(self._fairies.keys()):
            fairy.close()

        for q in self._connections.values():
            while True:
                try:
                    conn = q.get_nowait()
                    conn.close()
                except queue.Empty:
                    break

    def size(self):
        """ Returns the number of connections cached by the pool. """
        return sum(q.qsize() for q in self._connections.values()) + len(self._fairies)

class _PoolConnectionFairy(object):
    def __init__(self, key, pool):
        self._key = key
        self._pool = pool
        self._expired = False
        self._conn = None

    def expire(self):
        self._expired = True

    def close(self):
        self._pool.checkin(self, self._key, self._conn, expire_connection=self._expired)

    def connection_info(self):
        return (self._key[0], self._key[1])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __wrap_errors(self, fn, *args, **kwargs):
        def wrapped(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except IOError as e:
                if e.errno in [errno.ECONNRESET, errno.ECONNREFUSED, errno.ETIMEDOUT]:
                    # socket connection issues
                    self.__handle_connection_failure(e)
                else:
                    raise
            except _mysql.OperationalError as e:
                # _mysql specific database connect issues, internal state issues
                if self._conn is not None:
                    self.__potential_connection_failure(e)
                else:
                    self.__handle_connection_failure(e)
        return wrapped

    def __potential_connection_failure(self, e):
        """ OperationalError's are emitted by the _mysql library for
        almost every error code emitted by MySQL.  Because of this we
        verify that the error is actually a connection error before
        terminating the connection and firing off a PoolConnectionException
        """
        try:
            self._conn.query('SELECT 1')
        except (IOError, _mysql.OperationalError):
            # ok, it's actually an issue.
            self.__handle_connection_failure(e)
        else:
            # seems ok, probably programmer error
            raise _mysql.DatabaseError(*e.args)

    def __handle_connection_failure(self, e):
        # expire the connection so we don't return it to the pool accidentally
        self.expire()

        # build and raise the new consolidated exception
        message = None
        if isinstance(e, _mysql.OperationalError) or (hasattr(e, 'args') and len(e.args) >= 2):
            err_num = e.args[0]
            message = e.args[1]
        elif hasattr(e, 'errno'):
            err_num = e.errno
        else:
            err_num = errno.ECONNABORTED

        raise PoolConnectionException(err_num, message, self._key)

    ##################
    # Wrap DB Api to deal with connection issues and so on in an intelligent way

    def connect(self):
        self._conn = None
        try:
            conn = self._pool._connections[self._key].get_nowait()
            if self.__wrap_errors(conn.connected)():
                self._conn = conn
        except (queue.Empty, PoolConnectionException):
            pass

        if self._conn is None:
            (host, port, user, password, db_name, options, pid) = self._key
            _connect = self.__wrap_errors(database.connect)
            self._conn = _connect(
                host=host, port=port, user=user, password=password,
                database=db_name, options=options)

    # catchall
    def __getattr__(self, key):
        method = getattr(self._conn, key, None)
        if method is None:
            raise AttributeError('Attribute `%s` does not exist' % key)
        else:
            return self.__wrap_errors(method)
