import _mysql, errno, Queue, logging
import multiprocessing
from memsql.common import database_mysqldb

QUEUE_SIZE = 128

class PoolConnectionException(IOError):
    """ This exception consolidates all connection exceptions into one thing """

    def __init__(self, errno, message, connection_key):
        IOError.__init__(self, errno, message)
        self.message = message
        (self.host, self.port, self.user, self.password, self.db_name, self.pid) = connection_key

class ConnectionPool(object):
    def __init__(self):
        self._connections = {}
        self._fairies = {}
        self.logger = logging.getLogger('memsql.connection_pool')

    def connect(self, host, port, user, password, database):
        current_proc = multiprocessing.current_process()
        key = (host, port, user, password, database, current_proc.pid)

        if key not in self._connections:
            self._connections[key] = Queue.Queue(maxsize=QUEUE_SIZE)

        fairy = _PoolConnectionFairy(key, self)
        fairy.connect()
        self._fairies[fairy] = 1
        return fairy

    def checkin(self, fairy, key, conn, expire_connection=False):
        if key not in self._connections:
            self._connections[key] = Queue.Queue(maxsize=QUEUE_SIZE)

        if expire_connection:
            try:
                conn.close()
            except Exception:
                self.logger.error("Could not close connection after fairy expired")
        else:
            try:
                self._connections[key].put_nowait(conn)
            except Queue.Full:
                conn.close()

        if fairy in self._fairies:
            del(self._fairies[fairy])

    def close(self):
        for fairy in self._fairies.keys():
            fairy.close()
        for queue in self._connections.values():
            while True:
                try:
                    conn = queue.get_nowait()
                    conn.close()
                except Queue.Empty:
                    break

class _PoolConnectionFairy(object):
    def __init__(self, key, pool):
        self._key = key
        self._pool = pool
        self._expired = False

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
        try:
            return fn(*args, **kwargs)
        except IOError as e:
            if e.errno in [errno.ECONNRESET, errno.ECONNREFUSED, errno.ETIMEDOUT]:
                # socket connection issues
                self.__handle_connection_failure(e, *args, **kwargs)
            else:
                raise
        except _mysql.OperationalError as e:
            # _mysql specific database connect issues, internal state issues
            self.__handle_connection_failure(e, *args, **kwargs)

    def __handle_connection_failure(self, e, *args, **kwargs):
        # expire the connection so we don't return it to the pool accidentally
        self.expire()

        # build and raise the new consolidated exception
        err_num = errno.ECONNABORTED
        if hasattr(e, 'errno'):
            err_num = e.errno
        message = e.message
        if hasattr(e, 'args') and len(e.args) >= 2:
            message = e.args[1]
        raise PoolConnectionException(err_num, message, self._key)

    ##################
    ## Wrap DB Api to deal with connection issues and so on in an intelligent way

    def connect(self):
        try:
            self._conn = self._pool._connections[self._key].get_nowait()
        except Queue.Empty:
            (host, port, user, password, db_name, pid) = self._key
            self._conn = self.__wrap_errors(database_mysqldb.connect,
                host=host, port=port, user=user, password=password, database=db_name)

    def connected(self):
        return self.__wrap_errors(self._conn.connected)

    def reconnect(self):
        return self.__wrap_errors(self._conn.reconnect)

    def debug_query(self, query, *parameters):
        return self.__wrap_errors(self._conn.debug_query, query, *parameters)

    def query(self, query, *parameters):
        return self.__wrap_errors(self._conn.query, query, *parameters)

    def get(self, query, *parameters):
        return self.__wrap_errors(self._conn.get, query, *parameters)

    def execute(self, query, *parameters):
        return self.__wrap_errors(self._conn.execute, query, *parameters)

    def execute_lastrowid(self, query, *parameters):
        return self.__wrap_errors(self._conn.execute_lastrowid, query, *parameters)
