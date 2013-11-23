from memsql.common.connection_pool import ConnectionPool, PoolConnectionException
from memsql.common import errorcodes
from memsql.common.database import DatabaseError
from wraptor.decorators import memoize
import threading
import random
import logging

class RandomAggregatorPool(object):
    """ A automatic fail-over connection pool.

    One layer above the connection pool. It's purpose is to choose a
    random aggregator and use it while it is available. If not it fails
    over to another aggregator.  The class maintains the list of
    aggregators by periodically calling `SHOW AGGREGATORS`.

    Note: If you point this class at a MemSQL Singlebox instance, it
    will still work, but all connections will just be made to the
    singlebox node.
    """

    def __init__(self, host, port, user='root', password='', database='information_schema'):
        """ Initialize the RandomAggregatorPool with connection
        information for an aggregator in a MemSQL Distributed System.

        All aggregator connections will share the same user/password/database.
        """
        self.logger = logging.getLogger('memsql.random_aggregator_pool')
        self._pool = ConnectionPool()
        self._refresh_aggregator_list = memoize(30)(self._update_aggregator_list)
        self._lock = threading.RLock()

        self._primary_aggregator = (host, port)
        self._user = user
        self._password = password
        self._database = database
        self._aggregators = []
        self._aggregator = None
        self._master_aggregator = None

    def connect(self):
        """ Returns an aggregator connection, and periodically updates the aggregator list. """
        conn = self._connect()
        self._refresh_aggregator_list(conn)
        return conn

    def connect_master(self):
        if self._master_aggregator is None:
            with self._pool_connect(self._primary_aggregator) as conn:
                self._update_aggregator_list(conn)
                conn.expire()
        try:
            return self._pool_connect(self._master_aggregator)
        except PoolConnectionException:
            return None

    def close(self):
        self._pool.close()

    def _pool_connect(self, agg):
        """ `agg` should be (host, port)
            Returns a live connection from the connection pool
        """
        return self._pool.connect(agg[0], agg[1], self._user, self._password, self._database)

    def _connect(self):
        """ Returns an aggregator connection. """
        with self._lock:
            if self._aggregator:
                try:
                    return self._pool_connect(self._aggregator)
                except PoolConnectionException:
                    self._aggregator = None

            if not len(self._aggregators):
                with self._pool_connect(self._primary_aggregator) as conn:
                    self._update_aggregator_list(conn)
                    conn.expire()

            random.shuffle(self._aggregators)

            last_exception = None
            for aggregator in self._aggregators:
                self.logger.debug('Attempting connection with %s:%s' % (aggregator[0], aggregator[1]))

                try:
                    conn = self._pool_connect(aggregator)
                    # connection successful!
                    self._aggregator = aggregator
                    return conn
                except PoolConnectionException as e:
                    # connection error
                    last_exception = e
            else:
                # bad news bears...  try again later
                self._aggregator = None
                self._aggregators = []

                raise last_exception

    def _update_aggregator_list(self, conn):
        try:
            rows = conn.query('SHOW AGGREGATORS')
        except DatabaseError as e:
            if e.args[0] == errorcodes.ER_DISTRIBUTED_NOT_AGGREGATOR:
                # connected to memsql singlebox
                self._aggregators = [self._primary_aggregator]
                self._master_aggregator = self._primary_aggregator
            else:
                raise
        else:
            with self._lock:
                self._aggregators = []
                for row in rows:
                    if row.Host == '127.0.0.1':
                        # this is the aggregator we are connecting to
                        row['Host'] = conn.connection_info()[0]
                    if int(row.Master_Aggregator) == 1:
                        self._master_aggregator = (row.Host, row.Port)
                    self._aggregators.append((row.Host, row.Port))

            assert len(self._aggregators) > 0, "Failed to retrieve a list of aggregators"

        self.logger.debug('Aggregator list is updated to %s. Current aggregator is %s.' % (self._aggregators, self._aggregator))
