from memsql.common.connection_pool import ConnectionPool, PoolConnectionException
from wraptor.decorators import memoize
import threading
import random
import logging

class RandomAggregatorPool:
    """ A automatic fail-over connection pool.

    One layer above the connection pool. It's purpose is to choose a
    random aggregator and use it while it is available. If not it fails
    over to another aggregator.  The class maintains the list of
    aggregators by periodically calling `SHOW AGGREGATORS`.
    """

    def __init__(self):
        self.logger = logging.getLogger('memsql.random_aggregator_pool')
        self._pool = ConnectionPool()
        self._aggregators = []
        self._aggregator = None
        self._refresh_aggregator_list = memoize(30)(self._update_aggregator_list)
        self._lock = threading.Lock()

    def connect(self, host, port, user, password, database):
        conn = self._connect(host, port, user, password, database)
        self._refresh_aggregator_list(conn)
        return conn

    def _connect(self, host, port, user, password, database):
        if self._aggregator:
            try:
                return self._pool.connect(self._aggregator[0], self._aggregator[1], user, password, database)
            except PoolConnectionException:
                self._aggregator = None
                pass

        if not self._aggregators:
            with self._pool.connect(host, port, user, password, database) as conn:
                self._update_aggregator_list(conn)
                conn.expire()

        assert self._aggregators, "Failed to retrieve a list of aggregators"

        # we will run a few attempts of connecting to an
        # aggregator
        last_exception = None
        for i in range(10):
            with self._lock:
                self._aggregator = random.choice(self._aggregators)

            self.logger.debug('Connecting to %s:%s' % (self._aggregator[0], self._aggregator[1]))

            try:
                return self._pool.connect(self._aggregator[0], self._aggregator[1], user, password, database)
            except PoolConnectionException as e:
                last_exception = e
        else:
            with self._lock:
                self._aggregator = None
                self._aggregators = []

            raise last_exception

    def _update_aggregator_list(self, conn):
        with self._lock:
            self._aggregators = [(r['Host'], r['Port']) for r in conn.query('SHOW AGGREGATORS')]
            self.logger.debug('Aggregator list is updated to %s. Current aggregator is %s.' % (self._aggregators, self._aggregator))
