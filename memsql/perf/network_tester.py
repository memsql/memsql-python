from memsql.common import sql_utility, database, errorcodes
from wraptor.context import timer
import math

DATA_TABLE = """\
CREATE /*!90618 REFERENCE */ TABLE IF NOT EXISTS %(name)s (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data LONGTEXT
)"""

class NetworkTester(sql_utility.SQLUtility):
    def __init__(self, table_prefix=''):
        super(NetworkTester, self).__init__()

        self.table_prefix = table_prefix.rstrip('_')
        self.data_table = self.table_prefix + '_data'
        self._define_table(self.data_table, DATA_TABLE % { 'name': self.data_table })

    ###############################
    # Public Interface

    def estimate_latency(self, iterations=100):
        results = []
        with self._db_conn() as conn:
            for i in range(iterations):
                with timer() as t:
                    conn.ping()
                results.append(t.interval)

        computed = self._compute_results(results)
        return computed

    def estimate_throughput(self, payload_size=1024 * 100, iterations=100):
        results = []
        payload = 'a' * (payload_size / 2)
        delete = 'DELETE FROM %s' % (self.data_table)
        insert = 'INSERT INTO %s (data) VALUES ("%s")' % (self.data_table, payload)
        query = 'SELECT data AS a FROM %s LIMIT 1 # %s' % (self.data_table, payload)
        with self._db_conn() as conn:
            conn.execute(delete)  # ensure that the table is empty
            conn.execute(insert)  # ensure there is something to select
            conn.execute(query)  # ensure compile
            for i in range(iterations):
                with timer() as t:
                    conn.query(query)
                results.append(payload_size / (t.interval / 1000.))

        computed = self._compute_results(results)
        return computed

    def estimate_upload(self, payload_size=1024 * 100, iterations=100):
        results = []
        payload = 'a' * (payload_size - 2)
        query = '!!%s' % (payload)
        with self._db_conn() as conn:
            for i in range(iterations):
                try:
                    with timer() as t:
                        conn.query(query)
                except database.MySQLError as (errno, msg):
                    if errno == errorcodes.ER_PARSE_ERROR:
                        pass
                    else:
                        raise
                results.append(payload_size / (t.interval / 1000.))

        computed = self._compute_results(results)
        return computed

    def estimate_download(self, payload_size=1024 * 100, iterations=100):
        results = []
        payload = 'a' * payload_size
        delete = 'DELETE FROM %s' % (self.data_table)
        insert = 'INSERT INTO %s (data) VALUES ("%s")' % (self.data_table, payload)
        query = 'SELECT data AS a FROM %s LIMIT 1' % (self.data_table)
        with self._db_conn() as conn:
            conn.execute(delete)  # ensure that the table is empty
            conn.execute(insert)  # ensure there is something to select
            conn.execute(query)  # ensure compile
            for i in range(iterations):
                with timer() as t:
                    conn.query(query)
                results.append(payload_size / (t.interval / 1000.))

        computed = self._compute_results(results)
        return computed

    ###############################
    # Private Interface

    def _compute_results(self, data):
        data.sort()

        return {
            '99th': self._percentile(data, 0.997),
            '95th': self._percentile(data, 0.95),
            '68th': self._percentile(data, 0.68),
            'median': self._percentile(data, 0.50),
            'avg': sum(data) / len(data),
            'max': max(data),
            'min': min(data),
        }

    def _percentile(self, data, percent):
        """
        Find the percentile of a sorted list of values.
        From: http://stackoverflow.com/questions/2374640/how-do-i-calculate-percentiles-with-python-numpy

        data: sorted list of values
        percent: the percentile to compute. (range: 0, 1)
        """
        if not data:
            return None
        k = (len(data) - 1) * percent
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return data[int(k)]
        d0 = data[int(f)] * (c - k)
        d1 = data[int(c)] * (k - f)
        return d0 + d1
