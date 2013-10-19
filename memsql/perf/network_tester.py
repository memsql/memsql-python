from memsql.common import sql_utility, database, errorcodes
from wraptor.context import timer
import math

DATA_TABLE = """\
CREATE /*!90618 REFERENCE */ TABLE IF NOT EXISTS %(name)s (
    name VARCHAR(255) PRIMARY KEY,
    data LONGTEXT
)"""

class NetworkTester(sql_utility.SQLUtility):
    def __init__(self, table_prefix='network_tester', payload_size=1024 * 100):
        super(NetworkTester, self).__init__()

        self._payload_size = payload_size
        self.table_prefix = table_prefix.rstrip('_')
        self.data_table = self.table_prefix + '_data'
        self._define_table(self.data_table, DATA_TABLE % { 'name': self.data_table })

    ###############################
    # Public Interface

    def setup(self):
        super(NetworkTester, self).setup()

        self.half_payload = 'a' * (self._payload_size / 2)
        self.payload = self.half_payload * 2

        # bootstrap table
        with self._db_conn() as conn:
            try:
                conn.execute('''
                    INSERT INTO %s VALUES
                    ("half_payload", %%s),
                    ("payload", %%s)
                ''' % self.data_table, self.half_payload, self.payload)
            except database.MySQLError as (errno, msg):
                if errno == errorcodes.ER_DUP_KEY:
                    pass
                else:
                    raise

    def estimate_latency(self, iterations=100):
        results = []
        with self._db_conn() as conn:
            for _ in range(iterations):
                with timer() as t:
                    conn.ping()
                results.append(t.interval)

        computed = self._compute_results(results)
        return computed

    def estimate_roundtrip(self, iterations=100):
        results = []
        query = 'SELECT data AS a FROM %s # %%s' % (self.data_table)
        with self._db_conn() as conn:
            half_payload = conn.get('SELECT data FROM %s WHERE name="half_payload"' % self.data_table).data
            conn.execute(query, half_payload)  # ensure compile

            for _ in range(iterations):
                with timer() as t:
                    conn.query(query, half_payload)
                results.append((len(half_payload) * 2) / (t.interval / 1000.))

        computed = self._compute_results(results)
        return computed

    def estimate_upload(self, iterations=100):
        results = []
        query = '!!%s'
        with self._db_conn() as conn:
            payload = conn.get('SELECT data FROM %s WHERE name="payload"' % self.data_table).data

            for _ in range(iterations):
                try:
                    with timer() as t:
                        conn.query(query, payload)
                except database.MySQLError as (errno, msg):
                    if errno == errorcodes.ER_PARSE_ERROR:
                        pass
                    else:
                        raise
                results.append(len(payload) / (t.interval / 1000.))

        computed = self._compute_results(results)
        return computed

    def estimate_download(self, iterations=100):
        results = []
        query = 'SELECT data AS a FROM %s WHERE name="payload"' % (self.data_table)
        with self._db_conn() as conn:
            for _ in range(iterations):
                with timer() as t:
                    payload = conn.get(query).a
                results.append(len(payload) / (t.interval / 1000.))

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
